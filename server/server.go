package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gomoniter/collector"
	"gomoniter/storage"

	"github.com/google/uuid"
)

type DataSource struct {
	Name         string  `json:"name"`
	SourceType   string  `json:"source_type"`
	Rule         string  `json:"rule,omitempty"`
	Color        string  `json:"color"`
	Unit         string  `json:"unit"`
	YAxisID      string  `json:"y_axis_id"`
	MetricType   string  `json:"metric_type,omitempty"`
	CPUUnit      string  `json:"cpu_unit,omitempty"`
	ShellCommand string  `json:"shell_command,omitempty"`
	TCPAddress   string  `json:"tcp_address,omitempty"`
	YMin         float64 `json:"y_min"`
	YMax         float64 `json:"y_max"`
}

type ChartConfig struct {
	ID              string       `json:"id"`
	Name            string       `json:"name"`
	Enabled         bool         `json:"enabled"`
	Protected       bool         `json:"protected,omitempty"`
	Interval        int          `json:"interval"`
	RefreshInterval int          `json:"refresh_interval"`
	DataSources     []DataSource `json:"data_sources"`
	AutoRefresh     bool         `json:"auto_refresh"`
	lastCollect     int64        `json:"-"`
}

type CommandButton struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Command     string `json:"command"`
	DisplayType string `json:"display_type"`
	AutoRefresh int    `json:"auto_refresh"`
}

type FullConfig struct {
	Charts         []ChartConfig   `json:"charts"`
	Port           int             `json:"port"`
	CommandButtons []CommandButton `json:"command_buttons"`
}

type chartExportData struct {
	Name        string
	Charts      []*storage.DataPoint
	Annotations []*storage.Annotation
}

type Server struct {
	store         *storage.Store
	sysCollector  *collector.SystemCollector
	config        *FullConfig
	configMu      sync.RWMutex
	tcpCollectors map[string]*collector.TCPCollector
	tcpMu         sync.Mutex
	stopCh        chan struct{}
	webContent    []byte
}

func New(store *storage.Store, sysCol *collector.SystemCollector, webContent []byte) *Server {
	s := &Server{
		store:         store,
		sysCollector:  sysCol,
		tcpCollectors: make(map[string]*collector.TCPCollector),
		stopCh:        make(chan struct{}),
		webContent:    webContent,
	}
	s.loadConfig()
	return s
}

func (s *Server) loadConfig() {
	data, err := s.store.LoadConfig()
	if err != nil {
		s.config = defaultConfig()
		return
	}
	var cfg FullConfig
	if err := json.Unmarshal(data, &cfg); err != nil || len(cfg.Charts) == 0 {
		s.config = defaultConfig()
		return
	}
	s.ensureProtectedCharts(&cfg)
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}
	for i := range cfg.Charts {
		if cfg.Charts[i].Interval <= 0 {
			cfg.Charts[i].Interval = 3
		}
		if cfg.Charts[i].RefreshInterval <= 0 {
			cfg.Charts[i].RefreshInterval = cfg.Charts[i].Interval
		}
		if cfg.Charts[i].RefreshInterval < 1 {
			cfg.Charts[i].RefreshInterval = 1
		}
	}
	s.configMu.Lock()
	s.config = &cfg
	s.configMu.Unlock()
}

func (s *Server) ensureProtectedCharts(cfg *FullConfig) {
	hasCPU := false
	hasMem := false
	for _, c := range cfg.Charts {
		if c.ID == "cpu" {
			hasCPU = true
		}
		if c.ID == "memory" {
			hasMem = true
		}
	}
	def := defaultConfig()
	if !hasCPU {
		cfg.Charts = append(cfg.Charts, def.Charts[0])
	}
	if !hasMem {
		cfg.Charts = append(cfg.Charts, def.Charts[1])
	}
}

func defaultConfig() *FullConfig {
	return &FullConfig{
		Port: 8080,
		Charts: []ChartConfig{
			{
				ID: "cpu", Name: "CPU", Enabled: true, Protected: true,
				Interval: 2, RefreshInterval: 2, AutoRefresh: true,
				DataSources: []DataSource{
					{
						Name: "CPU使用率", SourceType: "system", Rule: "self",
						Color: "#3b82f6", Unit: "%", YAxisID: "y-cpu",
						MetricType: "cpu", CPUUnit: "percent", YMin: 0, YMax: 100,
					},
				},
			},
			{
				ID: "memory", Name: "内存", Enabled: true, Protected: true,
				Interval: 2, RefreshInterval: 2, AutoRefresh: true,
				DataSources: []DataSource{
					{
						Name: "使用率", SourceType: "system", Rule: "self",
						Color: "#3b82f6", Unit: "%", YAxisID: "y-mem-percent",
						MetricType: "memory", YMin: 0, YMax: 100,
					},
					{
						Name: "已用(GB)", SourceType: "system", Rule: "self",
						Color: "#22c55e", Unit: "GB", YAxisID: "y-mem-gb",
						MetricType: "memory",
					},
				},
			},
		},
		CommandButtons: []CommandButton{},
	}
}

func (s *Server) SetPort(port int) {
	s.configMu.Lock()
	s.config.Port = port
	s.configMu.Unlock()
}

func (s *Server) Start() {
	s.startCollectionLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/data", s.handleData)
	mux.HandleFunc("/api/data/clear", s.handleClearData)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/history/clear", s.handleClearHistory)
	mux.HandleFunc("/api/command", s.handleCommand)
	mux.HandleFunc("/api/annotations", s.handleAnnotations)
	mux.HandleFunc("/api/export", s.handleExport)
	mux.HandleFunc("/api/export/all", s.handleExportAll)

	port := s.config.Port
	log.Printf("服务器启动在 http://0.0.0.0:%d", port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

func (s *Server) startCollectionLoop() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.collectAndSave()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *Server) collectAndSave() {
	now := time.Now()
	nowMs := now.UnixMilli()

	s.configMu.RLock()
	charts := make([]ChartConfig, len(s.config.Charts))
	copy(charts, s.config.Charts)
	s.configMu.RUnlock()

	shellCache := make(map[string]string)
	tcpCache := make(map[string]string)

	for _, chart := range charts {
		if !chart.Enabled {
			continue
		}
		intervalNs := int64(chart.Interval) * 1000000000
		if intervalNs > 0 && now.UnixNano()-chart.lastCollect < intervalNs {
			continue
		}

		log.Printf("[Collect] 开始采集图表: %s (ID: %s)", chart.Name, chart.ID)

		values := make(map[string]float64)
		for _, ds := range chart.DataSources {
			log.Printf("[Collect] 处理数据源: %s (类型: %s)", ds.Name, ds.SourceType)
			dsVals := s.collectDataSourceCached(ds, shellCache, tcpCache)
			for k, v := range dsVals {
				values[k] = v
				log.Printf("[Collect] 获取值: %s = %f", k, v)
			}
			if len(dsVals) == 0 {
				log.Printf("[Collect] 数据源 %s 没有返回数据", ds.Name)
			}
		}
		if len(values) == 0 {
			log.Printf("[Collect] 图表 %s 没有有效数据，跳过", chart.Name)
			continue
		}

		dp := &storage.DataPoint{
			ChartID:   chart.ID,
			Timestamp: nowMs,
			Values:    values,
		}
		if err := s.store.PutDataPoint(dp); err != nil {
			log.Printf("[Collect] 保存数据点失败: %v", err)
		}
		if err := s.store.PutHistoryPoint(dp); err != nil {
			log.Printf("[Collect] 保存历史点失败: %v", err)
		}

		s.configMu.Lock()
		for i := range s.config.Charts {
			if s.config.Charts[i].ID == chart.ID {
				s.config.Charts[i].lastCollect = now.UnixNano()
				break
			}
		}
		s.configMu.Unlock()
	}
}

func (s *Server) collectDataSourceCached(ds DataSource, shellCache, tcpCache map[string]string) map[string]float64 {
	switch ds.SourceType {
	case "system":
		return s.collectSystem(ds)
	case "shell":
		return s.collectShellCached(ds, shellCache)
	case "tcp":
		return s.collectTCPCached(ds, tcpCache)
	}
	return nil
}

func (s *Server) collectDataSource(ds DataSource) map[string]float64 {
	switch ds.SourceType {
	case "system":
		return s.collectSystem(ds)
	case "shell":
		return s.collectShell(ds)
	case "tcp":
		return s.collectTCP(ds)
	}
	return nil
}

func (s *Server) collectShellCached(ds DataSource, shellCache map[string]string) map[string]float64 {
	if raw, ok := shellCache[ds.ShellCommand]; ok {
		log.Printf("[Shell] 使用缓存数据，命令: %s", ds.ShellCommand)
		val := parseValue(raw, ds.Rule, ds.Name)
		if math.IsNaN(val) {
			return nil
		}
		return map[string]float64{ds.Name: val}
	}
	sc := collector.NewShellCollector(ds.ShellCommand)
	raw, err := sc.Execute()
	if err != nil {
		log.Printf("[Shell] 执行命令失败: %v", err)
		return nil
	}
	shellCache[ds.ShellCommand] = raw
	val := parseValue(raw, ds.Rule, ds.Name)
	if math.IsNaN(val) {
		log.Printf("[Shell] 解析失败，原始数据: %s, 规则: %s", raw, ds.Rule)
		return nil
	}
	return map[string]float64{ds.Name: val}
}

func (s *Server) collectTCPCached(ds DataSource, tcpCache map[string]string) map[string]float64 {
	if raw, ok := tcpCache[ds.TCPAddress]; ok {
		log.Printf("[TCP] 使用缓存数据，地址: %s", ds.TCPAddress)
		val := parseValue(raw, ds.Rule, ds.Name)
		if math.IsNaN(val) {
			return nil
		}
		return map[string]float64{ds.Name: val}
	}
	s.tcpMu.Lock()
	tc, exists := s.tcpCollectors[ds.Name]
	if !exists {
		tc = collector.NewTCPCollector(ds.TCPAddress)
		s.tcpCollectors[ds.Name] = tc
		tc.Start()
	}
	s.tcpMu.Unlock()
	raw, err := tc.Read()
	if err != nil {
		log.Printf("[TCP] 读取失败: %v", err)
		return nil
	}
	if raw == "" {
		log.Printf("[TCP] 数据为空，等待服务器推送")
		return nil
	}
	tcpCache[ds.TCPAddress] = raw
	val := parseValue(raw, ds.Rule, ds.Name)
	if math.IsNaN(val) {
		log.Printf("[TCP] 解析失败，原始数据: %s, 规则: %s", raw, ds.Rule)
		return nil
	}
	return map[string]float64{ds.Name: val}
}

func (s *Server) collectSystem(ds DataSource) map[string]float64 {
	values := make(map[string]float64)
	switch ds.MetricType {
	case "cpu":
		if ds.CPUUnit == "load" {
			values["Load1"] = s.sysCollector.GetLoad1()
			values["Load5"] = s.sysCollector.GetLoad5()
			values["Load15"] = s.sysCollector.GetLoad15()
		} else {
			values["CPU使用率"] = s.sysCollector.GetCPUPercent()
		}
	case "memory":
		values["使用率"] = s.sysCollector.GetMemPercent()
		values["已用(GB)"] = math.Round(s.sysCollector.GetMemUsed()*100) / 100
	}
	result := make(map[string]float64)
	if v, ok := values[ds.Name]; ok {
		result[ds.Name] = v
	}
	return result
}

func (s *Server) collectShell(ds DataSource) map[string]float64 {
	sc := collector.NewShellCollector(ds.ShellCommand)
	raw, err := sc.Execute()
	if err != nil {
		return nil
	}
	val := parseValue(raw, ds.Rule, ds.Name)
	if math.IsNaN(val) {
		return nil
	}
	return map[string]float64{ds.Name: val}
}

func (s *Server) collectTCP(ds DataSource) map[string]float64 {
	s.tcpMu.Lock()
	tc, exists := s.tcpCollectors[ds.Name]
	if !exists {
		tc = collector.NewTCPCollector(ds.TCPAddress)
		s.tcpCollectors[ds.Name] = tc
	}
	s.tcpMu.Unlock()
	raw, err := tc.Read()
	if err != nil {
		return nil
	}
	val := parseValue(raw, ds.Rule, ds.Name)
	if math.IsNaN(val) {
		return nil
	}
	return map[string]float64{ds.Name: val}
}

func parseValue(raw, rule, dsName string) float64 {
	raw = strings.TrimSpace(raw)
	log.Printf("[Parse] 数据源: %s, 规则: %s, 原始数据长度: %d", dsName, rule, len(raw))
	if len(raw) > 0 {
		log.Printf("[Parse] 原始数据(前200字符): %s", truncate(raw, 200))
	}

	if rule == "self" || rule == "" {
		log.Printf("[Parse] 使用 self 规则")
		v, err := strconv.ParseFloat(raw, 64)
		if err == nil {
			log.Printf("[Parse] 解析成功: %f", v)
			return v
		}
		log.Printf("[Parse] self 规则解析失败: %v", err)
		return math.NaN()
	}
	if strings.HasPrefix(rule, "regex:") {
		log.Printf("[Parse] 使用 regex 规则: %s", rule)
		pattern := rule[6:]
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Printf("[Parse] 正则编译失败: %v", err)
			return math.NaN()
		}
		matches := re.FindStringSubmatch(raw)
		log.Printf("[Parse] 正则匹配结果: %v", matches)
		if len(matches) >= 2 {
			v, err := strconv.ParseFloat(matches[1], 64)
			if err == nil {
				log.Printf("[Parse] 解析成功: %f", v)
				return v
			}
			log.Printf("[Parse] 正则组解析失败: %v", err)
		}
		return math.NaN()
	}
	if strings.HasPrefix(rule, "split:") {
		log.Printf("[Parse] 使用 split 规则: %s", rule)
		parts := strings.SplitN(rule, ":", 3)
		if len(parts) >= 3 {
			sep := parts[1]
			idx, err := strconv.Atoi(parts[2])
			if err != nil {
				log.Printf("[Parse] split 索引解析失败: %v", err)
				return math.NaN()
			}
			fields := strings.Split(raw, sep)
			log.Printf("[Parse] split 结果: %v", fields)
			if idx >= 0 && idx < len(fields) {
				v, err := strconv.ParseFloat(strings.TrimSpace(fields[idx]), 64)
				if err == nil {
					log.Printf("[Parse] 解析成功: %f", v)
					return v
				}
				log.Printf("[Parse] split 字段解析失败: %v", err)
			} else {
				log.Printf("[Parse] split 索引越界: %d >= %d", idx, len(fields))
			}
		} else {
			log.Printf("[Parse] split 规则格式错误")
		}
		return math.NaN()
	}
	if strings.HasPrefix(rule, "json:") {
		log.Printf("[Parse] 使用 json 规则: %s", rule)
		path := rule[5:]
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			log.Printf("[Parse] JSON 解析失败: %v", err)
			return math.NaN()
		}
		log.Printf("[Parse] JSON 解析成功: %v", data)
		parts := strings.Split(path, ".")
		var current interface{} = data
		for _, p := range parts {
			if m, ok := current.(map[string]interface{}); ok {
				current = m[p]
			} else {
				log.Printf("[Parse] JSON 路径不存在: %s", p)
				return math.NaN()
			}
		}
		switch v := current.(type) {
		case float64:
			log.Printf("[Parse] 解析成功: %f", v)
			return v
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err == nil {
				log.Printf("[Parse] 字符串转数字成功: %f", f)
				return f
			}
			log.Printf("[Parse] 字符串转数字失败: %v", err)
		default:
			log.Printf("[Parse] JSON 值类型不支持: %T", v)
		}
		return math.NaN()
	}
	log.Printf("[Parse] 尝试直接解析")
	v, err := strconv.ParseFloat(raw, 64)
	if err == nil {
		log.Printf("[Parse] 解析成功: %f", v)
		return v
	}
	log.Printf("[Parse] 所有规则解析失败")
	return math.NaN()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if len(s.webContent) > 0 {
		w.Write(s.webContent)
		return
	}
	tmpl := template.Must(template.New("index").Parse(fallbackHTML))
	tmpl.Execute(w, nil)
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		s.configMu.RLock()
		defer s.configMu.RUnlock()
		json.NewEncoder(w).Encode(s.config)
	case http.MethodPost:
		var cfg FullConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		for i := range cfg.Charts {
			if cfg.Charts[i].Interval <= 0 {
				cfg.Charts[i].Interval = 3
			}
			if cfg.Charts[i].RefreshInterval <= 0 {
				cfg.Charts[i].RefreshInterval = cfg.Charts[i].Interval
			}
			if cfg.Charts[i].RefreshInterval < 1 {
				cfg.Charts[i].RefreshInterval = 1
			}
			if cfg.Charts[i].ID == "" {
				cfg.Charts[i].ID = "chart_" + uuid.New().String()[:8]
			}
			if cfg.Charts[i].ID == "cpu" || cfg.Charts[i].ID == "memory" {
				cfg.Charts[i].Protected = true
			}
			for j := range cfg.Charts[i].DataSources {
				if cfg.Charts[i].DataSources[j].SourceType == "" {
					cfg.Charts[i].DataSources[j].SourceType = "system"
				}
			}
		}
		if cfg.Port <= 0 {
			cfg.Port = 8080
		}
		s.ensureProtectedCharts(&cfg)
		s.configMu.Lock()
		s.config = &cfg
		s.configMu.Unlock()

		data, _ := json.Marshal(cfg)
		s.store.SaveConfig(data)

		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		http.Error(w, `{"error":"method not allowed"}`, 405)
	}
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chartID := r.URL.Query().Get("chart_id")
	var since, until, limit int64
	if sv := r.URL.Query().Get("since"); sv != "" {
		since, _ = strconv.ParseInt(sv, 10, 64)
	}
	if uv := r.URL.Query().Get("until"); uv != "" {
		until, _ = strconv.ParseInt(uv, 10, 64)
	}
	if lv := r.URL.Query().Get("limit"); lv != "" {
		limit, _ = strconv.ParseInt(lv, 10, 64)
	}

	points, err := s.store.GetDataPoints(chartID, since, until)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
		return
	}
	if points == nil {
		points = []*storage.DataPoint{}
	}
	if limit > 0 && int64(len(points)) > limit {
		points = points[len(points)-int(limit):]
	}
	json.NewEncoder(w).Encode(points)
}

func (s *Server) handleClearData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chartID := r.URL.Query().Get("chart_id")
	if chartID == "" {
		http.Error(w, `{"error":"chart_id required"}`, 400)
		return
	}
	if err := s.store.ClearChartData(chartID); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chartID := r.URL.Query().Get("chart_id")
	var since, until, limit int64
	if sv := r.URL.Query().Get("since"); sv != "" {
		since, _ = strconv.ParseInt(sv, 10, 64)
	}
	if uv := r.URL.Query().Get("until"); uv != "" {
		until, _ = strconv.ParseInt(uv, 10, 64)
	}
	if lv := r.URL.Query().Get("limit"); lv != "" {
		limit, _ = strconv.ParseInt(lv, 10, 64)
	}

	points, err := s.store.GetHistoryPoints(chartID, since, until)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
		return
	}
	if points == nil {
		points = []*storage.DataPoint{}
	}
	if limit > 0 && int64(len(points)) > limit {
		points = points[len(points)-int(limit):]
	}
	json.NewEncoder(w).Encode(points)
}

func (s *Server) handleClearHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chartID := r.URL.Query().Get("chart_id")
	if chartID == "" {
		if err := s.store.ClearAllHistory(); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
	} else {
		if err := s.store.ClearChartHistory(chartID); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, 405)
		return
	}
	var req struct {
		Command string `json:"command"`
		Timeout int    `json:"timeout"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, 400)
		return
	}
	sc := collector.NewShellCollector(req.Command)
	if req.Timeout > 0 {
		sc.SetTimeout(time.Duration(req.Timeout) * time.Second)
	}
	output, err := sc.Execute()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{
			"output": output,
			"error":  err.Error(),
		})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"output": output})
}

func (s *Server) handleAnnotations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		chartID := r.URL.Query().Get("chart_id")
		anns, err := s.store.GetAnnotations(chartID)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		if anns == nil {
			anns = []*storage.Annotation{}
		}
		json.NewEncoder(w).Encode(anns)
	case http.MethodPost:
		var ann storage.Annotation
		if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if ann.ID == "" {
			ann.ID = uuid.New().String()
		}
		if err := s.store.PutAnnotation(&ann); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		json.NewEncoder(w).Encode(ann)
	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, `{"error":"id required"}`, 400)
			return
		}
		if err := s.store.DeleteAnnotation(id); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		http.Error(w, `{"error":"method not allowed"}`, 405)
	}
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	chartID := r.URL.Query().Get("chart_id")
	chartName := chartID
	s.configMu.RLock()
	for _, c := range s.config.Charts {
		if c.ID == chartID {
			chartName = c.Name
			break
		}
	}
	s.configMu.RUnlock()

	points, err := s.store.GetHistoryPoints(chartID, 0, 0)
	if err != nil || len(points) == 0 {
		points, _ = s.store.GetDataPoints(chartID, 0, 0)
	}
	anns, _ := s.store.GetAnnotations(chartID)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.html", chartID))
	io.WriteString(w, buildExportHTML(chartName, points, anns))
}

func (s *Server) handleExportAll(w http.ResponseWriter, r *http.Request) {
	var since, until int64
	if sv := r.URL.Query().Get("since"); sv != "" {
		since, _ = strconv.ParseInt(sv, 10, 64)
	}
	if uv := r.URL.Query().Get("until"); uv != "" {
		until, _ = strconv.ParseInt(uv, 10, 64)
	}

	s.configMu.RLock()
	enabledIDs := []string{}
	chartNames := make(map[string]string)
	for _, c := range s.config.Charts {
		if c.Enabled {
			enabledIDs = append(enabledIDs, c.ID)
			chartNames[c.ID] = c.Name
		}
	}
	s.configMu.RUnlock()

	exports := make(map[string]*chartExportData)
	for _, cid := range enabledIDs {
		pts, err := s.store.GetHistoryPoints(cid, since, until)
		if err != nil || len(pts) == 0 {
			pts, _ = s.store.GetDataPoints(cid, since, until)
		}
		anns, _ := s.store.GetAnnotations(cid)
		exports[cid] = &chartExportData{Name: chartNames[cid], Charts: pts, Annotations: anns}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=gomoniter_export.html")
	io.WriteString(w, buildExportAllHTML(exports))
}

func buildExportHTML(name string, points []*storage.DataPoint, anns []*storage.Annotation) string {
	if len(points) == 0 {
		return fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>%s</title></head><body><h2>%s</h2><p>无数据</p></body></html>`, template.HTMLEscapeString(name), template.HTMLEscapeString(name))
	}

	type dsInfo struct {
		Name  string
		Color string
		Unit  string
	}
	var dsList []dsInfo
	dsSet := make(map[string]bool)
	for _, dp := range points {
		for k := range dp.Values {
			if !dsSet[k] {
				dsSet[k] = true
				dsList = append(dsList, dsInfo{Name: k, Color: "", Unit: ""})
			}
		}
	}

	labels := []string{}
	datasets := make(map[string][]float64)
	for _, dp := range points {
		t := time.UnixMilli(dp.Timestamp).Format("2006-01-02 15:04:05")
		labels = append(labels, t)
		for k, v := range dp.Values {
			datasets[k] = append(datasets[k], v)
		}
	}

	colors := []string{"#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#ec4899", "#84cc16"}

	var datasetLines []string
	i := 0
	for _, dsi := range dsList {
		vals := datasets[dsi.Name]
		if vals == nil {
			continue
		}
		color := colors[i%len(colors)]
		valsJSON, _ := json.Marshal(vals)
		datasetLines = append(datasetLines, fmt.Sprintf(`{label:"%s",data:%s,borderColor:"%s",backgroundColor:"%s",fill:false,tension:0.1,pointRadius:0}`,
			dsi.Name, string(valsJSON), color, color))
		i++
	}
	labelsJSON, _ := json.Marshal(labels)

	var annLines []string
	for _, a := range anns {
		annLines = append(annLines, fmt.Sprintf(`{type:'line',xMin:%d,xMax:%d,borderColor:'%s',borderWidth:1,borderDash:[4,2],label:{display:true,content:'%s',position:'start',backgroundColor:'rgba(239,68,68,0.75)',color:'#fff',font:{size:10}}}`,
			a.Timestamp, a.Timestamp, map[bool]string{true: "#f59e0b", false: "#ef4444"}[a.Pinned], template.JSEscapeString(a.Text)))
	}
	annJSON := strings.Join(annLines, ",")

	return fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>%s</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>
<style>body{max-width:1200px;margin:auto;padding:20px;background:#fff;font-family:sans-serif}</style></head><body>
<h2>%s</h2><div style="height:600px"><canvas id="chart" style="height:100%%"></canvas></div>
<script>
new Chart(document.getElementById('chart'),{type:'line',data:{labels:%s,datasets:[%s]},
options:{responsive:true,maintainAspectRatio:false,plugins:{annotation:{annotations:[%s]}},
scales:{x:{type:'time',time:{tooltipFormat:'yyyy-MM-dd HH:mm:ss'},title:{display:true,text:'时间'}},y:{title:{display:true,text:'值'}}}}});
</script></body></html>`, template.HTMLEscapeString(name), template.HTMLEscapeString(name), string(labelsJSON), strings.Join(datasetLines, ","), annJSON)
}

func buildExportAllHTML(exports map[string]*chartExportData) string {
	var sections strings.Builder
	colors := []string{"#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#ec4899", "#84cc16"}
	chartIdx := 1

	for _, exp := range exports {
		if len(exp.Charts) == 0 {
			continue
		}
		type dsInfo struct{ Name string }
		var dsList []dsInfo
		dsSet := make(map[string]bool)
		for _, dp := range exp.Charts {
			for k := range dp.Values {
				if !dsSet[k] {
					dsSet[k] = true
					dsList = append(dsList, dsInfo{Name: k})
				}
			}
		}
		labels := []string{}
		datasets := make(map[string][]float64)
		for _, dp := range exp.Charts {
			t := time.UnixMilli(dp.Timestamp).Format("2006-01-02 15:04:05")
			labels = append(labels, t)
			for k, v := range dp.Values {
				datasets[k] = append(datasets[k], v)
			}
		}
		var datasetLines []string
		i := 0
		for _, dsi := range dsList {
			vals := datasets[dsi.Name]
			if vals == nil {
				continue
			}
			color := colors[i%len(colors)]
			valsJSON, _ := json.Marshal(vals)
			datasetLines = append(datasetLines, fmt.Sprintf(`{label:"%s",data:%s,borderColor:"%s",backgroundColor:"%s",fill:false,tension:0.1,pointRadius:0}`,
				dsi.Name, string(valsJSON), color, color))
			i++
		}
		labelsJSON, _ := json.Marshal(labels)

		var annLines []string
		for _, a := range exp.Annotations {
			annLines = append(annLines, fmt.Sprintf(`{type:'line',xMin:%d,xMax:%d,borderColor:'%s',borderWidth:1,borderDash:[4,2],label:{display:true,content:'%s',position:'start',backgroundColor:'rgba(239,68,68,0.75)',color:'#fff',font:{size:10}}}`,
				a.Timestamp, a.Timestamp, map[bool]string{true: "#f59e0b", false: "#ef4444"}[a.Pinned], template.JSEscapeString(a.Text)))
		}
		annJSON := strings.Join(annLines, ",")

		sections.WriteString(fmt.Sprintf(`<h2>%s</h2><div style="height:500px"><canvas id="chart%d" style="height:100%%"></canvas></div>
<script>
new Chart(document.getElementById('chart%d'),{type:'line',data:{labels:%s,datasets:[%s]},
options:{responsive:true,maintainAspectRatio:false,plugins:{annotation:{annotations:[%s]}},
scales:{x:{type:'time',time:{tooltipFormat:'yyyy-MM-dd HH:mm:ss'},title:{display:true,text:'时间'}},y:{title:{display:true,text:'值'}}}}});
</script>`,
			template.HTMLEscapeString(exp.Name), chartIdx, chartIdx, string(labelsJSON), strings.Join(datasetLines, ","), annJSON))
		chartIdx++
	}

	return fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>GoMoniter Export</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>
<style>body{max-width:1200px;margin:auto;padding:20px;background:#fff;font-family:sans-serif}h2{color:#333}</style></head><body>%s</body></html>`,
		sections.String())
}

const fallbackHTML = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>GoMoniter</title></head>
<body><h1>GoMoniter</h1><p>Web assets not embedded. Build with: go build</p></body></html>`