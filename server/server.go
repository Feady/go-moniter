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
	Name    string `json:"name"`
	Rule    string `json:"rule"`
	Color   string `json:"color"`
	Unit    string `json:"unit"`
	YAxisID string `json:"y_axis_id"`
}

type ChartConfig struct {
	ID              string       `json:"id"`
	Name            string       `json:"name"`
	Type            string       `json:"type"`
	Enabled         bool         `json:"enabled"`
	Protected       bool         `json:"protected,omitempty"`
	MetricType      string       `json:"metric_type,omitempty"`
	CPUUnit         string       `json:"cpu_unit,omitempty"`
	SourceType      string       `json:"source_type,omitempty"`
	ShellCommand    string       `json:"shell_command,omitempty"`
	TCPAddress      string       `json:"tcp_address,omitempty"`
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
	Name   string
	Charts []*storage.DataPoint
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
		log.Printf("load config error: %v", err)
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
		if len(cfg.Charts[i].DataSources) == 0 && cfg.Charts[i].Type == "system" {
			cfg.Charts[i].DataSources = defaultDataSources(cfg.Charts[i].MetricType, cfg.Charts[i].CPUUnit)
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
				ID: "cpu", Name: "CPU 负载", Type: "system", Enabled: true, Protected: true,
				MetricType: "cpu", CPUUnit: "percent",
				Interval: 2, RefreshInterval: 2, AutoRefresh: true,
				DataSources: []DataSource{
					{Name: "CPU使用率", Rule: "self", Color: "#3b82f6", Unit: "%", YAxisID: "y-cpu"},
				},
			},
			{
				ID: "memory", Name: "内存", Type: "system", Enabled: true, Protected: true,
				MetricType: "memory",
				Interval: 2, RefreshInterval: 2, AutoRefresh: true,
				DataSources: []DataSource{
					{Name: "使用率", Rule: "self", Color: "#3b82f6", Unit: "%", YAxisID: "y-mem-percent"},
					{Name: "已用(GB)", Rule: "self", Color: "#22c55e", Unit: "GB", YAxisID: "y-mem-gb"},
				},
			},
		},
		CommandButtons: []CommandButton{},
	}
}

func defaultDataSources(metricType, cpuUnit string) []DataSource {
	switch metricType {
	case "cpu":
		if cpuUnit == "load" {
			return []DataSource{
				{Name: "Load1", Rule: "self", Color: "#3b82f6", Unit: "", YAxisID: "y-cpu"},
				{Name: "Load5", Rule: "self", Color: "#22c55e", Unit: "", YAxisID: "y-cpu"},
				{Name: "Load15", Rule: "self", Color: "#f59e0b", Unit: "", YAxisID: "y-cpu"},
			}
		}
		return []DataSource{
			{Name: "CPU使用率", Rule: "self", Color: "#3b82f6", Unit: "%", YAxisID: "y-cpu"},
		}
	case "memory":
		return []DataSource{
			{Name: "使用率", Rule: "self", Color: "#3b82f6", Unit: "%", YAxisID: "y-mem-percent"},
			{Name: "已用(GB)", Rule: "self", Color: "#22c55e", Unit: "GB", YAxisID: "y-mem-gb"},
		}
	}
	return nil
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
	mux.HandleFunc("/api/data/all", s.handleAllData)
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

	for _, chart := range charts {
		if !chart.Enabled {
			continue
		}
		intervalNs := int64(chart.Interval) * 1000000000
		if intervalNs > 0 && now.UnixNano()-chart.lastCollect < intervalNs {
			continue
		}

		var values map[string]float64
		switch chart.Type {
		case "system":
			values = s.collectSystem(chart)
		case "custom":
			values = s.collectCustom(chart)
		}
		if values == nil || len(values) == 0 {
			continue
		}

		dp := &storage.DataPoint{
			ChartID:   chart.ID,
			Timestamp: nowMs,
			Values:    values,
		}
		if err := s.store.PutDataPoint(dp); err != nil {
			log.Printf("save data point error: %v", err)
		}
		if err := s.store.PutHistoryPoint(dp); err != nil {
			log.Printf("save history point error: %v", err)
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

func (s *Server) collectSystem(chart ChartConfig) map[string]float64 {
	values := make(map[string]float64)
	switch chart.MetricType {
	case "cpu":
		if chart.CPUUnit == "load" {
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
	return values
}

func (s *Server) collectCustom(chart ChartConfig) map[string]float64 {
	values := make(map[string]float64)
	var raw string
	var err error

	switch chart.SourceType {
	case "shell":
		sc := collector.NewShellCollector(chart.ShellCommand)
		raw, err = sc.Execute()
	case "tcp":
		s.tcpMu.Lock()
		tc, exists := s.tcpCollectors[chart.ID]
		if !exists {
			tc = collector.NewTCPCollector(chart.TCPAddress)
			s.tcpCollectors[chart.ID] = tc
		}
		s.tcpMu.Unlock()
		raw, err = tc.Read()
	case "system_ref":
		sysVals := s.collectSystem(chart)
		if len(sysVals) == 0 {
			return nil
		}
		for _, ds := range chart.DataSources {
			if v, ok := sysVals[ds.Name]; ok {
				values[ds.Name] = v
			}
		}
		return values
	}

	if err != nil {
		log.Printf("collect %s error: %v", chart.ID, err)
		return nil
	}

	for _, ds := range chart.DataSources {
		val := parseValue(raw, ds.Rule)
		if !math.IsNaN(val) {
			values[ds.Name] = val
		}
	}
	return values
}

func parseValue(raw, rule string) float64 {
	raw = strings.TrimSpace(raw)
	if rule == "self" {
		v, err := strconv.ParseFloat(raw, 64)
		if err == nil {
			return v
		}
		return math.NaN()
	}
	if strings.HasPrefix(rule, "regex:") {
		pattern := rule[6:]
		re, err := regexp.Compile(pattern)
		if err != nil {
			return math.NaN()
		}
		matches := re.FindStringSubmatch(raw)
		if len(matches) >= 2 {
			v, err := strconv.ParseFloat(matches[1], 64)
			if err == nil {
				return v
			}
		}
		return math.NaN()
	}
	if strings.HasPrefix(rule, "split:") {
		parts := strings.SplitN(rule, ":", 3)
		if len(parts) >= 3 {
			sep := parts[1]
			idx, err := strconv.Atoi(parts[2])
			if err != nil {
				return math.NaN()
			}
			fields := strings.Split(raw, sep)
			if idx >= 0 && idx < len(fields) {
				v, err := strconv.ParseFloat(strings.TrimSpace(fields[idx]), 64)
				if err == nil {
					return v
				}
			}
		}
		return math.NaN()
	}
	if strings.HasPrefix(rule, "json:") {
		path := rule[5:]
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return math.NaN()
		}
		parts := strings.Split(path, ".")
		var current interface{} = data
		for _, p := range parts {
			if m, ok := current.(map[string]interface{}); ok {
				current = m[p]
			} else {
				return math.NaN()
			}
		}
		switch v := current.(type) {
		case float64:
			return v
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return f
			}
		}
		return math.NaN()
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err == nil {
		return v
	}
	return math.NaN()
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

func (s *Server) handleAllData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	chartIDs := r.URL.Query()["chart_id"]
	if len(chartIDs) == 0 {
		http.Error(w, `{"error":"chart_id required"}`, 400)
		return
	}
	var since, until int64
	if sv := r.URL.Query().Get("since"); sv != "" {
		since, _ = strconv.ParseInt(sv, 10, 64)
	}
	if uv := r.URL.Query().Get("until"); uv != "" {
		until, _ = strconv.ParseInt(uv, 10, 64)
	}

	result := make(map[string][]*storage.DataPoint)
	for _, cid := range chartIDs {
		pts, err := s.store.GetDataPoints(cid, since, until)
		if err != nil {
			continue
		}
		if pts == nil {
			pts = []*storage.DataPoint{}
		}
		result[cid] = pts
	}
	json.NewEncoder(w).Encode(result)
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.html", chartID))
	io.WriteString(w, buildExportHTML(chartName, points))
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
		exports[cid] = &chartExportData{Name: chartNames[cid], Charts: pts}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=gomoniter_export.html")
	io.WriteString(w, buildExportAllHTML(exports))
}

func buildExportHTML(name string, points []*storage.DataPoint) string {
	labels := []string{}
	datasets := make(map[string][]float64)
	for _, dp := range points {
		t := time.UnixMilli(dp.Timestamp).Format("15:04:05")
		labels = append(labels, t)
		for k, v := range dp.Values {
			datasets[k] = append(datasets[k], v)
		}
	}

	colors := []string{"#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#ec4899", "#84cc16"}

	datasetJSON := ""
	i := 0
	for dname, vals := range datasets {
		if i > 0 {
			datasetJSON += ","
		}
		color := colors[i%len(colors)]
		valsJSON, _ := json.Marshal(vals)
		datasetJSON += fmt.Sprintf(`{label:"%s",data:%s,borderColor:"%s",backgroundColor:"%s",fill:false,tension:0.1}`,
			dname, string(valsJSON), color, color)
		i++
	}
	labelsJSON, _ := json.Marshal(labels)

	return fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>%s</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head><body><div style="max-width:1200px;margin:auto"><canvas id="chart"></canvas></div>
<script>
new Chart(document.getElementById('chart'),{type:'line',data:{labels:%s,datasets:[%s]},
options:{responsive:true,scales:{x:{title:{display:true,text:'时间'}},y:{title:{display:true,text:'值'}}}}});
</script></body></html>`, template.HTMLEscapeString(name), string(labelsJSON), datasetJSON)
}

func buildExportAllHTML(exports map[string]*chartExportData) string {
	var sections strings.Builder
	colors := []string{"#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#ec4899", "#84cc16"}
	chartID := 1

	for _, exp := range exports {
		if len(exp.Charts) == 0 {
			continue
		}
		labels := []string{}
		datasets := make(map[string][]float64)
		for _, dp := range exp.Charts {
			t := time.UnixMilli(dp.Timestamp).Format("15:04:05")
			labels = append(labels, t)
			for k, v := range dp.Values {
				datasets[k] = append(datasets[k], v)
			}
		}
		datasetJSON := ""
		i := 0
		for dname, vals := range datasets {
			if i > 0 {
				datasetJSON += ","
			}
			color := colors[i%len(colors)]
			valsJSON, _ := json.Marshal(vals)
			datasetJSON += fmt.Sprintf(`{label:"%s",data:%s,borderColor:"%s",backgroundColor:"%s",fill:false,tension:0.1}`,
				dname, string(valsJSON), color, color)
			i++
		}
		labelsJSON, _ := json.Marshal(labels)

		sections.WriteString(fmt.Sprintf(`<h2 style="color:#333;font-family:sans-serif">%s</h2>
<div style="margin-bottom:24px"><canvas id="chart%d" style="max-height:300px"></canvas></div>
<script>
new Chart(document.getElementById('chart%d'),{type:'line',data:{labels:%s,datasets:[%s]},
options:{responsive:true,maintainAspectRatio:false,scales:{x:{title:{display:true,text:'时间'}},y:{title:{display:true,text:'值'}}}}});
</script>`,
			template.HTMLEscapeString(exp.Name), chartID, chartID, string(labelsJSON), datasetJSON))
		chartID++
	}

	return fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>GoMoniter Export</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>body{max-width:1200px;margin:auto;padding:20px;background:#fff}</style></head><body>%s</body></html>`,
		sections.String())
}

const fallbackHTML = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>GoMoniter</title></head>
<body><h1>GoMoniter</h1><p>Web assets not embedded. Build with: go build</p></body></html>`
