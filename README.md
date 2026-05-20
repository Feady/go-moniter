# GoMoniter - 轻量级服务器监控系统

## 概述

GoMoniter 是一个使用 Go 语言开发的单二进制文件监控系统，内置 Web 面板，支持系统指标和自定义数据采集，提供实时时序折线图展示、历史数据归档和远程命令执行功能。

## 特性

- **单文件部署**: 所有资源（HTML/JS/CSS）嵌入到可执行文件中，一个文件即可运行
- **跨平台**: 支持 Linux、ARM、x86、Windows，不使用 CGO
- **嵌入式数据库**: 使用纯 Go 实现的 BoltDB 存储，无需外部依赖
- **系统预设采集**: CPU 使用率/负载、内存使用率
- **自定义采集**: Shell 命令执行采集、TCP 连接数据采集
- **灵活的数据解析**: 支持 `self`（直接解析）、`regex:`（正则捕获）、`split:`（分隔取值）、`json:`（JSON 路径）
- **实时图表**: 时序折线图，后端时间戳，自动刷新
- **多 Y 轴**: 同一图表上多个数据源可使用独立的 Y 轴
- **注释系统**: 图表上可添加注释标记，支持固定/非固定
- **数据清理**: 一键清理图表当前数据（历史数据保留）
- **历史归档**: 所有数据持久化存储，支持查看和导出
- **HTML 导出**: 导出图表的独立 HTML 文件
- **远程命令**: Web 页面执行服务器命令
- **暗色主题**: 简约美观的暗色 UI

## 编译

### 前置条件

- Go 1.23+
- 网络连接（首次编译需下载依赖）

### 编译命令

```bash
# 下载依赖
go mod tidy

# 编译（当前平台）
go build -o gomoniter

# 交叉编译 Linux amd64
GOOS=linux GOARCH=amd64 go build -o gomoniter-linux-amd64

# 交叉编译 Linux arm64
GOOS=linux GOARCH=arm64 go build -o gomoniter-linux-arm64

# 交叉编译 Windows amd64
GOOS=windows GOARCH=amd64 go build -o gomoniter.exe
```

> **注意**: 编译时必须确保 `web/` 目录存在且包含 `index.html`。Go 的 `embed` 会将 `web/index.html` 嵌入到二进制文件中。

## 运行

```bash
# 默认端口 8080
./gomoniter

# 指定端口
./gomoniter -port 9090

# 指定数据库路径
./gomoniter -db /data/gomoniter.db
```

启动后访问 `http://localhost:8080` 即可看到监控面板。

## 功能说明

### 仪表盘 (Dashboard)

- **系统预设**: CPU 使用率/负载、内存使用率和已用内存的实时折线图
- **自定义采集**: 用户配置的自定义数据采集图表
- 图表支持：
  - 暂停/恢复自动刷新
  - 清理当前图表数据（历史数据保留）
  - 导出图表为独立 HTML
  - 添加/查看注释

### 数据源配置

配置页面可以管理所有图表：

- **系统预设图表**: CPU 和内存，可选使用率或负载模式
- **自定义采集图表**: 支持两种数据源类型

#### 解析规则

| 规则 | 格式 | 说明 |
|------|------|------|
| `self` | `self` | 将原始输出直接解析为浮点数 |
| `regex:` | `regex:正则表达式` | 正则捕获第一个分组 |
| `split:` | `split:分隔符:索引` | 按分隔符拆分后取指定索引 |
| `json:` | `json:key.path` | 从 JSON 中按路径提取值 |

#### 示例

```bash
# Shell 命令输出: "CPU: 45.2%"
# 解析规则: regex:(\d+\.?\d*)
# 结果: 45.2

# Shell 命令输出: "loadavg: 0.75 0.64 0.58"
# 解析规则: split: :1
# 结果: 0.75 0.64 0.58 (取索引1)
```

### 远程命令

直接输入 Shell 命令执行，输出结果显示在页面下方。

### 历史数据

- 选择图表查看所有历史数据点
- 支持导出为 HTML 折线图
- 支持按图表或全部清理历史数据

## 技术栈

| 组件 | 技术 |
|------|------|
| 后端 | Go 标准库 `net/http` |
| 数据库 | BoltDB (纯 Go) |
| 系统指标 | gopsutil/v4 (纯 Go) |
| 前端图表 | Chart.js v4 |
| 静态资源 | Go `embed` |
| UUID | google/uuid |

## 目录结构

```
gomoniter/
├── main.go              # 入口，embed 嵌入 web 目录
├── go.mod               # Go 模块依赖
├── collector/
│   ├── system.go        # 系统指标采集 (CPU/内存/Load)
│   ├── shell.go         # Shell 命令执行
│   └── tcp.go           # TCP 数据读取
├── server/
│   └── server.go        # HTTP 服务器 + REST API
├── storage/
│   └── storage.go       # BoltDB 数据持久化
└── web/
    └── index.html       # 前端单页应用
```
