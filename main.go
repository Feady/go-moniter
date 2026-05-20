package main

import (
	"embed"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"gomoniter/collector"
	"gomoniter/server"
	"gomoniter/storage"
)

//go:embed web/*
var webFiles embed.FS

func main() {
	port := flag.Int("port", 0, "HTTP server port")
	dbPath := flag.String("db", "", "database file path")
	flag.Parse()

	execDir, _ := os.Executable()
	execDir = filepath.Dir(execDir)

	dbFile := *dbPath
	if dbFile == "" {
		dbFile = filepath.Join(execDir, "gomoniter.db")
	}

	store, err := storage.NewStore(dbFile)
	if err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}
	defer store.Close()

	sysCol := collector.NewSystemCollector()
	logical, physical := sysCol.CPUInfo()
	log.Printf("CPU: %d 逻辑核心, %d 物理核心 | OS: %s/%s",
		logical, physical, runtime.GOOS, runtime.GOARCH)
	sysCol.Start(1 * time.Second)

	webContent := loadWebContent()

	srv := server.New(store, sysCol, webContent)
	if *port > 0 {
		srv.SetPort(*port)
	}
	srv.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("正在关闭...")
	sysCol.Stop()
}

func loadWebContent() []byte {
	data, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		log.Printf("读取嵌入网页失败: %v, 使用默认页面", err)
		return nil
	}
	return data
}
