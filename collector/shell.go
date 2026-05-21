package collector

import (
	"bytes"
	"context"
	"log"
	"os/exec"
	"runtime"
	"time"
)

type ShellCollector struct {
	command string
	timeout time.Duration
}

func NewShellCollector(command string) *ShellCollector {
	return &ShellCollector{
		command: command,
		timeout: 10 * time.Second,
	}
}

func (sc *ShellCollector) Execute() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sc.timeout)
	defer cancel()

	log.Printf("[Shell] 执行命令: %s", sc.command)

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		log.Printf("[Shell] Windows 系统，使用 cmd.exe")
		cmd = exec.CommandContext(ctx, "cmd.exe")
		cmd.Stdin = bytes.NewBufferString(sc.command + "\n")
	} else {
		log.Printf("[Shell] Linux/macOS 系统，使用 bash -c")
		cmd = exec.CommandContext(ctx, "bash", "-c", sc.command)
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	err := cmd.Run()
	output := stdout.String()
	log.Printf("[Shell] 命令执行完成，输出长度: %d, 错误: %v", len(output), err)
	if len(output) > 0 {
		log.Printf("[Shell] 输出内容(前500字符): %s", truncate(output, 500))
	}

	return output, err
}

func (sc *ShellCollector) SetTimeout(d time.Duration) {
	sc.timeout = d
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}