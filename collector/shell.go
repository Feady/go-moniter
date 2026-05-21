package collector

import (
	"bytes"
	"context"
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

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		// Windows: 使用 cmd.exe 处理多行命令
		cmd = exec.CommandContext(ctx, "cmd.exe")
		cmd.Stdin = bytes.NewBufferString(sc.command + "\n")
	} else {
		// Linux/macOS: 使用 sh -c 或直接 bash -c
		cmd = exec.CommandContext(ctx, "sh", "-c", sc.command)
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stdout

	err := cmd.Run()
	return stdout.String(), err
}

func (sc *ShellCollector) SetTimeout(d time.Duration) {
	sc.timeout = d
}
