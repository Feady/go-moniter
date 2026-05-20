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
		cmd = exec.CommandContext(ctx, "cmd", "/c", sc.command)
	} else {
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
