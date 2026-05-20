package collector

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"
)

type TCPCollector struct {
	address string
	timeout time.Duration
	conn    net.Conn
}

func NewTCPCollector(address string) *TCPCollector {
	return &TCPCollector{
		address: address,
		timeout: 5 * time.Second,
	}
}

func (tc *TCPCollector) SetTimeout(d time.Duration) {
	tc.timeout = d
}

func (tc *TCPCollector) Connect() error {
	tc.Close()
	d := net.Dialer{Timeout: tc.timeout}
	conn, err := d.DialContext(context.Background(), "tcp", tc.address)
	if err != nil {
		return err
	}
	tc.conn = conn
	return nil
}

func (tc *TCPCollector) Read() (string, error) {
	if tc.conn == nil {
		if err := tc.Connect(); err != nil {
			return "", err
		}
	}

	tc.conn.SetReadDeadline(time.Now().Add(tc.timeout))
	reader := bufio.NewReader(tc.conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		tc.conn = nil
		return "", fmt.Errorf("tcp read: %w", err)
	}
	return line, nil
}

func (tc *TCPCollector) Close() {
	if tc.conn != nil {
		tc.conn.Close()
		tc.conn = nil
	}
}
