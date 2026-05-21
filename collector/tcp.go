package collector

import (
	"bufio"
	"context"
	"fmt"
	"log"
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
	log.Printf("[TCP] 连接到 %s", tc.address)
	d := net.Dialer{Timeout: tc.timeout}
	conn, err := d.DialContext(context.Background(), "tcp", tc.address)
	if err != nil {
		log.Printf("[TCP] 连接失败: %v", err)
		return err
	}
	tc.conn = conn
	log.Printf("[TCP] 连接成功")
	return nil
}

func (tc *TCPCollector) Read() (string, error) {
	log.Printf("[TCP] 读取数据，地址: %s", tc.address)
	if tc.conn == nil {
		log.Printf("[TCP] 连接不存在，尝试重新连接")
		if err := tc.Connect(); err != nil {
			log.Printf("[TCP] 重新连接失败: %v", err)
			return "", err
		}
	}

	tc.conn.SetReadDeadline(time.Now().Add(tc.timeout))
	reader := bufio.NewReader(tc.conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[TCP] 读取失败: %v", err)
		tc.conn = nil
		return "", fmt.Errorf("tcp read: %w", err)
	}
	log.Printf("[TCP] 读取成功，数据长度: %d", len(line))
	if len(line) > 0 {
		log.Printf("[TCP] 数据内容: %s", line)
	}
	return line, nil
}

func (tc *TCPCollector) Close() {
	if tc.conn != nil {
		log.Printf("[TCP] 关闭连接: %s", tc.address)
		tc.conn.Close()
		tc.conn = nil
	}
}