package collector

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type TCPCollector struct {
	address string
	timeout time.Duration
	conn    net.Conn
	buf     []string
	mu      sync.Mutex
	stopCh  chan struct{}
}

func NewTCPCollector(address string) *TCPCollector {
	return &TCPCollector{
		address: address,
		timeout: 5 * time.Second,
		buf:     make([]string, 0),
		stopCh:  make(chan struct{}),
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
	log.Printf("[TCP] 连接成功，开始监听数据")
	go tc.listenForData()
	return nil
}

func (tc *TCPCollector) listenForData() {
	defer func() {
		tc.Close()
		log.Printf("[TCP] 监听结束")
	}()

	reader := bufio.NewReader(tc.conn)
	for {
		select {
		case <-tc.stopCh:
			return
		default:
			tc.conn.SetReadDeadline(time.Now().Add(tc.timeout * 10))
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("[TCP] 读取失败: %v", err)
				return
			}
			if len(line) > 0 {
				log.Printf("[TCP] 收到数据: %s", line)
				tc.mu.Lock()
				tc.buf = append(tc.buf, line)
				tc.mu.Unlock()
			}
		}
	}
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

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if len(tc.buf) > 0 {
		line := tc.buf[0]
		tc.buf = tc.buf[1:]
		log.Printf("[TCP] 返回缓存数据: %s", line)
		return line, nil
	}

	log.Printf("[TCP] 缓存为空，等待新数据")
	return "", nil
}

func (tc *TCPCollector) Close() {
	select {
	case <-tc.stopCh:
	default:
		close(tc.stopCh)
	}
	if tc.conn != nil {
		log.Printf("[TCP] 关闭连接: %s", tc.address)
		tc.conn.Close()
		tc.conn = nil
	}
	tc.mu.Lock()
	tc.buf = make([]string, 0)
	tc.mu.Unlock()
}