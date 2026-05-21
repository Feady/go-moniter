package collector

import (
	"bufio"
	"context"
	"log"
	"net"
	"sync"
	"time"
)

type TCPCollector struct {
	address   string
	timeout   time.Duration
	conn      net.Conn
	buf       []string
	mu        sync.Mutex
	stopCh    chan struct{}
	reconnect chan struct{}
	isRunning bool
	runMu     sync.Mutex
}

func NewTCPCollector(address string) *TCPCollector {
	return &TCPCollector{
		address:   address,
		timeout:   5 * time.Second,
		buf:       make([]string, 0),
		stopCh:    make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
}

func (tc *TCPCollector) SetTimeout(d time.Duration) {
	tc.timeout = d
}

func (tc *TCPCollector) Start() error {
	if err := tc.connect(); err != nil {
		return err
	}
	go tc.reconnectLoop()
	return nil
}

func (tc *TCPCollector) connect() error {
	tc.runMu.Lock()
	defer tc.runMu.Unlock()

	if tc.conn != nil {
		tc.conn.Close()
		tc.conn = nil
	}

	log.Printf("[TCP] 连接到 %s", tc.address)
	d := net.Dialer{Timeout: tc.timeout}
	conn, err := d.DialContext(context.Background(), "tcp", tc.address)
	if err != nil {
		log.Printf("[TCP] 连接失败: %v", err)
		return err
	}
	tc.conn = conn
	tc.isRunning = true
	log.Printf("[TCP] 连接成功，开始监听数据")
	go tc.listenForData()
	return nil
}

func (tc *TCPCollector) reconnectLoop() {
	for {
		select {
		case <-tc.stopCh:
			return
		case <-tc.reconnect:
			log.Printf("[TCP] 检测到断开，准备重连...")
			for {
				select {
				case <-tc.stopCh:
					return
				default:
				}
				if err := tc.connect(); err == nil {
					log.Printf("[TCP] 重连成功")
					break
				}
				log.Printf("[TCP] 重连失败，3秒后重试...")
				time.Sleep(3 * time.Second)
			}
		}
	}
}

func (tc *TCPCollector) listenForData() {
	defer func() {
		tc.runMu.Lock()
		tc.isRunning = false
		if tc.conn != nil {
			tc.conn.Close()
			tc.conn = nil
		}
		tc.runMu.Unlock()

		select {
		case tc.reconnect <- struct{}{}:
		default:
		}
	}()

	reader := bufio.NewReader(tc.conn)
	for {
		tc.conn.SetReadDeadline(time.Now().Add(tc.timeout * 10))
		line, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			log.Printf("[TCP] 读取失败: %v", err)
			return
		}
		if len(line) > 0 {
			line = line[:len(line)-1]
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
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if len(tc.buf) > 0 {
		line := tc.buf[0]
		tc.buf = tc.buf[1:]
		return line, nil
	}

	return "", nil
}

func (tc *TCPCollector) Close() {
	select {
	case <-tc.stopCh:
		return
	default:
		close(tc.stopCh)
	}

	tc.runMu.Lock()
	defer tc.runMu.Unlock()

	tc.isRunning = false
	if tc.conn != nil {
		log.Printf("[TCP] 关闭连接: %s", tc.address)
		tc.conn.Close()
		tc.conn = nil
	}
	tc.mu.Lock()
	tc.buf = make([]string, 0)
	tc.mu.Unlock()
}