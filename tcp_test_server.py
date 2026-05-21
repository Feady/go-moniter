# GoMoniter TCP 数据源测试服务器
# 使用方法:
#   python tcp_test_server.py            # 默认 localhost:10999
#   python tcp_test_server.py 0.0.0.0 8888  # 自定义地址和端口
#
# 然后在 GoMoniter 中创建图表:
#   数据源类型: TCP连接
#   TCP地址: localhost:10999
#   线条1 - 名称: cpu,  解析规则: split:,0     (取第一个字段)
#   线条2 - 名称: mem,  解析规则: split:,1     (取第二个字段)
#   线条3 - 名称: load, 解析规则: split:,2     (取第三个字段)
#
# 每条数据格式: "cpu百分配 内存百分比 load值\n"
# 例如: "42.5 68.3 1.25\n"

import socket
import time
import sys
import random
import threading

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 10999
INTERVAL = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0

running = True
# 初始值，模拟真实波动
cpu_val = 35.0
mem_val = 60.0
load_val = 1.0

def generate_metric():
    """模拟渐进变化的系统指标"""
    global cpu_val, mem_val, load_val
    cpu_val = max(1, min(99, cpu_val + random.uniform(-5, 5)))
    mem_val = max(5, min(98, mem_val + random.uniform(-2, 2)))
    load_val = max(0.1, min(10.0, load_val + random.uniform(-0.3, 0.3)))
    return f"{cpu_val:.1f} {mem_val:.1f} {load_val:.2f}\n"

def handle_client(conn, addr):
    """为每个连接持续发送数据"""
    global running
    print(f"[{addr}] 客户端已连接")
    try:
        while running:
            data = generate_metric()
            try:
                conn.sendall(data.encode())
                print(f"[{addr}] 发送: {data.strip()}")
            except (BrokenPipeError, ConnectionResetError):
                print(f"[{addr}] 客户端断开")
                break
            time.sleep(INTERVAL)
    finally:
        conn.close()
        print(f"[{addr}] 连接关闭")

def start_server():
    global running
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"TCP 测试服务器启动在 {HOST}:{PORT}")
    print(f"发送间隔: {INTERVAL}s")
    print(f"数据格式: 'CPU使用率 内存使用率 系统负载'")
    print(f"示例: '42.5 68.3 1.25'")
    print(f"\nGoMoniter 配置提示:")
    print(f"  图表 → 添加数据源 → TCP连接 → 地址: {HOST}:{PORT}")
    print(f"  线条1: 名称=cpu_usage,  规则=split: :0")
    print(f"  线条2: 名称=mem_usage,  规则=split: :1")
    print(f"  线条3: 名称=load_avg,   规则=split: :2")
    print()

    try:
        while running:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n正在关闭...")
    finally:
        running = False
        server.close()
        print("服务器已关闭")

if __name__ == "__main__":
    start_server()
