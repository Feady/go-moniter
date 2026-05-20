package collector

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type SystemCollector struct {
	mu          sync.RWMutex
	cpuPercent  float64
	memPercent  float64
	memUsed     float64
	memTotal    float64
	load1       float64
	load5       float64
	load15      float64
	cpuLogical  int
	cpuPhysical int
	stopCh      chan struct{}
}

func NewSystemCollector() *SystemCollector {
	cpuLogical, _ := cpu.Counts(true)
	cpuPhysical, _ := cpu.Counts(false)
	return &SystemCollector{
		cpuLogical:  cpuLogical,
		cpuPhysical: cpuPhysical,
		stopCh:      make(chan struct{}),
	}
}

func (sc *SystemCollector) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		sc.collect()
		for {
			select {
			case <-ticker.C:
				sc.collect()
			case <-sc.stopCh:
				return
			}
		}
	}()
}

func (sc *SystemCollector) Stop() {
	close(sc.stopCh)
}

func (sc *SystemCollector) collect() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if percents, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(percents) > 0 {
		val := math.Round(percents[0]*10) / 10
		sc.mu.Lock()
		sc.cpuPercent = val
		sc.mu.Unlock()
	}

	if avg, err := load.AvgWithContext(ctx); err == nil {
		sc.mu.Lock()
		sc.load1 = math.Round(avg.Load1*100) / 100
		sc.load5 = math.Round(avg.Load5*100) / 100
		sc.load15 = math.Round(avg.Load15*100) / 100
		sc.mu.Unlock()
	}

	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		sc.mu.Lock()
		sc.memPercent = math.Round(vm.UsedPercent*10) / 10
		sc.memUsed = float64(vm.Used) / (1024 * 1024 * 1024)
		sc.memTotal = float64(vm.Total) / (1024 * 1024 * 1024)
		sc.mu.Unlock()
	}
}

func (sc *SystemCollector) GetCPUPercent() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.cpuPercent
}

func (sc *SystemCollector) GetLoad1() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.load1
}

func (sc *SystemCollector) GetLoad5() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.load5
}

func (sc *SystemCollector) GetLoad15() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.load15
}

func (sc *SystemCollector) GetMemPercent() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.memPercent
}

func (sc *SystemCollector) GetMemUsed() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.memUsed
}

func (sc *SystemCollector) GetMemTotal() float64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.memTotal
}

func (sc *SystemCollector) CPUInfo() (logical, physical int) {
	return sc.cpuLogical, sc.cpuPhysical
}
