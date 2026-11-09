package main

import (
	"context"
	"fmt"
	"github.com/StarkTech-coder/aegis/pkg/aegis"
	"sync"
	"time"
)

type localHeartbeatProbe struct {
	mu sync.Mutex

	alive map[string]bool
}

func (p *localHeartbeatProbe) ExchangeHeartbeat(_ context.Context, target aegis.NodeEndpoint, msg aegis.HeartbeatMessage) (aegis.HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.alive[target.NodeID] {
		return aegis.HeartbeatResponse{}, fmt.Errorf("node %s is not responding", target.NodeID)
	}

	return aegis.HeartbeatResponse{
		SourceNodeID: target.NodeID,
		TargetNodeID: msg.SourceNodeID,
		ReceivedAt:   time.Now(),
	}, nil
}

func main() {
	cfg := aegis.DefaultHeartbeatConfig("controller-node")
	cfg.Interval = 500 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond
	cfg.SuspectThreshold = 1
	cfg.UnhealthyThreshold = 3

	probe := &localHeartbeatProbe{
		alive: map[string]bool{
			"edge-1": true,
			"edge-2": true,
		},
	}

	monitor, err := aegis.NewHeartbeatMonitor(cfg, probe, nil)
	if err != nil {
		panic(err)
	}

	if err := monitor.AddNode(aegis.NodeEndpoint{NodeID: "edge-1", Address: "10.0.0.11:9000"}); err != nil {
		panic(err)
	}

	if err := monitor.AddNode(aegis.NodeEndpoint{NodeID: "edge-2", Address: "10.0.0.12:9000"}); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := monitor.Start(ctx); err != nil {
		panic(err)
	}

	fmt.Println("heartbeat monitor started")
	time.Sleep(1300 * time.Millisecond)

	probe.mu.Lock()
	probe.alive["edge-2"] = false
	probe.mu.Unlock()

	fmt.Println("edge-2 marked non-responsive")
	time.Sleep(1800 * time.Millisecond)

	for _, health := range monitor.ListNodeHealth() {
		fmt.Printf("node=%s state=%s misses=%d last_ok=%s last_error=%q\n",
			health.NodeID,
			health.State,
			health.ConsecutiveMisses,
			health.LastSuccessfulBeat.Format(time.RFC3339Nano),
			health.LastError,
		)
	}

	if err := monitor.Stop(); err != nil {
		panic(err)
	}

	fmt.Println("heartbeat monitor stopped cleanly")
}
