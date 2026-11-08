package aegis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scriptedHeartbeatProber struct {
	mu sync.Mutex

	calls  []HeartbeatMessage
	script map[string][]error
}

func newScriptedHeartbeatProber() *scriptedHeartbeatProber {
	return &scriptedHeartbeatProber{script: make(map[string][]error)}
}

func (p *scriptedHeartbeatProber) setScript(nodeID string, errs ...error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.script[nodeID] = append([]error(nil), errs...)
}

func (p *scriptedHeartbeatProber) lastCall() (HeartbeatMessage, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.calls) == 0 {
		return HeartbeatMessage{}, false
	}

	return p.calls[len(p.calls)-1], true
}

func (p *scriptedHeartbeatProber) ExchangeHeartbeat(_ context.Context, target NodeEndpoint, msg HeartbeatMessage) (HeartbeatResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls = append(p.calls, msg)

	entries := p.script[target.NodeID]
	if len(entries) > 0 {
		err := entries[0]
		p.script[target.NodeID] = entries[1:]
		if err != nil {
			return HeartbeatResponse{}, err
		}
	}

	return HeartbeatResponse{
		SourceNodeID: target.NodeID,
		TargetNodeID: msg.SourceNodeID,
		ReceivedAt:   time.Now(),
	}, nil
}

func newHeartbeatMonitorForTest(t *testing.T, probe HeartbeatProbe) *HeartbeatMonitor {
	t.Helper()

	cfg := DefaultHeartbeatConfig("local-node")
	cfg.Interval = 50 * time.Millisecond
	cfg.Timeout = 25 * time.Millisecond
	cfg.SuspectThreshold = 1
	cfg.UnhealthyThreshold = 2

	monitor, err := NewHeartbeatMonitor(cfg, probe, nil)
	if err != nil {
		t.Fatalf("failed to create heartbeat monitor: %v", err)
	}

	return monitor
}

func TestHeartbeatSuccessfulProbeMarksHealthy(t *testing.T) {
	prober := newScriptedHeartbeatProber()
	monitor := newHeartbeatMonitorForTest(t, prober)

	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-a", Address: "127.0.0.1:1001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	if err := monitor.probeNode("node-a"); err != nil {
		t.Fatalf("probe failed: %v", err)
	}

	health, ok := monitor.GetNodeHealth("node-a")
	if !ok {
		t.Fatal("expected node health to exist")
	}

	if health.State != NodeHealthHealthy {
		t.Fatalf("expected healthy state, got %q", health.State)
	}

	if health.ConsecutiveMisses != 0 {
		t.Fatalf("expected zero misses, got %d", health.ConsecutiveMisses)
	}

	if health.LastSuccessfulBeat.IsZero() {
		t.Fatal("expected last successful heartbeat to be set")
	}

	lastCall, ok := prober.lastCall()
	if !ok {
		t.Fatal("expected at least one heartbeat call")
	}

	if lastCall.SourceNodeID != "local-node" || lastCall.TargetNodeID != "node-a" {
		t.Fatalf("unexpected heartbeat message identity: %+v", lastCall)
	}
}

func TestHeartbeatMissedProbeTransitionsSuspectToUnhealthy(t *testing.T) {
	prober := newScriptedHeartbeatProber()
	prober.setScript("node-a", nil, errors.New("missed"), errors.New("missed"))

	monitor := newHeartbeatMonitorForTest(t, prober)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-a", Address: "127.0.0.1:1001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	if err := monitor.probeNode("node-a"); err != nil {
		t.Fatalf("first probe should succeed: %v", err)
	}

	if err := monitor.probeNode("node-a"); err == nil {
		t.Fatal("expected second probe to fail")
	}

	health, _ := monitor.GetNodeHealth("node-a")
	if health.State != NodeHealthSuspect {
		t.Fatalf("expected suspect after one miss, got %q", health.State)
	}

	if err := monitor.probeNode("node-a"); err == nil {
		t.Fatal("expected third probe to fail")
	}

	health, _ = monitor.GetNodeHealth("node-a")
	if health.State != NodeHealthUnhealthy {
		t.Fatalf("expected unhealthy after second miss, got %q", health.State)
	}
}

func TestHeartbeatResponseTimeout(t *testing.T) {
	probe := HeartbeatProbeFunc(func(ctx context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		<-ctx.Done()
		return HeartbeatResponse{}, ctx.Err()
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-timeout", Address: "127.0.0.1:1002"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	err := monitor.probeNode("node-timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got: %v", err)
	}

	health, _ := monitor.GetNodeHealth("node-timeout")
	if health.State != NodeHealthSuspect {
		t.Fatalf("expected suspect state after timeout, got %q", health.State)
	}
}

func TestHeartbeatRecoveryUnhealthyToHealthy(t *testing.T) {
	prober := newScriptedHeartbeatProber()
	prober.setScript("node-a", errors.New("missed"), errors.New("missed"), nil)

	monitor := newHeartbeatMonitorForTest(t, prober)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-a", Address: "127.0.0.1:1001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	_ = monitor.probeNode("node-a")
	_ = monitor.probeNode("node-a")

	health, _ := monitor.GetNodeHealth("node-a")
	if health.State != NodeHealthUnhealthy {
		t.Fatalf("expected unhealthy before recovery, got %q", health.State)
	}

	if err := monitor.probeNode("node-a"); err != nil {
		t.Fatalf("recovery probe failed: %v", err)
	}

	health, _ = monitor.GetNodeHealth("node-a")
	if health.State != NodeHealthHealthy {
		t.Fatalf("expected healthy after recovery, got %q", health.State)
	}

	if health.ConsecutiveMisses != 0 {
		t.Fatalf("expected misses reset after recovery, got %d", health.ConsecutiveMisses)
	}
}

func TestHeartbeatMultipleNodes(t *testing.T) {
	prober := newScriptedHeartbeatProber()
	prober.setScript("node-a", nil)
	prober.setScript("node-b", errors.New("missed"))

	monitor := newHeartbeatMonitorForTest(t, prober)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-a", Address: "127.0.0.1:2001"}); err != nil {
		t.Fatalf("add node-a failed: %v", err)
	}

	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-b", Address: "127.0.0.1:2002"}); err != nil {
		t.Fatalf("add node-b failed: %v", err)
	}

	if err := monitor.probeNode("node-a"); err != nil {
		t.Fatalf("node-a probe failed: %v", err)
	}

	if err := monitor.probeNode("node-b"); err == nil {
		t.Fatal("expected node-b probe failure")
	}

	a, _ := monitor.GetNodeHealth("node-a")
	b, _ := monitor.GetNodeHealth("node-b")

	if a.State != NodeHealthHealthy {
		t.Fatalf("expected node-a healthy, got %q", a.State)
	}

	if b.State != NodeHealthSuspect {
		t.Fatalf("expected node-b suspect, got %q", b.State)
	}

	all := monitor.ListNodeHealth()
	if len(all) != 2 {
		t.Fatalf("expected 2 node health entries, got %d", len(all))
	}
}

func TestHeartbeatConcurrentUpdates(t *testing.T) {
	prober := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	monitor := newHeartbeatMonitorForTest(t, prober)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-concurrent", Address: "127.0.0.1:3001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := monitor.probeNode("node-concurrent"); err != nil {
					t.Errorf("probe failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	health, _ := monitor.GetNodeHealth("node-concurrent")
	if health.State != NodeHealthHealthy {
		t.Fatalf("expected healthy state, got %q", health.State)
	}
}

func TestHeartbeatMonitorContextCancellationAndGracefulShutdown(t *testing.T) {
	var calls atomic.Int32
	probe := HeartbeatProbeFunc(func(ctx context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		calls.Add(1)
		select {
		case <-ctx.Done():
			return HeartbeatResponse{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
			return HeartbeatResponse{}, nil
		}
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-a", Address: "127.0.0.1:4001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := monitor.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(70 * time.Millisecond)
	cancel()

	if err := monitor.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	countAfterStop := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != countAfterStop {
		t.Fatal("expected no additional heartbeat probes after stop")
	}
}

func TestHeartbeatMonitorStartTwiceFails(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.Start(context.Background()); err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	defer func() {
		_ = monitor.Stop()
	}()

	if err := monitor.Start(context.Background()); err == nil {
		t.Fatal("expected second start to fail")
	}
}

func TestHeartbeatInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  HeartbeatConfig
	}{
		{
			name: "missing local node id",
			cfg: HeartbeatConfig{
				Interval:           100 * time.Millisecond,
				Timeout:            50 * time.Millisecond,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "invalid interval",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           0,
				Timeout:            50 * time.Millisecond,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "invalid timeout",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           100 * time.Millisecond,
				Timeout:            0,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "negative interval",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           -100 * time.Millisecond,
				Timeout:            50 * time.Millisecond,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "negative timeout",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           100 * time.Millisecond,
				Timeout:            -10 * time.Millisecond,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "timeout greater than interval",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           100 * time.Millisecond,
				Timeout:            120 * time.Millisecond,
				SuspectThreshold:   1,
				UnhealthyThreshold: 2,
			},
		},
		{
			name: "unhealthy threshold lower than suspect",
			cfg: HeartbeatConfig{
				LocalNodeID:        "local",
				Interval:           100 * time.Millisecond,
				Timeout:            50 * time.Millisecond,
				SuspectThreshold:   2,
				UnhealthyThreshold: 1,
			},
		},
	}

	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewHeartbeatMonitor(tt.cfg, probe, nil)
			if err == nil {
				t.Fatalf("expected configuration error for case %q", tt.name)
			}
		})
	}
}

func TestHeartbeatValidConfiguration(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	cfg := HeartbeatConfig{
		LocalNodeID:        "local",
		Interval:           100 * time.Millisecond,
		Timeout:            80 * time.Millisecond,
		SuspectThreshold:   1,
		UnhealthyThreshold: 2,
	}

	if _, err := NewHeartbeatMonitor(cfg, probe, nil); err != nil {
		t.Fatalf("expected valid heartbeat config, got error: %v", err)
	}
}

func TestHeartbeatAddNodeValidation(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.AddNode(NodeEndpoint{}); err == nil {
		t.Fatal("expected add node to fail for empty node id")
	}

	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-dup", Address: "addr"}); err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-dup", Address: "addr"}); err == nil {
		t.Fatal("expected duplicate add to fail")
	}
}

func TestHeartbeatRemoveNode(t *testing.T) {
	prober := newScriptedHeartbeatProber()
	monitor := newHeartbeatMonitorForTest(t, prober)

	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-rm", Address: "127.0.0.1:5001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	monitor.RemoveNode("node-rm")
	if _, ok := monitor.GetNodeHealth("node-rm"); ok {
		t.Fatal("expected removed node to be absent")
	}

	err := monitor.probeNode("node-rm")
	if err == nil {
		t.Fatal("expected probe on removed node to fail")
	}

	if got := err.Error(); got == "" {
		t.Fatal("expected probe error message for removed node")
	}
}

func TestHeartbeatConcurrentProbesSameNodeSerialized(t *testing.T) {
	var active int32
	var maxActive int32

	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		cur := atomic.AddInt32(&active, 1)
		for {
			prev := atomic.LoadInt32(&maxActive)
			if cur <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, cur) {
				break
			}
		}

		time.Sleep(2 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return HeartbeatResponse{}, nil
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-serial", Address: "127.0.0.1:6001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := monitor.probeNode("node-serial"); err != nil {
				t.Errorf("probe failed: %v", err)
			}
		}()
	}

	wg.Wait()

	if got := atomic.LoadInt32(&maxActive); got != 1 {
		t.Fatalf("expected probes for same node to be serialized; max concurrent=%d", got)
	}
}

func TestHeartbeatStopWaitsForInFlightProbe(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)

	probe := HeartbeatProbeFunc(func(ctx context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		select {
		case started <- struct{}{}:
		default:
		}

		select {
		case <-ctx.Done():
			return HeartbeatResponse{}, ctx.Err()
		case <-release:
			return HeartbeatResponse{}, nil
		}
	})

	monitor := newHeartbeatMonitorForTest(t, probe)
	if err := monitor.AddNode(NodeEndpoint{NodeID: "node-stop", Address: "127.0.0.1:7001"}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}

	if err := monitor.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timed out waiting for heartbeat probe to start")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- monitor.Stop()
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not complete; possible goroutine leak or waitgroup deadlock")
	}

	close(release)
}
