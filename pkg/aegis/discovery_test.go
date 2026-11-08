package aegis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type scriptedDiscoverySource struct {
	mu sync.Mutex

	announcements []DiscoveryAnnouncement
	err           error
}

func (s *scriptedDiscoverySource) set(anns []DiscoveryAnnouncement, err error) {
	s.mu.Lock()
	s.announcements = append([]DiscoveryAnnouncement(nil), anns...)
	s.err = err
	s.mu.Unlock()
}

func (s *scriptedDiscoverySource) Discover(_ context.Context) ([]DiscoveryAnnouncement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := append([]DiscoveryAnnouncement(nil), s.announcements...)
	return out, s.err
}

func newDiscoveryManagerForTest(t *testing.T, source DiscoverySource, hb *HeartbeatMonitor) *DiscoveryManager {
	t.Helper()

	cfg := DefaultDiscoveryConfig()
	cfg.Interval = 20 * time.Millisecond
	cfg.CleanupInterval = 20 * time.Millisecond
	cfg.Expiration = 80 * time.Millisecond
	cfg.HealthSyncInterval = 20 * time.Millisecond
	cfg.StartHeartbeat = hb != nil

	mgr, err := NewDiscoveryManager(cfg, source, hb, nil)
	if err != nil {
		t.Fatalf("new discovery manager failed: %v", err)
	}

	return mgr
}

func TestDiscoveryRegisterDuplicateRejected(t *testing.T) {
	mgr := newDiscoveryManagerForTest(t, nil, nil)

	err := mgr.RegisterNode(NodeEndpoint{NodeID: "n1", Address: "127.0.0.1:1"}, nil)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := mgr.RegisterNode(NodeEndpoint{NodeID: "n1", Address: "127.0.0.1:1"}, nil); err == nil {
		t.Fatal("expected duplicate register to fail")
	}
}

func TestDiscoveryRemoveLookupAndMultipleNodes(t *testing.T) {
	mgr := newDiscoveryManagerForTest(t, nil, nil)

	for _, id := range []string{"a", "b", "c"} {
		if err := mgr.RegisterNode(NodeEndpoint{NodeID: id, Address: "addr-" + id}, nil); err != nil {
			t.Fatalf("register %s failed: %v", id, err)
		}
	}

	nodes := mgr.ListNodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	if _, ok := mgr.GetNode("b"); !ok {
		t.Fatal("expected node b to exist")
	}

	mgr.RemoveNode("b")
	if _, ok := mgr.GetNode("b"); ok {
		t.Fatal("expected node b removed")
	}
}

func TestDiscoveryNodeExpirationAndRediscovery(t *testing.T) {
	src := &scriptedDiscoverySource{}
	now := time.Now()
	src.set([]DiscoveryAnnouncement{{NodeID: "n1", Address: "127.0.0.1:1", SeenAt: now}}, nil)

	mgr := newDiscoveryManagerForTest(t, src, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = mgr.Stop() }()

	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return ok
	})

	src.set(nil, nil)
	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return !ok
	})

	src.set([]DiscoveryAnnouncement{{NodeID: "n1", Address: "127.0.0.1:1", SeenAt: time.Now()}}, nil)
	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return ok
	})
}

func TestDiscoveryCancellationStopsLoops(t *testing.T) {
	src := &scriptedDiscoverySource{}
	src.set([]DiscoveryAnnouncement{{NodeID: "n1", Address: "127.0.0.1:1"}}, nil)

	mgr := newDiscoveryManagerForTest(t, src, nil)
	ctx, cancel := context.WithCancel(context.Background())
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	cancel()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if mgr.IsRunning() {
		t.Fatal("expected discovery manager to be stopped")
	}
}

func TestDiscoveryConcurrentRegistrationAndRemoval(t *testing.T) {
	mgr := newDiscoveryManagerForTest(t, nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := "node-" + string(rune('a'+(i%10)))
			_ = mgr.RegisterNode(NodeEndpoint{NodeID: id, Address: "127.0.0.1:1"}, nil)
			mgr.RemoveNode(id)
		}()
	}
	wg.Wait()
}

func TestDiscoveryMalformedAnnouncementsRejected(t *testing.T) {
	src := &scriptedDiscoverySource{}
	src.set([]DiscoveryAnnouncement{
		{NodeID: "", Address: "127.0.0.1:1"},
		{NodeID: "n1", Address: ""},
	}, nil)

	mgr := newDiscoveryManagerForTest(t, src, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = mgr.Stop() }()

	time.Sleep(60 * time.Millisecond)
	if len(mgr.ListNodes()) != 0 {
		t.Fatal("expected malformed announcements to be ignored")
	}
}

func TestDiscoveryHeartbeatIntegrationUpdatesHealth(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, target NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		if target.NodeID == "bad" {
			return HeartbeatResponse{}, errors.New("unreachable")
		}
		return HeartbeatResponse{}, nil
	})

	hbCfg := DefaultHeartbeatConfig("local")
	hbCfg.Interval = 20 * time.Millisecond
	hbCfg.Timeout = 10 * time.Millisecond
	hbCfg.SuspectThreshold = 1
	hbCfg.UnhealthyThreshold = 1
	hb, err := NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		t.Fatalf("new heartbeat monitor failed: %v", err)
	}

	mgr := newDiscoveryManagerForTest(t, nil, hb)
	if err := mgr.RegisterNode(NodeEndpoint{NodeID: "good", Address: "127.0.0.1:1"}, nil); err != nil {
		t.Fatalf("register good failed: %v", err)
	}
	if err := mgr.RegisterNode(NodeEndpoint{NodeID: "bad", Address: "127.0.0.1:2"}, nil); err != nil {
		t.Fatalf("register bad failed: %v", err)
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = mgr.Stop() }()

	waitFor(t, time.Second, func() bool {
		good, gok := mgr.GetNode("good")
		bad, bok := mgr.GetNode("bad")
		return gok && bok && good.Health == NodeHealthHealthy && bad.Health == NodeHealthUnhealthy
	})
}

func TestDiscoveryInvalidConfig(t *testing.T) {
	cfg := DefaultDiscoveryConfig()
	cfg.CleanupInterval = 2 * time.Second
	cfg.Expiration = time.Second

	_, err := NewDiscoveryManager(cfg, nil, nil, nil)
	if err == nil {
		t.Fatal("expected invalid discovery config error")
	}
}

func TestDiscoveryRegisterNodeAlreadyInHeartbeatIsAccepted(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	hbCfg := DefaultHeartbeatConfig("local")
	hb, err := NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		t.Fatalf("new heartbeat monitor failed: %v", err)
	}

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "10.0.0.1:9000"}); err != nil {
		t.Fatalf("add heartbeat node failed: %v", err)
	}

	mgr := newDiscoveryManagerForTest(t, nil, hb)
	if err := mgr.RegisterNode(NodeEndpoint{NodeID: "node-a", Address: "10.0.0.1:9000"}, nil); err != nil {
		t.Fatalf("discovery register failed: %v", err)
	}

	if _, ok := mgr.GetNode("node-a"); !ok {
		t.Fatal("expected node to be present in discovery after registration")
	}
}

func TestDiscoveryRegisterNodeHeartbeatConflictRollsBack(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	hbCfg := DefaultHeartbeatConfig("local")
	hb, err := NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		t.Fatalf("new heartbeat monitor failed: %v", err)
	}

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "10.0.0.9:9000"}); err != nil {
		t.Fatalf("add heartbeat node failed: %v", err)
	}

	mgr := newDiscoveryManagerForTest(t, nil, hb)
	err = mgr.RegisterNode(NodeEndpoint{NodeID: "node-a", Address: "10.0.0.1:9000"}, nil)
	if err == nil {
		t.Fatal("expected registration conflict error")
	}

	if _, ok := mgr.GetNode("node-a"); ok {
		t.Fatal("expected discovery rollback on heartbeat conflict")
	}
}

func TestDiscoveryConcurrentSameNodeRegistration(t *testing.T) {
	mgr := newDiscoveryManagerForTest(t, nil, nil)

	const workers = 16
	errCh := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- mgr.RegisterNode(NodeEndpoint{NodeID: "same", Address: "127.0.0.1:1"}, nil)
		}()
	}

	wg.Wait()
	close(errCh)

	success := 0
	fail := 0
	for err := range errCh {
		if err == nil {
			success++
		} else {
			fail++
		}
	}

	if success != 1 {
		t.Fatalf("expected exactly one successful registration, got %d", success)
	}

	if fail != workers-1 {
		t.Fatalf("expected %d duplicate failures, got %d", workers-1, fail)
	}
}

func TestDiscoveryRemoveAndRediscoverNode(t *testing.T) {
	src := &scriptedDiscoverySource{}
	now := time.Now()
	src.set([]DiscoveryAnnouncement{{NodeID: "n1", Address: "127.0.0.1:1", SeenAt: now}}, nil)

	mgr := newDiscoveryManagerForTest(t, src, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = mgr.Stop() }()

	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return ok
	})

	mgr.RemoveNode("n1")
	if _, ok := mgr.GetNode("n1"); ok {
		t.Fatal("expected node to be removed")
	}

	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return ok
	})
}

func TestDiscoveryHeartbeatDoesNotRefreshPresence(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	hbCfg := DefaultHeartbeatConfig("local")
	hbCfg.Interval = 10 * time.Millisecond
	hbCfg.Timeout = 5 * time.Millisecond
	hb, err := NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		t.Fatalf("new heartbeat monitor failed: %v", err)
	}

	cfg := DefaultDiscoveryConfig()
	cfg.Interval = 500 * time.Millisecond
	cfg.CleanupInterval = 10 * time.Millisecond
	cfg.Expiration = 40 * time.Millisecond
	cfg.HealthSyncInterval = 10 * time.Millisecond
	cfg.StartHeartbeat = true

	mgr, err := NewDiscoveryManager(cfg, nil, hb, nil)
	if err != nil {
		t.Fatalf("new discovery manager failed: %v", err)
	}

	if err := mgr.RegisterNode(NodeEndpoint{NodeID: "n1", Address: "127.0.0.1:1"}, nil); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = mgr.Stop() }()

	waitFor(t, time.Second, func() bool {
		_, ok := mgr.GetNode("n1")
		return !ok
	})
}

func TestDiscoveryConcurrentStartStopSafety(t *testing.T) {
	mgr := newDiscoveryManagerForTest(t, nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = mgr.Start(context.Background())
		}()
		go func() {
			defer wg.Done()
			_ = mgr.Stop()
		}()
	}

	wg.Wait()
	_ = mgr.Stop()
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition not satisfied before timeout")
}
