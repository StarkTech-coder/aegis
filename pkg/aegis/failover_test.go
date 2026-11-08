package aegis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type failOnceClearStore struct {
	base LeaderStore
	mu   sync.Mutex
	used bool
}

func (s *failOnceClearStore) GetLeader(ctx context.Context) (LeaderRecord, error) {
	return s.base.GetLeader(ctx)
}

func (s *failOnceClearStore) CompareAndSwapLeader(ctx context.Context, expectedTerm uint64, expectedLeaderID string, newRecord LeaderRecord) (bool, error) {
	s.mu.Lock()
	if !s.used && newRecord.LeaderID == "" {
		s.used = true
		s.mu.Unlock()
		return false, errors.New("injected clear failure")
	}
	s.mu.Unlock()
	return s.base.CompareAndSwapLeader(ctx, expectedTerm, expectedLeaderID, newRecord)
}

func (s *failOnceClearStore) RegisterNode(ctx context.Context, nodeID string) error {
	return s.base.RegisterNode(ctx, nodeID)
}

func (s *failOnceClearStore) UnregisterNode(ctx context.Context, nodeID string) error {
	return s.base.UnregisterNode(ctx, nodeID)
}

func (s *failOnceClearStore) ListNodes(ctx context.Context) ([]string, error) {
	return s.base.ListNodes(ctx)
}

func newFailoverManagerForTest(t *testing.T, elector *LeaderElector, store LeaderStore, disc *DiscoveryManager, hb *HeartbeatMonitor) *FailoverManager {
	t.Helper()

	cfg := DefaultFailoverConfig()
	cfg.CheckInterval = 20 * time.Millisecond
	cfg.OperationTimeout = 30 * time.Millisecond
	cfg.HandlerTimeout = 100 * time.Millisecond

	m, err := NewFailoverManager(cfg, elector, store, disc, hb, nil)
	if err != nil {
		t.Fatalf("new failover manager failed: %v", err)
	}
	return m
}

func setLeaderForTest(t *testing.T, store LeaderStore, rec LeaderRecord) {
	t.Helper()
	ctx := context.Background()
	cur, err := store.GetLeader(ctx)
	if err != nil {
		t.Fatalf("get leader failed: %v", err)
	}
	ok, err := store.CompareAndSwapLeader(ctx, cur.Term, cur.LeaderID, rec)
	if err != nil {
		t.Fatalf("set leader cas failed: %v", err)
	}
	if !ok {
		t.Fatal("set leader cas rejected")
	}
}

func newHeartbeatForFailoverTest(t *testing.T, unhealthyThreshold int) *HeartbeatMonitor {
	t.Helper()
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})
	cfg := DefaultHeartbeatConfig("local")
	cfg.Interval = 20 * time.Millisecond
	cfg.Timeout = 10 * time.Millisecond
	cfg.SuspectThreshold = 1
	cfg.UnhealthyThreshold = unhealthyThreshold
	hb, err := NewHeartbeatMonitor(cfg, probe, nil)
	if err != nil {
		t.Fatalf("new heartbeat monitor failed: %v", err)
	}
	return hb
}

func TestFailoverHealthyLeaderRemainsLeader(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)

	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add hb node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add hb node-b failed: %v", err)
	}

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 5, LeaseExpiresAt: time.Now().Add(500 * time.Millisecond), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	time.Sleep(120 * time.Millisecond)
	leader, err := store.GetLeader(context.Background())
	if err != nil {
		t.Fatalf("get leader failed: %v", err)
	}
	if leader.LeaderID != "node-a" {
		t.Fatalf("expected healthy leader to remain node-a, got %q", leader.LeaderID)
	}
}

func TestFailoverSuspectLeaderNoFailoverByDefault(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 2)

	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add hb node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add hb node-b failed: %v", err)
	}
	hb.applyProbeFailure("node-a", errors.New("temporary")) // suspect, not unhealthy

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 2, LeaseExpiresAt: time.Now().Add(500 * time.Millisecond), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	time.Sleep(120 * time.Millisecond)
	leader, err := store.GetLeader(context.Background())
	if err != nil {
		t.Fatalf("get leader failed: %v", err)
	}
	if leader.LeaderID != "node-a" {
		t.Fatalf("expected suspect leader to remain node-a by default, got %q", leader.LeaderID)
	}
}

func TestFailoverUnhealthyLeaderTriggersReelection(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)

	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add hb node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add hb node-b failed: %v", err)
	}
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 4, LeaseExpiresAt: time.Now().Add(500 * time.Millisecond), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b"
	})
}

func TestFailoverLeaseExpiryTriggersReelection(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)

	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add hb node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add hb node-b failed: %v", err)
	}

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 7, LeaseExpiresAt: time.Now().Add(-50 * time.Millisecond), UpdatedAt: time.Now().Add(-time.Second)})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b"
	})
}

func TestFailoverFailedAttemptRetries(t *testing.T) {
	base := NewInMemoryLeaderStore()
	store := &failOnceClearStore{base: base}
	hb := newHeartbeatForFailoverTest(t, 1)

	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add hb node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add hb node-b failed: %v", err)
	}
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 1, LeaseExpiresAt: time.Now().Add(500 * time.Millisecond), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b"
	})
}

func TestFailoverRecoveredLeaderDoesNotAutoReclaim(t *testing.T) {
	store := NewInMemoryLeaderStore()
	probeAlive := map[string]bool{"node-a": true, "node-b": true}
	var probeMu sync.RWMutex
	probe := HeartbeatProbeFunc(func(_ context.Context, target NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		probeMu.RLock()
		alive := probeAlive[target.NodeID]
		probeMu.RUnlock()
		if !alive {
			return HeartbeatResponse{}, errors.New("down")
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
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add node-a failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add node-b failed: %v", err)
	}
	if err := hb.Start(context.Background()); err != nil {
		t.Fatalf("heartbeat start failed: %v", err)
	}
	defer func() { _ = hb.Stop() }()

	eA := newElectorForTest(t, "node-a", store, nil, hb)
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eA.Start(context.Background()); err != nil {
		t.Fatalf("start eA failed: %v", err)
	}
	if err := eB.Start(context.Background()); err != nil {
		t.Fatalf("start eB failed: %v", err)
	}
	defer func() {
		_ = eA.Stop()
		_ = eB.Stop()
	}()

	waitFor(t, time.Second, func() bool { return eA.CurrentLeader().LeaderID == "node-a" })

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	probeMu.Lock()
	probeAlive["node-a"] = false
	probeMu.Unlock()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b"
	})

	probeMu.Lock()
	probeAlive["node-a"] = true
	probeMu.Unlock()

	time.Sleep(200 * time.Millisecond)
	leader, err := store.GetLeader(context.Background())
	if err != nil {
		t.Fatalf("get leader failed: %v", err)
	}
	if leader.LeaderID != "node-b" {
		t.Fatalf("expected recovered old leader not to reclaim, got %q", leader.LeaderID)
	}
}

func TestFailoverConcurrentAttemptsSafe(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	_ = hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"})
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))
	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 3, LeaseExpiresAt: time.Now().Add(time.Second), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.detectLeaderFailure()
		}()
	}
	wg.Wait()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b"
	})
}

func TestFailoverMultipleNodesFail(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	_ = store.RegisterNode(context.Background(), "node-a")
	_ = store.RegisterNode(context.Background(), "node-b")
	eC := newElectorForTest(t, "node-c", store, nil, hb)
	if err := eC.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eC.Leave(context.Background()) }()

	_ = hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-c", Address: "c"})
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))
	hb.applyProbeFailure("node-b", errors.New("unhealthy"))

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 2, LeaseExpiresAt: time.Now().Add(time.Second), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eC, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-c"
	})
}

func TestFailoverSingleLeaderRecognition(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-c", Address: "c"})

	eB := newElectorForTest(t, "node-b", store, nil, hb)
	eC := newElectorForTest(t, "node-c", store, nil, hb)
	if err := eB.Start(context.Background()); err != nil {
		t.Fatalf("start eB failed: %v", err)
	}
	if err := eC.Start(context.Background()); err != nil {
		t.Fatalf("start eC failed: %v", err)
	}
	defer func() {
		_ = eB.Stop()
		_ = eC.Stop()
	}()

	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 1, LeaseExpiresAt: time.Now().Add(-time.Second), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leaders := 0
		for _, e := range []*LeaderElector{eB, eC} {
			if e.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	})
}

func TestFailoverRoutingHandlerReceivesNewLeader(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	_ = hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"})
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))
	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 2, LeaseExpiresAt: time.Now().Add(time.Second), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	received := make(chan string, 4)
	_, err := m.RegisterHandler(func(_ context.Context, event FailoverEvent) error {
		if event.LeaderID != "" {
			received <- event.LeaderID
		}
		return nil
	})
	if err != nil {
		t.Fatalf("register handler failed: %v", err)
	}

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		select {
		case id := <-received:
			return id == "node-b"
		default:
			return false
		}
	})
}

func TestFailoverHandlerErrorsDoNotCrashManager(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	_ = store.RegisterNode(context.Background(), "node-a")
	eB := newElectorForTest(t, "node-b", store, nil, hb)
	if err := eB.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = eB.Leave(context.Background()) }()

	_ = hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"})
	_ = hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"})
	hb.applyProbeFailure("node-a", errors.New("unhealthy"))
	setLeaderForTest(t, store, LeaderRecord{LeaderID: "node-a", Term: 2, LeaseExpiresAt: time.Now().Add(time.Second), UpdatedAt: time.Now()})

	m := newFailoverManagerForTest(t, eB, store, nil, hb)
	var calls atomic.Int32
	_, _ = m.RegisterHandler(func(_ context.Context, _ FailoverEvent) error {
		return errors.New("handler failure")
	})
	_, _ = m.RegisterHandler(func(_ context.Context, _ FailoverEvent) error {
		calls.Add(1)
		return nil
	})

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failover manager failed: %v", err)
	}
	defer func() { _ = m.Stop() }()

	waitFor(t, time.Second, func() bool {
		leader, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return leader.LeaderID == "node-b" && calls.Load() > 0
	})

	if !m.IsRunning() {
		t.Fatal("expected manager to remain running after handler errors")
	}
}

func TestFailoverContextCancellationAndShutdown(t *testing.T) {
	store := NewInMemoryLeaderStore()
	hb := newHeartbeatForFailoverTest(t, 1)
	e := newElectorForTest(t, "node-a", store, nil, hb)
	if err := e.Join(context.Background()); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	defer func() { _ = e.Leave(context.Background()) }()

	m := newFailoverManagerForTest(t, e, store, nil, hb)
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	cancel()
	if err := m.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if m.IsRunning() {
		t.Fatal("expected manager to be stopped")
	}
}
