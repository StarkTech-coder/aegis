package aegis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingLeaderStore struct {
	base            *InMemoryLeaderStore
	unregisterCalls atomic.Int32
	registerCalls   atomic.Int32
}

func newCountingLeaderStore() *countingLeaderStore {
	return &countingLeaderStore{base: NewInMemoryLeaderStore()}
}

func (s *countingLeaderStore) GetLeader(ctx context.Context) (LeaderRecord, error) {
	return s.base.GetLeader(ctx)
}

func (s *countingLeaderStore) CompareAndSwapLeader(ctx context.Context, expectedTerm uint64, expectedLeaderID string, newRecord LeaderRecord) (bool, error) {
	return s.base.CompareAndSwapLeader(ctx, expectedTerm, expectedLeaderID, newRecord)
}

func (s *countingLeaderStore) RegisterNode(ctx context.Context, nodeID string) error {
	s.registerCalls.Add(1)
	return s.base.RegisterNode(ctx, nodeID)
}

func (s *countingLeaderStore) UnregisterNode(ctx context.Context, nodeID string) error {
	s.unregisterCalls.Add(1)
	return s.base.UnregisterNode(ctx, nodeID)
}

func (s *countingLeaderStore) ListNodes(ctx context.Context) ([]string, error) {
	return s.base.ListNodes(ctx)
}

func newElectorForTest(t *testing.T, nodeID string, store LeaderStore, discovery *DiscoveryManager, heartbeat *HeartbeatMonitor) *LeaderElector {
	t.Helper()

	cfg := DefaultLeaderElectionConfig(nodeID)
	cfg.ElectionInterval = 20 * time.Millisecond
	cfg.LeaderLease = 100 * time.Millisecond
	cfg.CampaignTimeout = 40 * time.Millisecond

	e, err := NewLeaderElector(cfg, store, discovery, heartbeat, nil)
	if err != nil {
		t.Fatalf("new leader elector failed: %v", err)
	}

	return e
}

func TestLeaderStoreCASRejectsStaleTerm(t *testing.T) {
	store := NewInMemoryLeaderStore()
	ctx := context.Background()

	if err := store.RegisterNode(ctx, "n1"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	ok, err := store.CompareAndSwapLeader(ctx, 0, "", LeaderRecord{LeaderID: "n1", Term: 1, LeaseExpiresAt: time.Now().Add(time.Second)})
	if err != nil || !ok {
		t.Fatalf("initial CAS failed: ok=%v err=%v", ok, err)
	}

	ok, err = store.CompareAndSwapLeader(ctx, 0, "", LeaderRecord{LeaderID: "n2", Term: 1, LeaseExpiresAt: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatalf("stale CAS returned error: %v", err)
	}
	if ok {
		t.Fatal("expected stale CAS to be rejected")
	}
}

func TestLeaderElectionSingleLeader(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = e.Stop() }()

	waitFor(t, time.Second, func() bool {
		return e.IsLeader()
	})

	leader := e.CurrentLeader()
	if leader.LeaderID != "node-a" {
		t.Fatalf("expected node-a as leader, got %q", leader.LeaderID)
	}
}

func TestLeaderElectionMultipleNodesSingleLeaderConsistency(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	e2 := newElectorForTest(t, "node-b", store, nil, nil)
	e3 := newElectorForTest(t, "node-c", store, nil, nil)

	for _, e := range []*LeaderElector{e1, e2, e3} {
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		defer func(el *LeaderElector) { _ = el.Stop() }(e)
	}

	waitFor(t, time.Second, func() bool {
		l1 := e1.CurrentLeader().LeaderID
		l2 := e2.CurrentLeader().LeaderID
		l3 := e3.CurrentLeader().LeaderID
		return l1 != "" && l1 == l2 && l2 == l3
	})

	leader := e1.CurrentLeader().LeaderID
	if leader != "node-a" {
		t.Fatalf("expected deterministic winner node-a, got %q", leader)
	}

	leaders := 0
	for _, e := range []*LeaderElector{e1, e2, e3} {
		if e.IsLeader() {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("expected exactly one local leader view, got %d", leaders)
	}
}

func TestLeaderFailureTriggersReelection(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	e2 := newElectorForTest(t, "node-b", store, nil, nil)

	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("start e1 failed: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("start e2 failed: %v", err)
	}
	defer func() { _ = e2.Stop() }()

	waitFor(t, time.Second, func() bool {
		return e1.IsLeader()
	})

	if err := e1.Stop(); err != nil {
		t.Fatalf("stop e1 failed: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		return e2.CurrentLeader().LeaderID == "node-b" && e2.IsLeader()
	})

	if e1.IsLeader() {
		t.Fatal("expected resigning node-a to step down")
	}
}

func TestLeaderResignationDoesNotImmediatelyReclaim(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	e2 := newElectorForTest(t, "node-b", store, nil, nil)
	e3 := newElectorForTest(t, "node-c", store, nil, nil)

	for _, e := range []*LeaderElector{e1, e2, e3} {
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		defer func(el *LeaderElector) { _ = el.Stop() }(e)
	}

	waitFor(t, time.Second, func() bool { return e1.IsLeader() })

	ok, err := e1.Resign(context.Background())
	if err != nil {
		t.Fatalf("resign failed: %v", err)
	}
	if !ok {
		t.Fatal("expected resignation to succeed")
	}

	waitFor(t, time.Second, func() bool {
		leader := e2.CurrentLeader().LeaderID
		return (leader == "node-b" || leader == "node-c") && !e1.IsLeader()
	})

	deadline := time.Now().Add(70 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e1.IsLeader() {
			t.Fatal("expected resigning node-a not to immediately reclaim leadership")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLeaderElectionConcurrentResignationAndCampaign(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	e2 := newElectorForTest(t, "node-b", store, nil, nil)
	e3 := newElectorForTest(t, "node-c", store, nil, nil)

	for _, e := range []*LeaderElector{e1, e2, e3} {
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		defer func(el *LeaderElector) { _ = el.Stop() }(e)
	}

	waitFor(t, time.Second, func() bool { return e1.IsLeader() })

	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			_, err := e1.Resign(context.Background())
			errCh <- err
		}()
	}

	for i := 0; i < 8; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent resign failed: %v", err)
		}
	}

	waitFor(t, time.Second, func() bool {
		leader := e2.CurrentLeader().LeaderID
		return (leader == "node-b" || leader == "node-c") && !e1.IsLeader()
	})

	leaders := 0
	for _, e := range []*LeaderElector{e1, e2, e3} {
		if e.IsLeader() {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("expected exactly one leader view after concurrent resignation, got %d", leaders)
	}
}

func TestLeaderResignationTriggersNewLeader(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	e2 := newElectorForTest(t, "node-b", store, nil, nil)

	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("start e1 failed: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("start e2 failed: %v", err)
	}
	defer func() {
		_ = e1.Stop()
		_ = e2.Stop()
	}()

	waitFor(t, time.Second, func() bool { return e1.IsLeader() })

	ok, err := e1.Resign(context.Background())
	if err != nil {
		t.Fatalf("resign failed: %v", err)
	}
	if !ok {
		t.Fatal("expected resignation to succeed")
	}

	waitFor(t, time.Second, func() bool {
		return e2.CurrentLeader().LeaderID == "node-b" && e2.IsLeader()
	})
}

func TestLeaderElectionNodeJoinLeave(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e1 := newElectorForTest(t, "node-a", store, nil, nil)
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("start e1 failed: %v", err)
	}
	defer func() { _ = e1.Stop() }()

	e2 := newElectorForTest(t, "node-b", store, nil, nil)
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("start e2 failed: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			return false
		}
		return len(nodes) == 2
	})

	if err := e2.Stop(); err != nil {
		t.Fatalf("stop e2 failed: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			return false
		}
		return len(nodes) == 1 && nodes[0] == "node-a"
	})
}

func TestLeaderElectionConcurrentStartNoSplitBrain(t *testing.T) {
	store := NewInMemoryLeaderStore()
	electors := []*LeaderElector{
		newElectorForTest(t, "node-a", store, nil, nil),
		newElectorForTest(t, "node-b", store, nil, nil),
		newElectorForTest(t, "node-c", store, nil, nil),
		newElectorForTest(t, "node-d", store, nil, nil),
	}

	for _, e := range electors {
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		defer func(el *LeaderElector) { _ = el.Stop() }(e)
	}

	waitFor(t, time.Second, func() bool {
		rec, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		return rec.LeaderID != ""
	})

	leaders := 0
	for _, e := range electors {
		if e.IsLeader() {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("expected one leader, got %d", leaders)
	}
}

func TestLeaderElectionContextCancellationAndShutdown(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	if err := e.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	cancel()
	if err := e.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestLeaderElectionWithDiscoveryAndHeartbeatUnhealthyLeaderReplaced(t *testing.T) {
	var probeMu sync.RWMutex
	probeAlive := map[string]bool{"node-a": true, "node-b": true}
	probe := HeartbeatProbeFunc(func(_ context.Context, target NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		probeMu.RLock()
		alive := probeAlive[target.NodeID]
		probeMu.RUnlock()
		if !alive {
			return HeartbeatResponse{}, errors.New("unhealthy")
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
		t.Fatalf("heartbeat monitor create failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-a", Address: "a"}); err != nil {
		t.Fatalf("add node-a heartbeat failed: %v", err)
	}
	if err := hb.AddNode(NodeEndpoint{NodeID: "node-b", Address: "b"}); err != nil {
		t.Fatalf("add node-b heartbeat failed: %v", err)
	}
	if err := hb.Start(context.Background()); err != nil {
		t.Fatalf("heartbeat start failed: %v", err)
	}
	defer func() { _ = hb.Stop() }()

	dCfg := DefaultDiscoveryConfig()
	dCfg.StartHeartbeat = false
	disc, err := NewDiscoveryManager(dCfg, nil, hb, nil)
	if err != nil {
		t.Fatalf("discovery manager create failed: %v", err)
	}
	if err := disc.RegisterNode(NodeEndpoint{NodeID: "node-a", Address: "a"}, nil); err != nil {
		t.Fatalf("discovery register node-a failed: %v", err)
	}
	if err := disc.RegisterNode(NodeEndpoint{NodeID: "node-b", Address: "b"}, nil); err != nil {
		t.Fatalf("discovery register node-b failed: %v", err)
	}
	if err := disc.Start(context.Background()); err != nil {
		t.Fatalf("discovery start failed: %v", err)
	}
	defer func() { _ = disc.Stop() }()

	store := NewInMemoryLeaderStore()
	eA := newElectorForTest(t, "node-a", store, disc, hb)
	eB := newElectorForTest(t, "node-b", store, disc, hb)

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

	waitFor(t, time.Second, func() bool {
		return eA.CurrentLeader().LeaderID == "node-a"
	})

	probeMu.Lock()
	probeAlive["node-a"] = false
	probeMu.Unlock()

	waitFor(t, time.Second, func() bool {
		n, ok := disc.GetNode("node-a")
		return ok && n.Health == NodeHealthUnhealthy
	})

	waitFor(t, time.Second, func() bool {
		return eB.CurrentLeader().LeaderID == "node-b" && eB.IsLeader()
	})
}

func TestLeaderElectionNodeRemovalDuringElection(t *testing.T) {
	store := NewInMemoryLeaderStore()
	eA := newElectorForTest(t, "node-a", store, nil, nil)
	eB := newElectorForTest(t, "node-b", store, nil, nil)

	if err := eA.Start(context.Background()); err != nil {
		t.Fatalf("start eA failed: %v", err)
	}
	if err := eB.Start(context.Background()); err != nil {
		t.Fatalf("start eB failed: %v", err)
	}
	defer func() { _ = eA.Stop() }()

	waitFor(t, time.Second, func() bool {
		return eA.IsLeader()
	})

	if err := eA.Stop(); err != nil {
		t.Fatalf("stop eA failed: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		return eB.IsLeader() && eB.CurrentLeader().LeaderID == "node-b"
	})

	if err := eB.Stop(); err != nil {
		t.Fatalf("stop eB failed: %v", err)
	}
}

func TestLeaderElectionConcurrentNodeRegistrationAndElection(t *testing.T) {
	store := NewInMemoryLeaderStore()

	first := newElectorForTest(t, "node-a", store, nil, nil)
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first failed: %v", err)
	}
	defer func() { _ = first.Stop() }()

	others := []*LeaderElector{
		newElectorForTest(t, "node-b", store, nil, nil),
		newElectorForTest(t, "node-c", store, nil, nil),
		newElectorForTest(t, "node-d", store, nil, nil),
	}

	for _, e := range others {
		e := e
		go func() {
			_ = e.Start(context.Background())
		}()
		defer func(el *LeaderElector) { _ = el.Stop() }(e)
	}

	waitFor(t, time.Second, func() bool {
		rec, err := store.GetLeader(context.Background())
		if err != nil {
			return false
		}
		if rec.LeaderID == "" {
			return false
		}

		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			return false
		}

		return len(nodes) >= 2
	})
}

func TestLeaderElectionCannotRestartAfterStop(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if err := e.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if err := e.Start(context.Background()); err == nil {
		t.Fatal("expected restart after stop to fail")
	}
}

func TestLeaderElectionLeaveUnregistersOnlyOnceWhenLeader(t *testing.T) {
	store := newCountingLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	waitFor(t, time.Second, func() bool { return e.IsLeader() })

	if err := e.Leave(context.Background()); err != nil {
		t.Fatalf("leave failed: %v", err)
	}

	if got := store.unregisterCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one unregister call, got %d", got)
	}

	_ = e.Stop()
}

func TestLeaderElectionNodeEligibilityPolicy(t *testing.T) {
	probe := HeartbeatProbeFunc(func(_ context.Context, _ NodeEndpoint, _ HeartbeatMessage) (HeartbeatResponse, error) {
		return HeartbeatResponse{}, nil
	})

	hbCfg := DefaultHeartbeatConfig("local")
	hbCfg.SuspectThreshold = 1
	hbCfg.UnhealthyThreshold = 1
	hb, err := NewHeartbeatMonitor(hbCfg, probe, nil)
	if err != nil {
		t.Fatalf("heartbeat monitor create failed: %v", err)
	}

	disc, err := NewDiscoveryManager(DefaultDiscoveryConfig(), nil, hb, nil)
	if err != nil {
		t.Fatalf("discovery manager create failed: %v", err)
	}

	e := newElectorForTest(t, "local", NewInMemoryLeaderStore(), disc, hb)

	if !e.nodeEligible("local") {
		t.Fatal("expected local node to be eligible")
	}

	if e.nodeEligible("remote-missing") {
		t.Fatal("expected remote node missing from discovery and heartbeat to be ineligible")
	}

	if err := hb.AddNode(NodeEndpoint{NodeID: "remote", Address: "addr"}); err != nil {
		t.Fatalf("add heartbeat node failed: %v", err)
	}

	hb.applyProbeFailure("remote", errors.New("down"))
	if e.nodeEligible("remote") {
		t.Fatal("expected unhealthy heartbeat node to be ineligible")
	}

	hbCfg2 := DefaultHeartbeatConfig("local")
	hbCfg2.SuspectThreshold = 1
	hbCfg2.UnhealthyThreshold = 2
	hb2, err := NewHeartbeatMonitor(hbCfg2, probe, nil)
	if err != nil {
		t.Fatalf("heartbeat monitor create failed: %v", err)
	}
	if err := hb2.AddNode(NodeEndpoint{NodeID: "remote-suspect", Address: "addr"}); err != nil {
		t.Fatalf("add heartbeat node failed: %v", err)
	}
	hb2.applyProbeFailure("remote-suspect", errors.New("miss"))

	disc2, err := NewDiscoveryManager(DefaultDiscoveryConfig(), nil, hb2, nil)
	if err != nil {
		t.Fatalf("discovery manager create failed: %v", err)
	}
	e2 := newElectorForTest(t, "local", NewInMemoryLeaderStore(), disc2, hb2)
	if !e2.nodeEligible("remote-suspect") {
		t.Fatal("expected suspect heartbeat node to remain eligible")
	}

	hb2.applyProbeSuccess("remote-suspect")
	if !e2.nodeEligible("remote-suspect") {
		t.Fatal("expected healthy heartbeat node to be eligible")
	}
}

func TestLeaderElectionConcurrentMembershipOperationsSafe(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = e.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = e.Join(context.Background())
				_, _ = e.Resign(context.Background())
				_ = e.Leave(context.Background())
			}
		}()
	}

	wg.Wait()
}

func TestLeaderElectionConcurrentStopAndResignSafe(t *testing.T) {
	store := NewInMemoryLeaderStore()
	e := newElectorForTest(t, "node-a", store, nil, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	waitFor(t, time.Second, func() bool { return e.IsLeader() })

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Resign(context.Background())
		}()
	}

	if err := e.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	wg.Wait()
}
