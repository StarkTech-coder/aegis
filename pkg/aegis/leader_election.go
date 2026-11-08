package aegis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// LeaderRecord is the shared leader state stored in an election backend.
type LeaderRecord struct {
	LeaderID       string
	Term           uint64
	LeaseExpiresAt time.Time
	UpdatedAt      time.Time
}

// LeadershipChangeEvent describes a leader transition observed by an elector.
type LeadershipChangeEvent struct {
	Term          uint64
	LeaderID      string
	LocalIsLeader bool
	ChangedAt     time.Time
	Reason        string
}

// LeaderElectionConfig controls election timing and behavior.
type LeaderElectionConfig struct {
	LocalNodeID           string
	ElectionInterval      time.Duration
	LeaderLease           time.Duration
	CampaignTimeout       time.Duration
	LeadershipEventBuffer int
}

// DefaultLeaderElectionConfig returns baseline settings.
func DefaultLeaderElectionConfig(localNodeID string) LeaderElectionConfig {
	return LeaderElectionConfig{
		LocalNodeID:           localNodeID,
		ElectionInterval:      300 * time.Millisecond,
		LeaderLease:           1200 * time.Millisecond,
		CampaignTimeout:       250 * time.Millisecond,
		LeadershipEventBuffer: 16,
	}
}

func (c *LeaderElectionConfig) sanitize() {
	if c == nil {
		return
	}

	if c.ElectionInterval <= 0 {
		c.ElectionInterval = 300 * time.Millisecond
	}

	if c.LeaderLease <= 0 {
		c.LeaderLease = 1200 * time.Millisecond
	}

	if c.CampaignTimeout <= 0 {
		c.CampaignTimeout = 250 * time.Millisecond
	}

	if c.LeadershipEventBuffer <= 0 {
		c.LeadershipEventBuffer = 16
	}
}

// Validate checks election configuration bounds.
func (c LeaderElectionConfig) Validate() error {
	if c.LocalNodeID == "" {
		return errors.New("aegis-election: local node id cannot be empty")
	}

	if c.ElectionInterval <= 0 {
		return errors.New("aegis-election: election interval must be greater than zero")
	}

	if c.LeaderLease <= 0 {
		return errors.New("aegis-election: leader lease must be greater than zero")
	}

	if c.LeaderLease < c.ElectionInterval {
		return errors.New("aegis-election: leader lease cannot be shorter than election interval")
	}

	if c.CampaignTimeout <= 0 {
		return errors.New("aegis-election: campaign timeout must be greater than zero")
	}

	if c.LeadershipEventBuffer <= 0 {
		return errors.New("aegis-election: leadership event buffer must be greater than zero")
	}

	return nil
}

// LeaderStore is a transport-agnostic coordination backend for election state.
// Consistency guarantees depend on the concrete implementation.
type LeaderStore interface {
	GetLeader(ctx context.Context) (LeaderRecord, error)
	CompareAndSwapLeader(ctx context.Context, expectedTerm uint64, expectedLeaderID string, newRecord LeaderRecord) (bool, error)
	RegisterNode(ctx context.Context, nodeID string) error
	UnregisterNode(ctx context.Context, nodeID string) error
	ListNodes(ctx context.Context) ([]string, error)
}

// InMemoryLeaderStore is a process-local coordination backend for testing and examples.
// It does not provide distributed fault tolerance by itself.
type InMemoryLeaderStore struct {
	mu      sync.Mutex
	leader  LeaderRecord
	nodes   map[string]struct{}
	nowFunc func() time.Time
}

// NewInMemoryLeaderStore creates a process-local leader store.
func NewInMemoryLeaderStore() *InMemoryLeaderStore {
	return &InMemoryLeaderStore{
		nodes:   make(map[string]struct{}),
		nowFunc: time.Now,
	}
}

// GetLeader returns the current leader record.
func (s *InMemoryLeaderStore) GetLeader(_ context.Context) (LeaderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader, nil
}

// CompareAndSwapLeader atomically updates leader state if expected term and leader match.
func (s *InMemoryLeaderStore) CompareAndSwapLeader(_ context.Context, expectedTerm uint64, expectedLeaderID string, newRecord LeaderRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leader.Term != expectedTerm || s.leader.LeaderID != expectedLeaderID {
		return false, nil
	}

	if newRecord.UpdatedAt.IsZero() {
		newRecord.UpdatedAt = s.nowFunc()
	}

	s.leader = newRecord
	return true, nil
}

// RegisterNode registers a participant.
func (s *InMemoryLeaderStore) RegisterNode(_ context.Context, nodeID string) error {
	if nodeID == "" {
		return errors.New("aegis-election: node id cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[nodeID] = struct{}{}
	return nil
}

// UnregisterNode unregisters a participant.
func (s *InMemoryLeaderStore) UnregisterNode(_ context.Context, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, nodeID)
	return nil
}

// ListNodes returns sorted participant node ids.
func (s *InMemoryLeaderStore) ListNodes(_ context.Context) ([]string, error) {
	s.mu.Lock()
	out := make([]string, 0, len(s.nodes))
	for nodeID := range s.nodes {
		out = append(out, nodeID)
	}
	s.mu.Unlock()

	sort.Strings(out)
	return out, nil
}

// LeaderElector coordinates leader election with heartbeat and discovery state.
type LeaderElector struct {
	cfg       LeaderElectionConfig
	store     LeaderStore
	discovery *DiscoveryManager
	heartbeat *HeartbeatMonitor
	logger    *slog.Logger
	now       func() time.Time

	mu            sync.RWMutex
	lifecycleMu   sync.Mutex
	membershipMu  sync.Mutex
	running       bool
	terminated    bool
	joined        bool
	runCtx        context.Context
	runCancel     context.CancelFunc
	currentLeader LeaderRecord
	localLeader   bool
	resignedUntil time.Time
	excludedUntil map[string]time.Time
	changes       chan LeadershipChangeEvent
	changesClosed bool
	wg            sync.WaitGroup
}

// NewLeaderElector constructs a leader elector instance.
func NewLeaderElector(cfg LeaderElectionConfig, store LeaderStore, discovery *DiscoveryManager, heartbeat *HeartbeatMonitor, logger *slog.Logger) (*LeaderElector, error) {
	cfg.sanitize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-election: invalid configuration: %w", err)
	}

	if store == nil {
		return nil, errors.New("aegis-election: leader store cannot be nil")
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &LeaderElector{
		cfg:           cfg,
		store:         store,
		discovery:     discovery,
		heartbeat:     heartbeat,
		logger:        logger,
		now:           time.Now,
		excludedUntil: make(map[string]time.Time),
		changes:       make(chan LeadershipChangeEvent, cfg.LeadershipEventBuffer),
	}, nil
}

// Start starts the election loop and joins the participant set.
func (e *LeaderElector) Start(ctx context.Context) error {
	if e == nil {
		return errors.New("aegis-election: elector is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()

	e.mu.Lock()
	if e.terminated {
		e.mu.Unlock()
		return errors.New("aegis-election: elector cannot be restarted after shutdown")
	}

	if e.running {
		e.mu.Unlock()
		return errors.New("aegis-election: elector is already running")
	}

	e.runCtx, e.runCancel = context.WithCancel(ctx)
	e.running = true
	e.mu.Unlock()

	joinCtx, cancel := context.WithTimeout(e.runCtx, e.cfg.CampaignTimeout)
	joinErr := e.Join(joinCtx)
	cancel()
	if joinErr != nil {
		_ = e.Stop()
		return joinErr
	}

	e.wg.Add(1)
	go e.runLoop()

	return nil
}

// Stop stops election loop and leaves election safely.
func (e *LeaderElector) Stop() error {
	if e == nil {
		return nil
	}

	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()

	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}

	cancel := e.runCancel
	e.running = false
	e.runCtx = nil
	e.runCancel = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	e.wg.Wait()

	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), e.cfg.CampaignTimeout)
	leaveErr := e.Leave(leaveCtx)
	leaveCancel()

	e.mu.Lock()
	if !e.changesClosed {
		close(e.changes)
		e.changesClosed = true
	}
	e.terminated = true
	e.mu.Unlock()

	return leaveErr
}

// Join registers the local node as a participant.
func (e *LeaderElector) Join(ctx context.Context) error {
	if e == nil {
		return errors.New("aegis-election: elector is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	e.membershipMu.Lock()
	defer e.membershipMu.Unlock()

	e.mu.RLock()
	alreadyJoined := e.joined
	e.mu.RUnlock()
	if alreadyJoined {
		return nil
	}

	if err := e.store.RegisterNode(ctx, e.cfg.LocalNodeID); err != nil {
		return fmt.Errorf("aegis-election: failed to join election: %w", err)
	}

	e.mu.Lock()
	e.joined = true
	e.mu.Unlock()

	return nil
}

// Leave resigns if needed and unregisters the local node.
func (e *LeaderElector) Leave(ctx context.Context) error {
	if e == nil {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	e.membershipMu.Lock()
	defer e.membershipMu.Unlock()

	e.mu.RLock()
	joined := e.joined
	e.mu.RUnlock()
	if !joined {
		return nil
	}

	if _, err := e.resignLocked(ctx); err != nil {
		return err
	}

	e.mu.RLock()
	joined = e.joined
	e.mu.RUnlock()
	if !joined {
		return nil
	}

	if err := e.store.UnregisterNode(ctx, e.cfg.LocalNodeID); err != nil {
		return fmt.Errorf("aegis-election: failed to leave election: %w", err)
	}

	e.mu.Lock()
	e.joined = false
	e.mu.Unlock()

	return nil
}

// LeadershipChanges exposes leadership transition notifications.
func (e *LeaderElector) LeadershipChanges() <-chan LeadershipChangeEvent {
	if e == nil {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.changes
}

// CurrentLeader returns the latest observed leader record.
func (e *LeaderElector) CurrentLeader() LeaderRecord {
	if e == nil {
		return LeaderRecord{}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentLeader
}

// IsLeader reports whether this elector currently believes local node is leader.
func (e *LeaderElector) IsLeader() bool {
	if e == nil {
		return false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.localLeader
}

// Resign attempts graceful leadership resignation.
func (e *LeaderElector) Resign(ctx context.Context) (bool, error) {
	if e == nil {
		return false, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	e.membershipMu.Lock()
	defer e.membershipMu.Unlock()

	return e.resignLocked(ctx)
}

func (e *LeaderElector) resignLocked(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rec, err := e.store.GetLeader(ctx)
	if err != nil {
		return false, fmt.Errorf("aegis-election: failed reading leader for resignation: %w", err)
	}

	if rec.LeaderID != e.cfg.LocalNodeID {
		return false, nil
	}

	now := e.now()
	newRec := LeaderRecord{LeaderID: "", Term: rec.Term + 1, UpdatedAt: now}
	ok, err := e.store.CompareAndSwapLeader(ctx, rec.Term, rec.LeaderID, newRec)
	if err != nil {
		return false, fmt.Errorf("aegis-election: failed to resign leadership: %w", err)
	}

	if ok {
		if err := e.store.UnregisterNode(ctx, e.cfg.LocalNodeID); err != nil {
			return false, fmt.Errorf("aegis-election: failed to unregister during resignation: %w", err)
		}

		e.mu.Lock()
		e.joined = false
		e.resignedUntil = now.Add(e.cfg.LeaderLease)
		e.mu.Unlock()
		e.applyLeaderState(newRec, "resigned")
	}

	return ok, nil
}

func (e *LeaderElector) runLoop() {
	defer e.wg.Done()

	_ = e.reconcile()

	ticker := time.NewTicker(e.cfg.ElectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := e.reconcile(); err != nil && !errors.Is(err, context.Canceled) {
				e.logger.Debug("leader election reconcile failed", "error", err)
			}
		case <-e.done():
			return
		}
	}
}

func (e *LeaderElector) done() <-chan struct{} {
	e.mu.RLock()
	ctx := e.runCtx
	e.mu.RUnlock()

	if ctx == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return ctx.Done()
}

func (e *LeaderElector) reconcile() error {
	ctx, cancel := context.WithTimeout(e.contextOrBackground(), e.cfg.CampaignTimeout)
	defer cancel()

	now := e.now()
	if err := e.maybeRejoinAfterCooldown(ctx, now); err != nil {
		return err
	}

	current, err := e.store.GetLeader(ctx)
	if err != nil {
		return fmt.Errorf("aegis-election: failed to read leader record: %w", err)
	}

	leaseValid := current.LeaderID != "" && now.Before(current.LeaseExpiresAt)
	leaderHealthy := e.nodeEligible(current.LeaderID)

	if leaseValid && leaderHealthy {
		if current.LeaderID == e.cfg.LocalNodeID {
			return e.renewLeadership(ctx, current)
		}

		e.applyLeaderState(current, "leader-observed")
		return nil
	}

	participants, err := e.store.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("aegis-election: failed to list participants: %w", err)
	}

	eligible := make([]string, 0, len(participants))
	for _, nodeID := range participants {
		if e.candidateEligible(nodeID, now) {
			eligible = append(eligible, nodeID)
		}
	}

	if len(eligible) == 0 {
		e.applyLeaderState(current, "no-eligible-candidates")
		return nil
	}

	sort.Strings(eligible)
	winner := eligible[0]
	if winner != e.cfg.LocalNodeID {
		e.applyLeaderState(current, "candidate-waiting")
		return nil
	}

	newRec := LeaderRecord{
		LeaderID:       e.cfg.LocalNodeID,
		Term:           current.Term + 1,
		LeaseExpiresAt: now.Add(e.cfg.LeaderLease),
		UpdatedAt:      now,
	}

	ok, err := e.store.CompareAndSwapLeader(ctx, current.Term, current.LeaderID, newRec)
	if err != nil {
		return fmt.Errorf("aegis-election: failed to campaign for leadership: %w", err)
	}

	if !ok {
		latest, getErr := e.store.GetLeader(ctx)
		if getErr == nil {
			e.applyLeaderState(latest, "campaign-conflict")
		}
		return nil
	}

	e.applyLeaderState(newRec, "elected")
	return nil
}

func (e *LeaderElector) renewLeadership(ctx context.Context, current LeaderRecord) error {
	now := e.now()
	newRec := LeaderRecord{
		LeaderID:       e.cfg.LocalNodeID,
		Term:           current.Term,
		LeaseExpiresAt: now.Add(e.cfg.LeaderLease),
		UpdatedAt:      now,
	}

	ok, err := e.store.CompareAndSwapLeader(ctx, current.Term, current.LeaderID, newRec)
	if err != nil {
		return fmt.Errorf("aegis-election: failed to renew leader lease: %w", err)
	}

	if !ok {
		latest, getErr := e.store.GetLeader(ctx)
		if getErr == nil {
			e.applyLeaderState(latest, "renew-conflict")
		} else {
			e.applyLeaderState(LeaderRecord{}, "renew-conflict-unknown")
		}
		return nil
	}

	e.applyLeaderState(newRec, "lease-renewed")
	return nil
}

func (e *LeaderElector) maybeRejoinAfterCooldown(ctx context.Context, now time.Time) error {
	e.membershipMu.Lock()
	defer e.membershipMu.Unlock()

	e.mu.RLock()
	joined := e.joined
	resignedUntil := e.resignedUntil
	e.mu.RUnlock()

	if joined || now.Before(resignedUntil) {
		return nil
	}

	if err := e.store.RegisterNode(ctx, e.cfg.LocalNodeID); err != nil {
		return fmt.Errorf("aegis-election: failed to rejoin election after resignation: %w", err)
	}

	e.mu.Lock()
	e.joined = true
	e.resignedUntil = time.Time{}
	e.mu.Unlock()

	return nil
}

func (e *LeaderElector) candidateEligible(nodeID string, now time.Time) bool {
	if nodeID == "" {
		return false
	}

	e.mu.RLock()
	excludedUntil := e.excludedUntil[nodeID]
	resignedUntil := e.resignedUntil
	e.mu.RUnlock()

	if !excludedUntil.IsZero() && now.Before(excludedUntil) {
		return false
	}

	if nodeID == e.cfg.LocalNodeID {
		if now.Before(resignedUntil) {
			return false
		}
	}

	return e.nodeEligible(nodeID)
}

func (e *LeaderElector) excludeCandidate(nodeID string, duration time.Duration) {
	if e == nil || nodeID == "" || duration <= 0 {
		return
	}

	until := e.now().Add(duration)

	e.mu.Lock()
	current := e.excludedUntil[nodeID]
	if current.IsZero() || until.After(current) {
		e.excludedUntil[nodeID] = until
	}
	e.mu.Unlock()
}

func (e *LeaderElector) nodeEligible(nodeID string) bool {
	if nodeID == "" {
		return false
	}

	if nodeID == e.cfg.LocalNodeID {
		return true
	}

	hasSignal := false

	if e.discovery != nil {
		node, ok := e.discovery.GetNode(nodeID)
		if ok {
			hasSignal = true
			if node.Health == NodeHealthUnhealthy {
				return false
			}
		}
	}

	if e.heartbeat != nil {
		health, ok := e.heartbeat.GetNodeHealth(nodeID)
		if ok {
			hasSignal = true
			if health.State == NodeHealthUnhealthy {
				return false
			}
		}
	}

	if e.discovery != nil || e.heartbeat != nil {
		return hasSignal
	}

	return true
}

func (e *LeaderElector) applyLeaderState(rec LeaderRecord, reason string) {
	e.mu.Lock()
	previous := e.currentLeader
	prevLocal := e.localLeader
	e.currentLeader = rec
	e.localLeader = rec.LeaderID == e.cfg.LocalNodeID && rec.LeaderID != ""
	changed := previous.LeaderID != rec.LeaderID || previous.Term != rec.Term || prevLocal != e.localLeader
	e.mu.Unlock()

	if changed {
		e.emitChange(LeadershipChangeEvent{
			Term:          rec.Term,
			LeaderID:      rec.LeaderID,
			LocalIsLeader: rec.LeaderID == e.cfg.LocalNodeID && rec.LeaderID != "",
			ChangedAt:     e.now(),
			Reason:        reason,
		})
	}
}

func (e *LeaderElector) emitChange(evt LeadershipChangeEvent) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.changes == nil || e.changesClosed {
		return
	}

	select {
	case e.changes <- evt:
	default:
		e.logger.Warn("leadership change notification channel is full", "leader_id", evt.LeaderID, "term", evt.Term)
	}
}

func (e *LeaderElector) contextOrBackground() context.Context {
	e.mu.RLock()
	ctx := e.runCtx
	e.mu.RUnlock()

	if ctx == nil {
		return context.Background()
	}

	return ctx
}
