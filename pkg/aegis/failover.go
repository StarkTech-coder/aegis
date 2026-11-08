package aegis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// FailoverHandler is invoked when a leader transition is observed.
type FailoverHandler func(ctx context.Context, event FailoverEvent) error

// FailoverEvent describes a routed failover transition.
type FailoverEvent struct {
	PreviousLeaderID string
	LeaderID         string
	Term             uint64
	FencingToken     uint64
	Reason           string
	ChangedAt        time.Time
}

// FailoverConfig controls failover detection and callback behavior.
type FailoverConfig struct {
	CheckInterval         time.Duration
	OperationTimeout      time.Duration
	TreatSuspectAsFailure bool
	EventBuffer           int
	HandlerTimeout        time.Duration
}

// DefaultFailoverConfig returns baseline failover settings.
func DefaultFailoverConfig() FailoverConfig {
	return FailoverConfig{
		CheckInterval:         200 * time.Millisecond,
		OperationTimeout:      150 * time.Millisecond,
		TreatSuspectAsFailure: false,
		EventBuffer:           32,
		HandlerTimeout:        2 * time.Second,
	}
}

func (c *FailoverConfig) sanitize() {
	if c == nil {
		return
	}

	if c.CheckInterval <= 0 {
		c.CheckInterval = 200 * time.Millisecond
	}

	if c.OperationTimeout <= 0 {
		c.OperationTimeout = 150 * time.Millisecond
	}

	if c.EventBuffer <= 0 {
		c.EventBuffer = 32
	}

	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = 2 * time.Second
	}
}

// Validate checks failover configuration bounds.
func (c FailoverConfig) Validate() error {
	if c.CheckInterval <= 0 {
		return errors.New("aegis-failover: check interval must be greater than zero")
	}

	if c.OperationTimeout <= 0 {
		return errors.New("aegis-failover: operation timeout must be greater than zero")
	}

	if c.EventBuffer <= 0 {
		return errors.New("aegis-failover: event buffer must be greater than zero")
	}

	if c.HandlerTimeout <= 0 {
		return errors.New("aegis-failover: handler timeout must be greater than zero")
	}

	return nil
}

// FailoverManager coordinates failure detection and application-level routing updates.
//
// Guarantees depend on the configured LeaderStore semantics. InMemoryLeaderStore is
// process-local and does not provide distributed consistency on its own.
type FailoverManager struct {
	cfg       FailoverConfig
	elector   *LeaderElector
	store     LeaderStore
	discovery *DiscoveryManager
	heartbeat *HeartbeatMonitor
	logger    *slog.Logger
	now       func() time.Time

	lifecycleMu        sync.Mutex
	failoverMu         sync.Mutex
	failoverInProgress bool

	mu            sync.RWMutex
	running       bool
	terminated    bool
	runCtx        context.Context
	runCancel     context.CancelFunc
	active        LeaderRecord
	events        chan FailoverEvent
	changes       chan FailoverEvent
	changesClosed bool
	handlers      map[uint64]FailoverHandler
	nextHandlerID uint64
	wg            sync.WaitGroup
}

// NewFailoverManager constructs a failover manager.
func NewFailoverManager(cfg FailoverConfig, elector *LeaderElector, store LeaderStore, discovery *DiscoveryManager, heartbeat *HeartbeatMonitor, logger *slog.Logger) (*FailoverManager, error) {
	cfg.sanitize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-failover: invalid configuration: %w", err)
	}

	if elector == nil {
		return nil, errors.New("aegis-failover: elector cannot be nil")
	}

	if store == nil {
		return nil, errors.New("aegis-failover: leader store cannot be nil")
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &FailoverManager{
		cfg:       cfg,
		elector:   elector,
		store:     store,
		discovery: discovery,
		heartbeat: heartbeat,
		logger:    logger,
		now:       time.Now,
		events:    make(chan FailoverEvent, cfg.EventBuffer),
		changes:   make(chan FailoverEvent, cfg.EventBuffer),
		handlers:  make(map[uint64]FailoverHandler),
	}, nil
}

// Start starts failover monitoring and routing event dispatch.
func (m *FailoverManager) Start(ctx context.Context) error {
	if m == nil {
		return errors.New("aegis-failover: manager is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.RLock()
	if m.running {
		m.mu.RUnlock()
		return errors.New("aegis-failover: manager is already running")
	}
	if m.terminated {
		m.mu.RUnlock()
		return errors.New("aegis-failover: manager cannot be restarted after shutdown")
	}
	m.mu.RUnlock()

	runCtx, cancel := context.WithCancel(ctx)
	cur, err := m.readLeader(runCtx)
	if err != nil {
		cancel()
		return err
	}

	m.mu.Lock()
	m.runCtx = runCtx
	m.runCancel = cancel
	m.running = true
	m.active = cur
	m.wg.Add(3)
	m.mu.Unlock()

	go m.monitorLoop()
	go m.watchElectionLoop()
	go m.dispatchLoop()

	return nil
}

// Stop stops failover monitoring and closes change notifications.
func (m *FailoverManager) Stop() error {
	if m == nil {
		return nil
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	cancel := m.runCancel
	m.running = false
	m.runCtx = nil
	m.runCancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.wg.Wait()

	m.mu.Lock()
	if !m.changesClosed {
		close(m.changes)
		m.changesClosed = true
	}
	m.terminated = true
	m.mu.Unlock()

	return nil
}

// IsRunning reports whether failover lifecycle is active.
func (m *FailoverManager) IsRunning() bool {
	if m == nil {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// ActiveLeader returns the latest observed leader record.
func (m *FailoverManager) ActiveLeader() LeaderRecord {
	if m == nil {
		return LeaderRecord{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active
}

// FailoverChanges exposes failover transition notifications.
func (m *FailoverManager) FailoverChanges() <-chan FailoverEvent {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.changes
}

// RegisterHandler registers a failover handler and returns its ID.
func (m *FailoverManager) RegisterHandler(handler FailoverHandler) (uint64, error) {
	if m == nil {
		return 0, errors.New("aegis-failover: manager is nil")
	}

	if handler == nil {
		return 0, errors.New("aegis-failover: handler cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextHandlerID++
	id := m.nextHandlerID
	m.handlers[id] = handler
	return id, nil
}

// UnregisterHandler removes a failover handler.
func (m *FailoverManager) UnregisterHandler(handlerID uint64) {
	if m == nil || handlerID == 0 {
		return
	}

	m.mu.Lock()
	delete(m.handlers, handlerID)
	m.mu.Unlock()
}

func (m *FailoverManager) monitorLoop() {
	defer m.wg.Done()

	_ = m.detectLeaderFailure()

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.detectLeaderFailure(); err != nil && !errors.Is(err, context.Canceled) {
				m.logger.Debug("failover detection cycle failed", "error", err)
			}
		case <-m.done():
			return
		}
	}
}

func (m *FailoverManager) watchElectionLoop() {
	defer m.wg.Done()

	changeCh := m.elector.LeadershipChanges()
	if changeCh == nil {
		<-m.done()
		return
	}

	for {
		select {
		case evt, ok := <-changeCh:
			if !ok {
				return
			}
			m.recordLeaderTransition(m.elector.CurrentLeader(), "election-"+evt.Reason)
		case <-m.done():
			return
		}
	}
}

func (m *FailoverManager) dispatchLoop() {
	defer m.wg.Done()

	for {
		select {
		case evt := <-m.events:
			m.emitChange(evt)
			m.invokeHandlers(evt)
		case <-m.done():
			return
		}
	}
}

func (m *FailoverManager) detectLeaderFailure() error {
	ctx, cancel := context.WithTimeout(m.contextOrBackground(), m.cfg.OperationTimeout)
	defer cancel()

	rec, err := m.readLeader(ctx)
	if err != nil {
		return err
	}

	if rec.LeaderID == "" {
		m.recordLeaderTransition(rec, "no-leader")
		if reconcileErr := m.elector.reconcile(); reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			return reconcileErr
		}
		return nil
	}

	leaseExpired := rec.LeaseExpiresAt.IsZero() || !m.now().Before(rec.LeaseExpiresAt)
	healthFailure := m.isLeaderHealthFailure(rec.LeaderID)
	if !leaseExpired && !healthFailure {
		m.recordLeaderTransition(rec, "leader-healthy")
		return nil
	}

	if !m.beginFailoverAttempt() {
		return nil
	}
	defer m.endFailoverAttempt()

	if rec.LeaderID != "" {
		m.elector.excludeCandidate(rec.LeaderID, m.elector.cfg.LeaderLease)
	}

	if rec.LeaderID == m.elector.cfg.LocalNodeID {
		if _, err := m.elector.Resign(ctx); err != nil {
			return err
		}
	} else {
		clear := LeaderRecord{LeaderID: "", Term: rec.Term + 1, UpdatedAt: m.now()}
		ok, casErr := m.store.CompareAndSwapLeader(ctx, rec.Term, rec.LeaderID, clear)
		if casErr != nil {
			return fmt.Errorf("aegis-failover: failed to clear stale leader: %w", casErr)
		}
		if ok {
			m.recordLeaderTransition(clear, "leader-failed")
		}
	}

	if err := m.elector.reconcile(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func (m *FailoverManager) readLeader(ctx context.Context) (LeaderRecord, error) {
	rec, err := m.store.GetLeader(ctx)
	if err != nil {
		return LeaderRecord{}, fmt.Errorf("aegis-failover: failed to read leader: %w", err)
	}
	return rec, nil
}

func (m *FailoverManager) isLeaderHealthFailure(nodeID string) bool {
	if nodeID == "" {
		return true
	}

	if m.discovery != nil {
		node, ok := m.discovery.GetNode(nodeID)
		if ok {
			if node.Health == NodeHealthUnhealthy {
				return true
			}
			if node.Health == NodeHealthSuspect && m.cfg.TreatSuspectAsFailure {
				return true
			}
			return false
		}
	}

	if m.heartbeat != nil {
		health, ok := m.heartbeat.GetNodeHealth(nodeID)
		if ok {
			if health.State == NodeHealthUnhealthy {
				return true
			}
			if health.State == NodeHealthSuspect && m.cfg.TreatSuspectAsFailure {
				return true
			}
			return false
		}
	}

	// Missing health signals are treated as unknown and do not force failover.
	return false
}

func (m *FailoverManager) beginFailoverAttempt() bool {
	m.failoverMu.Lock()
	if m.failoverInProgress {
		m.failoverMu.Unlock()
		return false
	}
	m.failoverInProgress = true
	m.failoverMu.Unlock()

	m.mu.RLock()
	running := m.running
	ctx := m.runCtx
	m.mu.RUnlock()

	if !running {
		m.endFailoverAttempt()
		return false
	}

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			m.endFailoverAttempt()
			return false
		}
	}

	return true
}

func (m *FailoverManager) endFailoverAttempt() {
	m.failoverMu.Lock()
	m.failoverInProgress = false
	m.failoverMu.Unlock()
}

func (m *FailoverManager) recordLeaderTransition(rec LeaderRecord, reason string) {
	m.mu.Lock()
	prev := m.active
	if prev.LeaderID == rec.LeaderID && prev.Term == rec.Term {
		m.mu.Unlock()
		return
	}
	m.active = rec
	evt := FailoverEvent{
		PreviousLeaderID: prev.LeaderID,
		LeaderID:         rec.LeaderID,
		Term:             rec.Term,
		FencingToken:     rec.Term,
		Reason:           reason,
		ChangedAt:        m.now(),
	}
	m.mu.Unlock()

	select {
	case m.events <- evt:
	case <-m.done():
	}
}

func (m *FailoverManager) emitChange(evt FailoverEvent) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.changesClosed {
		return
	}

	select {
	case m.changes <- evt:
	default:
		m.logger.Warn("failover change notification channel is full", "leader_id", evt.LeaderID, "term", evt.Term)
	}
}

func (m *FailoverManager) invokeHandlers(evt FailoverEvent) {
	m.mu.RLock()
	handlers := make([]FailoverHandler, 0, len(m.handlers))
	for _, h := range m.handlers {
		handlers = append(handlers, h)
	}
	runCtx := m.runCtx
	m.mu.RUnlock()

	for _, h := range handlers {
		ctx := context.Background()
		if runCtx != nil {
			ctx = runCtx
		}
		if m.cfg.HandlerTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, m.cfg.HandlerTimeout)
			err := h(ctx, evt)
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				m.logger.Warn("failover handler returned error", "error", err, "leader_id", evt.LeaderID, "term", evt.Term)
			}
			continue
		}

		if err := h(ctx, evt); err != nil {
			m.logger.Warn("failover handler returned error", "error", err, "leader_id", evt.LeaderID, "term", evt.Term)
		}
	}
}

func (m *FailoverManager) done() <-chan struct{} {
	m.mu.RLock()
	ctx := m.runCtx
	m.mu.RUnlock()

	if ctx == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return ctx.Done()
}

func (m *FailoverManager) contextOrBackground() context.Context {
	m.mu.RLock()
	ctx := m.runCtx
	m.mu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
