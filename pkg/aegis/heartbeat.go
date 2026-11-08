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

// NodeHealthState represents the current heartbeat health status of a node.
type NodeHealthState string

const (
	NodeHealthHealthy   NodeHealthState = "healthy"
	NodeHealthSuspect   NodeHealthState = "suspect"
	NodeHealthUnhealthy NodeHealthState = "unhealthy"
)

// HeartbeatConfig defines timing and transition behavior for node monitoring.
type HeartbeatConfig struct {
	LocalNodeID        string
	Interval           time.Duration
	Timeout            time.Duration
	SuspectThreshold   int
	UnhealthyThreshold int
}

// DefaultHeartbeatConfig returns baseline heartbeat settings.
func DefaultHeartbeatConfig(localNodeID string) HeartbeatConfig {
	return HeartbeatConfig{
		LocalNodeID:        localNodeID,
		Interval:           2 * time.Second,
		Timeout:            1 * time.Second,
		SuspectThreshold:   1,
		UnhealthyThreshold: 3,
	}
}

func (c *HeartbeatConfig) sanitize() {
	if c == nil {
		return
	}

	if c.SuspectThreshold <= 0 {
		c.SuspectThreshold = 1
	}

	if c.UnhealthyThreshold <= 0 {
		c.UnhealthyThreshold = 3
	}
}

// Validate checks heartbeat configuration invariants.
func (c HeartbeatConfig) Validate() error {
	if c.LocalNodeID == "" {
		return errors.New("aegis-heartbeat: local node id cannot be empty")
	}

	if c.Interval <= 0 {
		return errors.New("aegis-heartbeat: heartbeat interval must be greater than zero")
	}

	if c.Timeout <= 0 {
		return errors.New("aegis-heartbeat: heartbeat timeout must be greater than zero")
	}

	if c.Timeout > c.Interval {
		return errors.New("aegis-heartbeat: heartbeat timeout cannot exceed heartbeat interval")
	}

	if c.SuspectThreshold < 1 {
		return errors.New("aegis-heartbeat: suspect threshold must be at least 1")
	}

	if c.UnhealthyThreshold < c.SuspectThreshold {
		return errors.New("aegis-heartbeat: unhealthy threshold must be greater than or equal to suspect threshold")
	}

	return nil
}

// NodeEndpoint identifies a monitored node.
type NodeEndpoint struct {
	NodeID  string
	Address string
}

// HeartbeatMessage is sent by the local monitor to a target node.
type HeartbeatMessage struct {
	SourceNodeID string
	TargetNodeID string
	SentAt       time.Time
}

// HeartbeatResponse is returned by the probe implementation when a heartbeat succeeds.
type HeartbeatResponse struct {
	SourceNodeID string
	TargetNodeID string
	ReceivedAt   time.Time
}

// HeartbeatProbe sends a heartbeat and waits for a response from a target node.
type HeartbeatProbe interface {
	ExchangeHeartbeat(ctx context.Context, target NodeEndpoint, msg HeartbeatMessage) (HeartbeatResponse, error)
}

// HeartbeatProbeFunc adapts a function to HeartbeatProbe.
type HeartbeatProbeFunc func(ctx context.Context, target NodeEndpoint, msg HeartbeatMessage) (HeartbeatResponse, error)

// ExchangeHeartbeat invokes the wrapped function.
func (f HeartbeatProbeFunc) ExchangeHeartbeat(ctx context.Context, target NodeEndpoint, msg HeartbeatMessage) (HeartbeatResponse, error) {
	if f == nil {
		return HeartbeatResponse{}, errors.New("aegis-heartbeat: probe function is nil")
	}

	return f(ctx, target, msg)
}

// NodeHealthInfo is an immutable health snapshot for a monitored node.
type NodeHealthInfo struct {
	NodeID             string
	Address            string
	State              NodeHealthState
	LastSuccessfulBeat time.Time
	LastCheckedAt      time.Time
	ConsecutiveMisses  int
	LastError          string
}

type heartbeatNodeState struct {
	endpoint NodeEndpoint
	health   NodeHealthInfo

	probeMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

// HeartbeatMonitor periodically probes nodes and tracks health transitions.
type HeartbeatMonitor struct {
	cfg    HeartbeatConfig
	probe  HeartbeatProbe
	logger *slog.Logger

	now func() time.Time

	mu      sync.RWMutex
	nodes   map[string]*heartbeatNodeState
	running bool

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup
}

// NewHeartbeatMonitor creates a monitor with validated heartbeat settings.
func NewHeartbeatMonitor(cfg HeartbeatConfig, probe HeartbeatProbe, logger *slog.Logger) (*HeartbeatMonitor, error) {
	cfg.sanitize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-heartbeat: invalid configuration: %w", err)
	}

	if probe == nil {
		return nil, errors.New("aegis-heartbeat: probe cannot be nil")
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &HeartbeatMonitor{
		cfg:    cfg,
		probe:  probe,
		logger: logger,
		nodes:  make(map[string]*heartbeatNodeState),
		now:    time.Now,
	}, nil
}

// Start begins heartbeat workers for all monitored nodes.
func (m *HeartbeatMonitor) Start(ctx context.Context) error {
	if m == nil {
		return errors.New("aegis-heartbeat: monitor is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return errors.New("aegis-heartbeat: monitor is already running")
	}

	m.runCtx, m.runCancel = context.WithCancel(ctx)
	m.running = true

	for nodeID := range m.nodes {
		m.startNodeLoopLocked(nodeID)
	}

	return nil
}

// Stop cancels all active heartbeat workers and waits for shutdown.
func (m *HeartbeatMonitor) Stop() error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	cancel := m.runCancel
	m.running = false
	m.runCtx = nil
	m.runCancel = nil

	for _, node := range m.nodes {
		if node.cancel != nil {
			node.cancel()
			node.cancel = nil
			node.ctx = nil
		}
	}
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.wg.Wait()
	return nil
}

// IsRunning reports whether the monitor lifecycle is currently active.
func (m *HeartbeatMonitor) IsRunning() bool {
	if m == nil {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// AddNode registers a node for heartbeat monitoring.
func (m *HeartbeatMonitor) AddNode(endpoint NodeEndpoint) error {
	if m == nil {
		return errors.New("aegis-heartbeat: monitor is nil")
	}

	if endpoint.NodeID == "" {
		return errors.New("aegis-heartbeat: node id cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[endpoint.NodeID]; exists {
		return fmt.Errorf("aegis-heartbeat: node %q is already registered", endpoint.NodeID)
	}

	now := m.now()
	m.nodes[endpoint.NodeID] = &heartbeatNodeState{
		endpoint: endpoint,
		health: NodeHealthInfo{
			NodeID:            endpoint.NodeID,
			Address:           endpoint.Address,
			State:             NodeHealthSuspect,
			LastCheckedAt:     now,
			ConsecutiveMisses: 0,
		},
	}

	if m.running {
		m.startNodeLoopLocked(endpoint.NodeID)
	}

	return nil
}

// RemoveNode unregisters a node and stops its worker if running.
func (m *HeartbeatMonitor) RemoveNode(nodeID string) {
	if m == nil || nodeID == "" {
		return
	}

	m.mu.Lock()
	node, ok := m.nodes[nodeID]
	if ok {
		delete(m.nodes, nodeID)
		if node.cancel != nil {
			node.cancel()
			node.cancel = nil
			node.ctx = nil
		}
	}
	m.mu.Unlock()
}

// GetNodeHealth returns a snapshot for a node.
func (m *HeartbeatMonitor) GetNodeHealth(nodeID string) (NodeHealthInfo, bool) {
	if m == nil || nodeID == "" {
		return NodeHealthInfo{}, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return NodeHealthInfo{}, false
	}

	return node.health, true
}

// ListNodeHealth returns snapshots for all monitored nodes sorted by node id.
func (m *HeartbeatMonitor) ListNodeHealth() []NodeHealthInfo {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	out := make([]NodeHealthInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		out = append(out, node.health)
	}
	m.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})

	return out
}

// HasNode reports whether the monitor currently tracks the provided node id.
func (m *HeartbeatMonitor) HasNode(nodeID string) bool {
	if m == nil || nodeID == "" {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.nodes[nodeID]
	return ok
}

func (m *HeartbeatMonitor) startNodeLoopLocked(nodeID string) {
	node, ok := m.nodes[nodeID]
	if !ok || node.cancel != nil || m.runCtx == nil {
		return
	}

	nodeCtx, cancel := context.WithCancel(m.runCtx)
	node.ctx = nodeCtx
	node.cancel = cancel

	m.wg.Add(1)
	go m.runNodeHeartbeat(nodeID)
}

func (m *HeartbeatMonitor) runNodeHeartbeat(nodeID string) {
	defer m.wg.Done()

	if err := m.probeNode(nodeID); err != nil {
		if !errors.Is(err, context.Canceled) {
			m.logger.Debug("initial heartbeat probe failed", "node_id", nodeID, "error", err)
		}
	}

	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.probeNode(nodeID); err != nil {
				if !errors.Is(err, context.Canceled) {
					m.logger.Debug("heartbeat probe failed", "node_id", nodeID, "error", err)
				}
			}
		case <-m.nodeDone(nodeID):
			return
		}
	}
}

func (m *HeartbeatMonitor) nodeDone(nodeID string) <-chan struct{} {
	m.mu.RLock()
	node, ok := m.nodes[nodeID]
	if !ok || node.ctx == nil {
		m.mu.RUnlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	done := node.ctx.Done()
	m.mu.RUnlock()
	return done
}

func (m *HeartbeatMonitor) probeNode(nodeID string) error {
	m.mu.RLock()
	node, ok := m.nodes[nodeID]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("aegis-heartbeat: node %q not found", nodeID)
	}

	nodeCtx := node.ctx
	endpoint := node.endpoint
	m.mu.RUnlock()

	node.probeMu.Lock()
	defer node.probeMu.Unlock()

	if nodeCtx == nil {
		nodeCtx = context.Background()
	}

	if err := nodeCtx.Err(); err != nil {
		return err
	}

	probeCtx, cancel := context.WithTimeout(nodeCtx, m.cfg.Timeout)
	defer cancel()

	now := m.now()
	msg := HeartbeatMessage{
		SourceNodeID: m.cfg.LocalNodeID,
		TargetNodeID: endpoint.NodeID,
		SentAt:       now,
	}

	_, err := m.probe.ExchangeHeartbeat(probeCtx, endpoint, msg)
	if err != nil {
		if errors.Is(err, context.Canceled) && nodeCtx.Err() != nil {
			return err
		}

		m.applyProbeFailure(nodeID, err)
		return err
	}

	m.applyProbeSuccess(nodeID)
	return nil
}

func (m *HeartbeatMonitor) applyProbeSuccess(nodeID string) {
	now := m.now()

	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return
	}

	node.health.State = NodeHealthHealthy
	node.health.LastSuccessfulBeat = now
	node.health.LastCheckedAt = now
	node.health.ConsecutiveMisses = 0
	node.health.LastError = ""
}

func (m *HeartbeatMonitor) applyProbeFailure(nodeID string, err error) {
	now := m.now()

	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return
	}

	node.health.LastCheckedAt = now
	node.health.ConsecutiveMisses++
	node.health.LastError = err.Error()

	switch {
	case node.health.ConsecutiveMisses >= m.cfg.UnhealthyThreshold:
		node.health.State = NodeHealthUnhealthy
	case node.health.ConsecutiveMisses >= m.cfg.SuspectThreshold:
		node.health.State = NodeHealthSuspect
	}
}
