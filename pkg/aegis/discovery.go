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

// DiscoveryConfig controls discovery polling, health sync, and expiration behavior.
type DiscoveryConfig struct {
	Interval           time.Duration
	Expiration         time.Duration
	CleanupInterval    time.Duration
	HealthSyncInterval time.Duration
	StartHeartbeat     bool
}

// DefaultDiscoveryConfig returns baseline discovery settings.
func DefaultDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{
		Interval:           2 * time.Second,
		Expiration:         10 * time.Second,
		CleanupInterval:    time.Second,
		HealthSyncInterval: 500 * time.Millisecond,
		StartHeartbeat:     true,
	}
}

func (c *DiscoveryConfig) sanitize() {
	if c == nil {
		return
	}

	if c.Interval <= 0 {
		c.Interval = 2 * time.Second
	}

	if c.Expiration <= 0 {
		c.Expiration = 10 * time.Second
	}

	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Second
	}

	if c.HealthSyncInterval <= 0 {
		c.HealthSyncInterval = 500 * time.Millisecond
	}
}

// Validate checks discovery configuration bounds.
func (c DiscoveryConfig) Validate() error {
	if c.Interval <= 0 {
		return errors.New("aegis-discovery: discovery interval must be greater than zero")
	}

	if c.Expiration <= 0 {
		return errors.New("aegis-discovery: node expiration must be greater than zero")
	}

	if c.CleanupInterval <= 0 {
		return errors.New("aegis-discovery: cleanup interval must be greater than zero")
	}

	if c.CleanupInterval > c.Expiration {
		return errors.New("aegis-discovery: cleanup interval cannot exceed expiration")
	}

	if c.HealthSyncInterval <= 0 {
		return errors.New("aegis-discovery: health sync interval must be greater than zero")
	}

	return nil
}

// DiscoveryAnnouncement is a source-provided node presence observation.
type DiscoveryAnnouncement struct {
	NodeID   string
	Address  string
	SeenAt   time.Time
	Metadata map[string]string
}

// DiscoverySource retrieves discovery announcements from a concrete backend implementation.
// AEGIS does not implement a network discovery protocol by default.
type DiscoverySource interface {
	Discover(ctx context.Context) ([]DiscoveryAnnouncement, error)
}

// DiscoverySourceFunc adapts a function to DiscoverySource.
type DiscoverySourceFunc func(ctx context.Context) ([]DiscoveryAnnouncement, error)

// Discover invokes the wrapped function.
func (f DiscoverySourceFunc) Discover(ctx context.Context) ([]DiscoveryAnnouncement, error) {
	if f == nil {
		return nil, errors.New("aegis-discovery: source function is nil")
	}

	return f(ctx)
}

// DiscoveredNode is a registry snapshot entry.
type DiscoveredNode struct {
	NodeID       string
	Address      string
	DiscoveredAt time.Time
	LastSeenAt   time.Time
	Health       NodeHealthState
	Metadata     map[string]string
}

// DiscoveryManager coordinates source polling, registry updates, and heartbeat sync.
type DiscoveryManager struct {
	cfg       DiscoveryConfig
	source    DiscoverySource
	heartbeat *HeartbeatMonitor
	logger    *slog.Logger
	now       func() time.Time

	mu               sync.RWMutex
	lifecycleMu      sync.Mutex
	running          bool
	runCtx           context.Context
	runCancel        context.CancelFunc
	startedHeartbeat bool
	nodes            map[string]DiscoveredNode
	wg               sync.WaitGroup
}

// NewDiscoveryManager creates a discovery manager.
func NewDiscoveryManager(cfg DiscoveryConfig, source DiscoverySource, heartbeat *HeartbeatMonitor, logger *slog.Logger) (*DiscoveryManager, error) {
	cfg.sanitize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-discovery: invalid configuration: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &DiscoveryManager{
		cfg:       cfg,
		source:    source,
		heartbeat: heartbeat,
		logger:    logger,
		now:       time.Now,
		nodes:     make(map[string]DiscoveredNode),
	}, nil
}

// Start starts discovery loops and optionally starts heartbeat monitoring.
func (m *DiscoveryManager) Start(ctx context.Context) error {
	if m == nil {
		return errors.New("aegis-discovery: manager is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.RLock()
	alreadyRunning := m.running
	m.mu.RUnlock()
	if alreadyRunning {
		return errors.New("aegis-discovery: manager is already running")
	}

	runCtx, runCancel := context.WithCancel(ctx)
	startedHeartbeat := false
	if m.heartbeat != nil && m.cfg.StartHeartbeat && !m.heartbeat.IsRunning() {
		if err := m.heartbeat.Start(runCtx); err != nil {
			runCancel()
			return fmt.Errorf("aegis-discovery: failed to start heartbeat monitor: %w", err)
		}
		startedHeartbeat = true
	}

	m.mu.Lock()
	m.runCtx = runCtx
	m.runCancel = runCancel
	m.running = true
	m.startedHeartbeat = startedHeartbeat
	m.wg.Add(3)
	m.mu.Unlock()

	go m.runDiscoveryLoop()
	go m.runCleanupLoop()
	go m.runHealthSyncLoop()

	return nil
}

// Stop stops all discovery loops and heartbeat monitor if it was started by this manager.
func (m *DiscoveryManager) Stop() error {
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
	startedHeartbeat := m.startedHeartbeat
	m.running = false
	m.runCtx = nil
	m.runCancel = nil
	m.startedHeartbeat = false
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.wg.Wait()

	if startedHeartbeat && m.heartbeat != nil {
		return m.heartbeat.Stop()
	}

	return nil
}

// IsRunning reports whether the manager lifecycle is currently active.
func (m *DiscoveryManager) IsRunning() bool {
	if m == nil {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// RegisterNode registers a node directly in discovery registry.
func (m *DiscoveryManager) RegisterNode(endpoint NodeEndpoint, metadata map[string]string) error {
	if m == nil {
		return errors.New("aegis-discovery: manager is nil")
	}

	if endpoint.NodeID == "" {
		return errors.New("aegis-discovery: node id cannot be empty")
	}

	if endpoint.Address == "" {
		return errors.New("aegis-discovery: node address cannot be empty")
	}

	now := m.now()

	m.mu.Lock()
	if _, exists := m.nodes[endpoint.NodeID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("aegis-discovery: node %q is already registered", endpoint.NodeID)
	}

	m.nodes[endpoint.NodeID] = DiscoveredNode{
		NodeID:       endpoint.NodeID,
		Address:      endpoint.Address,
		DiscoveredAt: now,
		LastSeenAt:   now,
		Health:       NodeHealthSuspect,
		Metadata:     cloneMetadata(metadata),
	}
	m.mu.Unlock()

	if err := m.ensureHeartbeatNode(endpoint); err != nil {
		m.mu.Lock()
		delete(m.nodes, endpoint.NodeID)
		m.mu.Unlock()
		return err
	}

	return nil
}

// UpdateNode updates an existing node and refreshes last-seen timestamp.
func (m *DiscoveryManager) UpdateNode(endpoint NodeEndpoint, metadata map[string]string) error {
	if m == nil {
		return errors.New("aegis-discovery: manager is nil")
	}

	if endpoint.NodeID == "" {
		return errors.New("aegis-discovery: node id cannot be empty")
	}

	if endpoint.Address == "" {
		return errors.New("aegis-discovery: node address cannot be empty")
	}

	m.mu.Lock()
	node, exists := m.nodes[endpoint.NodeID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("aegis-discovery: node %q not found", endpoint.NodeID)
	}
	previous := node

	node.Address = endpoint.Address
	node.LastSeenAt = m.now()
	node.Metadata = cloneMetadata(metadata)
	m.nodes[endpoint.NodeID] = node
	m.mu.Unlock()

	if err := m.ensureHeartbeatNode(endpoint); err != nil {
		m.mu.Lock()
		m.nodes[endpoint.NodeID] = previous
		m.mu.Unlock()
		return err
	}

	return nil
}

// RemoveNode removes a node from discovery and heartbeat monitoring.
func (m *DiscoveryManager) RemoveNode(nodeID string) {
	if m == nil || nodeID == "" {
		return
	}

	m.mu.Lock()
	_, exists := m.nodes[nodeID]
	if exists {
		delete(m.nodes, nodeID)
	}
	m.mu.Unlock()

	if exists && m.heartbeat != nil {
		m.heartbeat.RemoveNode(nodeID)
	}
}

// GetNode returns a discovered node snapshot.
func (m *DiscoveryManager) GetNode(nodeID string) (DiscoveredNode, bool) {
	if m == nil || nodeID == "" {
		return DiscoveredNode{}, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return DiscoveredNode{}, false
	}

	node.Metadata = cloneMetadata(node.Metadata)
	return node, true
}

// ListNodes returns all discovered node snapshots sorted by node ID.
func (m *DiscoveryManager) ListNodes() []DiscoveredNode {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	out := make([]DiscoveredNode, 0, len(m.nodes))
	for _, node := range m.nodes {
		node.Metadata = cloneMetadata(node.Metadata)
		out = append(out, node)
	}
	m.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})

	return out
}

func (m *DiscoveryManager) runDiscoveryLoop() {
	defer m.wg.Done()

	if err := m.discoverOnce(); err != nil && !errors.Is(err, context.Canceled) {
		m.logger.Debug("initial discovery cycle failed", "error", err)
	}

	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.discoverOnce(); err != nil && !errors.Is(err, context.Canceled) {
				m.logger.Debug("discovery cycle failed", "error", err)
			}
		case <-m.done():
			return
		}
	}
}

func (m *DiscoveryManager) runCleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupExpiredNodes()
		case <-m.done():
			return
		}
	}
}

func (m *DiscoveryManager) runHealthSyncLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HealthSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.syncHealth()
		case <-m.done():
			return
		}
	}
}

func (m *DiscoveryManager) done() <-chan struct{} {
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

func (m *DiscoveryManager) discoverOnce() error {
	if m.source == nil {
		return nil
	}

	ctx := m.contextOrBackground()
	announcements, err := m.source.Discover(ctx)
	if err != nil {
		return err
	}

	for _, ann := range announcements {
		if ann.NodeID == "" || ann.Address == "" {
			m.logger.Warn("ignoring malformed discovery announcement", "node_id", ann.NodeID, "address", ann.Address)
			continue
		}

		m.upsertDiscoveredNode(ann)
	}

	return nil
}

func (m *DiscoveryManager) upsertDiscoveredNode(ann DiscoveryAnnouncement) {
	seenAt := ann.SeenAt
	if seenAt.IsZero() {
		seenAt = m.now()
	}

	created := false

	m.mu.Lock()
	node, exists := m.nodes[ann.NodeID]
	if !exists {
		created = true
		node = DiscoveredNode{
			NodeID:       ann.NodeID,
			Address:      ann.Address,
			DiscoveredAt: seenAt,
			Health:       NodeHealthSuspect,
		}
	}

	node.Address = ann.Address
	node.LastSeenAt = seenAt
	node.Metadata = cloneMetadata(ann.Metadata)
	m.nodes[ann.NodeID] = node
	m.mu.Unlock()

	if created {
		if err := m.ensureHeartbeatNode(NodeEndpoint{NodeID: ann.NodeID, Address: ann.Address}); err != nil {
			m.logger.Warn("failed to add discovered node to heartbeat monitor", "node_id", ann.NodeID, "error", err)
		}
	}
}

func (m *DiscoveryManager) cleanupExpiredNodes() {
	expiration := m.cfg.Expiration
	now := m.now()

	expired := make([]string, 0)

	m.mu.Lock()
	for nodeID, node := range m.nodes {
		if now.Sub(node.LastSeenAt) > expiration {
			expired = append(expired, nodeID)
			delete(m.nodes, nodeID)
		}
	}
	m.mu.Unlock()

	if m.heartbeat != nil {
		for _, nodeID := range expired {
			m.heartbeat.RemoveNode(nodeID)
		}
	}
}

func (m *DiscoveryManager) syncHealth() {
	if m.heartbeat == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeID, node := range m.nodes {
		health, ok := m.heartbeat.GetNodeHealth(nodeID)
		if !ok {
			continue
		}

		node.Health = health.State
		m.nodes[nodeID] = node
	}
}

func (m *DiscoveryManager) ensureHeartbeatNode(endpoint NodeEndpoint) error {
	if m == nil || m.heartbeat == nil {
		return nil
	}

	if m.heartbeat.HasNode(endpoint.NodeID) {
		health, ok := m.heartbeat.GetNodeHealth(endpoint.NodeID)
		if ok && health.Address != "" && endpoint.Address != "" && health.Address != endpoint.Address {
			return fmt.Errorf("aegis-discovery: heartbeat already tracks node %q with conflicting address %q", endpoint.NodeID, health.Address)
		}
		return nil
	}

	if err := m.heartbeat.AddNode(endpoint); err != nil {
		return fmt.Errorf("aegis-discovery: failed to add node to heartbeat monitor: %w", err)
	}

	return nil
}

func (m *DiscoveryManager) contextOrBackground() context.Context {
	m.mu.RLock()
	ctx := m.runCtx
	m.mu.RUnlock()

	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}

	return out
}
