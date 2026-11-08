package aegis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rudpHeaderSize      = 16
	rudpVersion         = 1
	rudpTypeData        = 1
	rudpTypeAck         = 2
	rudpMaxFlagsMask    = 0x00
	rudpDuplicateWindow = 1024
	maxUDPDatagramSize  = 65507
	maxUDPPayloadSize   = maxUDPDatagramSize - HeaderSize
	maxRUDPPayloadSize  = maxUDPDatagramSize - rudpHeaderSize - HeaderSize
)

var rudpMagic = [2]byte{'A', 'R'}

type rudpFrame struct {
	version uint8
	typ     uint8
	flags   uint8
	seq     uint32
	ack     uint32
	payload []byte
}

func encodeRUDPFrame(frame rudpFrame) ([]byte, error) {
	payloadLen := len(frame.payload)

	if payloadLen > int(maxRUDPPayloadSize) {
		return nil, fmt.Errorf(
			"%w: RUDP payload size %d exceeds limit %d",
			ErrPacketTooLarge,
			payloadLen,
			maxRUDPPayloadSize,
		)
	}

	if frame.typ == rudpTypeAck && payloadLen != 0 {
		return nil, fmt.Errorf(
			"%w: ACK frames must not include payload",
			ErrRUDPInvalidPacket,
		)
	}

	buf := make(
		[]byte,
		rudpHeaderSize+payloadLen,
	)

	copy(
		buf[:2],
		rudpMagic[:],
	)

	buf[2] = rudpVersion
	buf[3] = frameTypeByte(frame.typ)
	buf[4] = frame.flags
	buf[5] = 0

	binary.BigEndian.PutUint32(
		buf[6:10],
		frame.seq,
	)

	binary.BigEndian.PutUint32(
		buf[10:14],
		frame.ack,
	)

	binary.BigEndian.PutUint16(
		buf[14:16],
		uint16(payloadLen),
	)

	copy(
		buf[rudpHeaderSize:],
		frame.payload,
	)

	return buf, nil
}

func decodeRUDPFrame(
	data []byte,
) (rudpFrame, error) {
	if len(data) < rudpHeaderSize {
		return rudpFrame{}, fmt.Errorf(
			"%w: header is too short",
			ErrRUDPInvalidPacket,
		)
	}

	if data[0] != rudpMagic[0] ||
		data[1] != rudpMagic[1] {
		return rudpFrame{}, fmt.Errorf(
			"%w: invalid magic bytes",
			ErrRUDPInvalidPacket,
		)
	}

	if data[2] != rudpVersion {
		return rudpFrame{}, fmt.Errorf(
			"%w: unsupported version %d",
			ErrRUDPInvalidPacket,
			data[2],
		)
	}

	typ := data[3]
	if typ != rudpTypeData && typ != rudpTypeAck {
		return rudpFrame{}, fmt.Errorf(
			"%w: unsupported frame type %d",
			ErrRUDPInvalidPacket,
			typ,
		)
	}

	flags := data[4]
	if flags&rudpMaxFlagsMask != flags {
		return rudpFrame{}, fmt.Errorf(
			"%w: unsupported flags 0x%02x",
			ErrRUDPInvalidPacket,
			flags,
		)
	}

	payloadLen := int(binary.BigEndian.Uint16(data[14:16]))
	expectedLen := rudpHeaderSize + payloadLen
	if expectedLen != len(data) {
		return rudpFrame{}, fmt.Errorf(
			"%w: payload length mismatch expected %d got %d",
			ErrRUDPInvalidPacket,
			expectedLen,
			len(data),
		)
	}

	if payloadLen > int(maxRUDPPayloadSize) {
		return rudpFrame{}, fmt.Errorf(
			"%w: payload size %d exceeds limit %d",
			ErrPacketTooLarge,
			payloadLen,
			maxRUDPPayloadSize,
		)
	}

	ack := binary.BigEndian.Uint32(data[10:14])
	if typ == rudpTypeData && ack != 0 {
		return rudpFrame{}, fmt.Errorf(
			"%w: DATA frame must not carry ACK number",
			ErrRUDPInvalidPacket,
		)
	}

	if typ == rudpTypeAck {
		if payloadLen != 0 {
			return rudpFrame{}, fmt.Errorf(
				"%w: ACK frame payload must be zero",
				ErrRUDPInvalidPacket,
			)
		}
	}

	frame := rudpFrame{
		version: data[2],
		typ:     typ,
		flags:   flags,
		seq: binary.BigEndian.Uint32(
			data[6:10],
		),
		ack: ack,
	}

	if payloadLen > 0 {
		frame.payload = make([]byte, payloadLen)
		copy(frame.payload, data[rudpHeaderSize:])
	}

	return frame, nil
}

func frameTypeByte(t uint8) byte {
	return byte(t)
}

func encodeRUDPData(seq uint32, payload []byte) ([]byte, error) {
	return encodeRUDPFrame(rudpFrame{
		version: rudpVersion,
		typ:     rudpTypeData,
		seq:     seq,
		ack:     0,
		payload: payload,
	})
}

func encodeRUDPAck(seq uint32) ([]byte, error) {
	return encodeRUDPFrame(rudpFrame{
		version: rudpVersion,
		typ:     rudpTypeAck,
		seq:     0,
		ack:     seq,
		payload: nil,
	})
}

func seqLess(a, b uint32) bool {
	return int32(a-b) < 0
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next <= 0 || next > max {
		return max
	}

	return next
}

type rudpReceiveWindow struct {
	seen  map[uint32]struct{}
	order []uint32
	max   int
}

func newRUDPReceiveWindow(max int) rudpReceiveWindow {
	if max <= 0 {
		max = rudpDuplicateWindow
	}

	return rudpReceiveWindow{
		seen: make(map[uint32]struct{}, max),
		max:  max,
	}
}

func (w *rudpReceiveWindow) add(seq uint32) {
	if w == nil {
		return
	}

	if _, exists := w.seen[seq]; exists {
		return
	}

	w.seen[seq] = struct{}{}
	w.order = append(w.order, seq)

	if len(w.order) <= w.max {
		return
	}

	evicted := w.order[0]
	w.order = w.order[1:]
	delete(w.seen, evicted)
}

func (w *rudpReceiveWindow) contains(seq uint32) bool {
	if w == nil {
		return false
	}

	_, ok := w.seen[seq]
	return ok
}

type rudpConnState struct {
	client *Client
	conn   net.Conn

	mu        sync.Mutex
	writeMu   sync.Mutex
	nextSeq   uint32
	pending   map[uint32]chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error

	recvQueue chan []byte

	recvInit bool
	recvLast uint32
	window   rudpReceiveWindow
}

type rudpIncomingDecision int

const (
	rudpIncomingDeliver rudpIncomingDecision = iota
	rudpIncomingDuplicate
	rudpIncomingFuture
)

func (s *rudpConnState) closeWithError(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closeErr = err
		for seq := range s.pending {
			delete(s.pending, seq)
		}
		s.mu.Unlock()

		close(s.closed)
	})
}

func (s *rudpConnState) ackReceived(ack uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, ok := s.pending[ack]
	if !ok {
		return
	}

	close(ch)
	delete(s.pending, ack)
}

func (s *rudpConnState) reservePending() (uint32, chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeErr != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrRUDPSessionClosed, s.closeErr)
	}

	if len(s.pending) >= s.client.config.RUDPMaxInFlight {
		return 0, nil, ErrRUDPInFlightCapacity
	}

	s.nextSeq++
	seq := s.nextSeq
	ch := make(chan struct{})
	s.pending[seq] = ch

	return seq, ch, nil
}

func (s *rudpConnState) writeFrame(ctx context.Context, timeout time.Duration, data []byte) error {
	if s == nil || s.conn == nil {
		return fmt.Errorf("%w: missing session connection", ErrRUDPSessionClosed)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := setWriteDeadline(ctx, s.conn, timeout); err != nil {
		return err
	}

	n, err := s.conn.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

func (s *rudpConnState) clearPending(seq uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, ok := s.pending[seq]
	if !ok {
		return
	}

	close(ch)
	delete(s.pending, seq)
}

func (s *rudpConnState) sessionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeErr == nil {
		return ErrRUDPSessionClosed
	}

	return fmt.Errorf("%w: %v", ErrRUDPSessionClosed, s.closeErr)
}

func (s *rudpConnState) classifyIncomingData(seq uint32) (rudpIncomingDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recvInit {
		if seq != 1 {
			return rudpIncomingFuture, fmt.Errorf("%w: expected initial sequence 1 got %d", ErrRUDPInvalidSequence, seq)
		}

		return rudpIncomingDeliver, nil
	}

	if seq == s.recvLast+1 {
		return rudpIncomingDeliver, nil
	}

	if s.window.contains(seq) || seq == s.recvLast || seqLess(seq, s.recvLast+1) {
		return rudpIncomingDuplicate, nil
	}

	return rudpIncomingFuture, fmt.Errorf("%w: expected sequence %d got %d", ErrRUDPInvalidSequence, s.recvLast+1, seq)
}

func (s *rudpConnState) markDelivered(seq uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recvInit = true
	s.recvLast = seq
	s.window.add(seq)
}

func (c *Client) getOrCreateRUDPSession(conn net.Conn) *rudpConnState {
	if c == nil || conn == nil {
		return nil
	}

	if state, ok := c.rudpSessions.Load(conn); ok {
		return state.(*rudpConnState)
	}

	state := &rudpConnState{
		client:    c,
		conn:      conn,
		pending:   make(map[uint32]chan struct{}),
		closed:    make(chan struct{}),
		recvQueue: make(chan []byte, c.config.RUDPReceiveQueueSize),
		window:    newRUDPReceiveWindow(rudpDuplicateWindow),
	}

	actual, loaded := c.rudpSessions.LoadOrStore(conn, state)
	if loaded {
		return actual.(*rudpConnState)
	}

	go c.runRUDPClientReader(state)

	return state
}

func (c *Client) deleteRUDPSession(conn net.Conn) {
	if c == nil || conn == nil {
		return
	}

	state, ok := c.rudpSessions.Load(conn)
	if ok {
		state.(*rudpConnState).closeWithError(io.EOF)
	}

	c.rudpSessions.Delete(conn)
}

func (c *Client) runRUDPClientReader(state *rudpConnState) {
	if c == nil || state == nil || state.conn == nil {
		return
	}

	buffer := make([]byte, maxUDPDatagramSize)

	for {
		select {
		case <-state.closed:
			return
		default:
		}

		if c.config.ReadTimeout > 0 {
			_ = state.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		}

		n, err := state.conn.Read(buffer)
		if err != nil {
			if isTimeout(err) {
				continue
			}

			if isConnectionClosedError(err) || isClosedNetworkError(err) {
				state.closeWithError(fmt.Errorf("%w: %v", ErrConnectionClosed, err))
				return
			}

			state.closeWithError(err)
			return
		}

		if n == 0 {
			continue
		}

		frame, err := decodeRUDPFrame(buffer[:n])
		if err != nil {
			continue
		}

		switch frame.typ {
		case rudpTypeAck:
			state.ackReceived(frame.ack)

		case rudpTypeData:
			decision, seqErr := state.classifyIncomingData(frame.seq)
			if seqErr != nil {
				continue
			}

			switch decision {
			case rudpIncomingDuplicate:
				ackData, err := encodeRUDPAck(frame.seq)
				if err == nil {
					_ = state.writeFrame(context.Background(), c.config.WriteTimeout, ackData)
				}

				continue

			case rudpIncomingFuture:
				continue
			}

			packet, err := Deserialize(frame.payload, c.config.MaxPayloadSize)
			if err != nil {
				continue
			}

			state.markDelivered(frame.seq)

			ackData, err := encodeRUDPAck(frame.seq)
			if err == nil {
				_ = state.writeFrame(context.Background(), c.config.WriteTimeout, ackData)
			}

			payload := append([]byte(nil), packet.Payload...)

			select {
			case state.recvQueue <- payload:
			case <-state.closed:
				return
			}
		}
	}
}

func (c *Client) sendRUDPWithSession(ctx context.Context, conn net.Conn, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if uint64(len(payload)) > uint64(maxRUDPPayloadSize) {
		return fmt.Errorf("%w: payload size %d exceeds RUDP limit %d", ErrPacketTooLarge, len(payload), maxRUDPPayloadSize)
	}

	state := c.getOrCreateRUDPSession(conn)
	if state == nil {
		return fmt.Errorf("%w: missing session state", ErrRUDPSessionClosed)
	}

	seq, ackCh, err := state.reservePending()
	if err != nil {
		return err
	}

	defer state.clearPending(seq)

	frame, err := encodeRUDPData(seq, payload)
	if err != nil {
		return err
	}

	backoff := c.config.RUDPInitialRetransmitTimeout

	for attempt := 0; attempt < c.config.RUDPMaxRetries; attempt++ {
		err := state.writeFrame(ctx, c.config.WriteTimeout, frame)
		if err != nil {
			if isTimeout(err) {
				continue
			}

			if isConnectionClosedError(err) || isClosedNetworkError(err) {
				state.closeWithError(fmt.Errorf("%w: %v", ErrConnectionClosed, err))
				return state.sessionError()
			}

			return fmt.Errorf("aegis-client: failed to send RUDP frame: %w", err)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ackCh:
			if !timer.Stop() {
				<-timer.C
			}

			return nil

		case <-timer.C:
			backoff = nextBackoff(backoff, c.config.RUDPMaxRetransmitTimeout)

		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return ctx.Err()

		case <-state.closed:
			if !timer.Stop() {
				<-timer.C
			}

			return state.sessionError()
		}
	}

	return fmt.Errorf("%w: sequence %d", ErrRUDPRetryExhausted, seq)
}

func (c *Client) receiveRUDP(ctx context.Context, conn net.Conn) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	state := c.getOrCreateRUDPSession(conn)
	if state == nil {
		return nil, fmt.Errorf("%w: missing session state", ErrRUDPSessionClosed)
	}

	select {
	case payload := <-state.recvQueue:
		return payload, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-state.closed:
		return nil, state.sessionError()
	}
}

type rudpServerPeerState struct {
	addr *net.UDPAddr

	mu      sync.Mutex
	nextSeq uint32
	pending map[uint32]chan struct{}

	recvInit bool
	recvLast uint32
	window   rudpReceiveWindow

	lastSeen time.Time
}

func newRUDPServerPeerState(addr *net.UDPAddr) *rudpServerPeerState {
	return &rudpServerPeerState{
		addr:     addr,
		pending:  make(map[uint32]chan struct{}),
		window:   newRUDPReceiveWindow(rudpDuplicateWindow),
		lastSeen: time.Now(),
	}
}

func (p *rudpServerPeerState) touch() {
	p.mu.Lock()
	p.lastSeen = time.Now()
	p.mu.Unlock()
}

func (p *rudpServerPeerState) snapshot() (time.Time, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lastSeen, len(p.pending)
}

func (p *rudpServerPeerState) reservePending(maxInFlight int) (uint32, chan struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pending) >= maxInFlight {
		return 0, nil, ErrRUDPInFlightCapacity
	}

	p.nextSeq++
	seq := p.nextSeq
	ch := make(chan struct{})
	p.pending[seq] = ch

	return seq, ch, nil
}

func (p *rudpServerPeerState) clearPending(seq uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, ok := p.pending[seq]
	if !ok {
		return
	}

	close(ch)
	delete(p.pending, seq)
}

func (p *rudpServerPeerState) ackReceived(ack uint32) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, ok := p.pending[ack]
	if !ok {
		return false
	}

	close(ch)
	delete(p.pending, ack)
	p.lastSeen = time.Now()
	return true
}

func (p *rudpServerPeerState) classifyIncomingData(seq uint32) (rudpIncomingDecision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastSeen = time.Now()

	if !p.recvInit {
		if seq != 1 {
			return rudpIncomingFuture, fmt.Errorf("%w: expected initial sequence 1 got %d", ErrRUDPInvalidSequence, seq)
		}

		return rudpIncomingDeliver, nil
	}

	if seq == p.recvLast+1 {
		return rudpIncomingDeliver, nil
	}

	if p.window.contains(seq) || seq == p.recvLast || seqLess(seq, p.recvLast+1) {
		return rudpIncomingDuplicate, nil
	}

	return rudpIncomingFuture, fmt.Errorf("%w: expected sequence %d got %d", ErrRUDPInvalidSequence, p.recvLast+1, seq)
}

func (p *rudpServerPeerState) markDelivered(seq uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.recvInit = true
	p.recvLast = seq
	p.window.add(seq)
	p.lastSeen = time.Now()
}

// RUDPTransport provides reliable ordered datagram transport over UDP.
type RUDPTransport struct {
	server *Server
	conn   *net.UDPConn

	mu    sync.RWMutex
	peers map[string]*rudpServerPeerState

	writeMu sync.Mutex

	responseWG     sync.WaitGroup
	workerMu       sync.Mutex
	workersClosing bool
	shuttingDown   atomic.Bool
}

// NewRUDPTransport creates a RUDP transport for the server.
func NewRUDPTransport(s *Server) *RUDPTransport {
	return &RUDPTransport{
		server: s,
		peers:  make(map[string]*rudpServerPeerState),
	}
}

// Start binds the RUDP socket and processes incoming frames.
func (r *RUDPTransport) Start(ctx context.Context) error {
	if r == nil || r.server == nil {
		return fmt.Errorf(
			"aegis-rudp: server is nil",
		)
	}

	if ctx.Err() != nil {
		return ErrServerClosed
	}

	addr, err := net.ResolveUDPAddr(
		"udp",
		r.server.config.Address,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-rudp: failed to resolve address %s: %w",
			r.server.config.Address,
			err,
		)
	}

	conn, err := net.ListenUDP(
		"udp",
		addr,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-rudp: failed to bind socket: %w",
			err,
		)
	}

	r.conn = conn

	defer func() {
		_ = conn.Close()
	}()

	r.server.logger.Info(
		"Aegis RUDP Engine successfully running",
		"address",
		r.server.config.Address,
	)

	buffer := make([]byte, maxUDPDatagramSize)
	peerGCInterval := time.Second
	if r.server.config.ReadTimeout > 0 && r.server.config.ReadTimeout < peerGCInterval {
		peerGCInterval = r.server.config.ReadTimeout
	}
	peerGCTicker := time.NewTicker(peerGCInterval)
	defer peerGCTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-r.server.quit:
			return nil

		case <-peerGCTicker.C:
			r.cleanupExpiredPeers()

		default:
		}

		if r.server.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(
				time.Now().Add(
					r.server.config.ReadTimeout,
				),
			)
		}

		n, remoteAddr, err := conn.ReadFromUDP(buffer)

		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}

			if isClosedUDPReadError(err) {
				return nil
			}

			if isTimeout(err) {
				continue
			}

			if isTemporaryNetworkError(err) {
				r.server.logger.Warn(
					"Temporary RUDP read error",
					"error",
					err,
				)

				continue
			}

			return fmt.Errorf(
				"aegis-rudp: failed to read datagram: %w",
				err,
			)
		}

		frame, err := decodeRUDPFrame(buffer[:n])

		if err != nil {
			r.server.logger.Warn(
				"Failed to decode RUDP frame",
				"remote_addr",
				remoteAddr.String(),
				"error",
				err,
			)

			continue
		}

		if frame.typ == rudpTypeAck {
			peer := r.getPeer(remoteAddr)
			if peer == nil {
				r.server.logger.Debug(
					"Ignored ACK from unknown RUDP peer",
					"remote_addr",
					remoteAddr.String(),
					"ack",
					frame.ack,
				)

				continue
			}

			if !peer.ackReceived(frame.ack) {
				r.server.logger.Debug(
					"Ignored unmatched RUDP ACK",
					"remote_addr",
					remoteAddr.String(),
					"ack",
					frame.ack,
				)
			}

			continue
		}

		peer := r.getOrCreatePeer(remoteAddr)
		if peer == nil {
			r.server.logger.Warn(
				"RUDP peer limit reached",
				"remote_addr",
				remoteAddr.String(),
			)

			continue
		}

		if frame.typ != rudpTypeData {
			continue
		}

		decision, seqErr := peer.classifyIncomingData(frame.seq)
		if seqErr != nil {
			r.server.logger.Debug(
				"Rejected out-of-order RUDP sequence",
				"remote_addr",
				remoteAddr.String(),
				"sequence",
				frame.seq,
				"error",
				seqErr,
			)

			continue
		}

		switch decision {
		case rudpIncomingDuplicate:
			ackData, err := encodeRUDPAck(frame.seq)
			if err == nil {
				_ = r.writeToPeer(remoteAddr, ackData)
			}
			continue

		case rudpIncomingFuture:
			continue
		}

		packet, err := Deserialize(frame.payload, r.server.config.MaxPayloadSize)

		if err != nil {
			r.server.logger.Warn(
				"Failed to deserialize RUDP packet",
				"remote_addr",
				remoteAddr.String(),
				"error",
				err,
			)

			continue
		}

		peer.markDelivered(frame.seq)

		ackData, err := encodeRUDPAck(frame.seq)
		if err == nil {
			_ = r.writeToPeer(remoteAddr, ackData)
		}

		if r.server.handler != nil {
			if rudpHandler, ok := r.server.handler.(RUDPPacketHandler); ok {
				responsePayload, handlerErr := rudpHandler.HandleRUDPPacket(packet.Payload, remoteAddr)
				if handlerErr != nil {
					r.server.logger.Error(
						"RUDP packet handler failed",
						"remote_addr",
						remoteAddr.String(),
						"error",
						handlerErr,
					)
				} else if responsePayload != nil {
					r.startResponseWorker(remoteAddr, append([]byte(nil), responsePayload...), peer)
				}
			} else {
				if err := r.server.handler.HandlePacket(packet.Payload); err != nil {
					r.server.logger.Error(
						"RUDP packet handler failed",
						"remote_addr",
						remoteAddr.String(),
						"error",
						err,
					)
				}
			}
		}

		r.server.logger.Debug(
			"Valid Aegis RUDP packet processed",
			"remote_addr",
			remoteAddr.String(),
			"sequence",
			frame.seq,
			"payload_len",
			packet.Length,
			"checksum",
			packet.Checksum,
		)
	}
}

func (r *RUDPTransport) startResponseWorker(addr *net.UDPAddr, payload []byte, peerState *rudpServerPeerState) bool {
	if r == nil || addr == nil || peerState == nil {
		return false
	}

	r.workerMu.Lock()
	if r.workersClosing || r.shuttingDown.Load() {
		r.workerMu.Unlock()
		return false
	}

	r.responseWG.Add(1)
	r.workerMu.Unlock()

	go func() {
		defer r.responseWG.Done()

		if err := r.sendReliableResponse(r.server.ctx, peerState, addr, payload); err != nil {
			r.server.logger.Warn(
				"Failed to send reliable RUDP response",
				"remote_addr",
				addr.String(),
				"error",
				err,
			)
		}
	}()

	return true
}

func (r *RUDPTransport) getPeer(remoteAddr *net.UDPAddr) *rudpServerPeerState {
	if r == nil || remoteAddr == nil {
		return nil
	}

	key := remoteAddr.String()

	r.mu.RLock()
	peer := r.peers[key]
	r.mu.RUnlock()

	if peer != nil {
		peer.touch()
	}

	return peer
}

func (r *RUDPTransport) getOrCreatePeer(remoteAddr *net.UDPAddr) *rudpServerPeerState {
	if r == nil || remoteAddr == nil {
		return nil
	}

	key := remoteAddr.String()

	r.mu.RLock()
	peer, ok := r.peers[key]
	r.mu.RUnlock()
	if ok {
		peer.touch()
		return peer
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, exists := r.peers[key]; exists {
		existing.touch()
		return existing
	}

	if len(r.peers) >= r.server.config.MaxConnections {
		return nil
	}

	peer = newRUDPServerPeerState(remoteAddr)
	r.peers[key] = peer
	return peer
}

func (r *RUDPTransport) cleanupExpiredPeers() {
	if r == nil || r.server == nil {
		return
	}

	idleTTL := 2 * r.server.config.ReadTimeout
	if idleTTL <= 0 {
		idleTTL = 10 * time.Second
	}

	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for key, peer := range r.peers {
		lastSeen, pending := peer.snapshot()
		if pending > 0 {
			continue
		}

		if now.Sub(lastSeen) > idleTTL {
			delete(r.peers, key)
		}
	}
}

func (r *RUDPTransport) writeToPeer(remoteAddr *net.UDPAddr, data []byte) error {
	if r == nil || r.conn == nil || remoteAddr == nil {
		return fmt.Errorf("%w: missing RUDP transport state", ErrRUDPSessionClosed)
	}

	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	if r.server != nil && r.server.config.WriteTimeout > 0 {
		if err := r.conn.SetWriteDeadline(time.Now().Add(r.server.config.WriteTimeout)); err != nil {
			return err
		}
	}

	n, err := r.conn.WriteToUDP(data, remoteAddr)
	if err != nil {
		return err
	}

	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

func (r *RUDPTransport) sendReliableResponse(
	ctx context.Context,
	peer *rudpServerPeerState,
	remoteAddr *net.UDPAddr,
	payload []byte,
) error {
	if r == nil || r.conn == nil || peer == nil || remoteAddr == nil {
		return fmt.Errorf("%w: missing RUDP response state", ErrRUDPSessionClosed)
	}

	if uint64(len(payload)) > uint64(r.server.config.MaxPayloadSize) {
		return fmt.Errorf("%w: payload size %d exceeds configured limit %d", ErrPacketTooLarge, len(payload), r.server.config.MaxPayloadSize)
	}

	packet := NewPacket(payload)
	packetBytes, err := packet.Serialize()
	if err != nil {
		return fmt.Errorf("aegis-rudp: failed to serialize response packet: %w", err)
	}

	seq, ackCh, err := peer.reservePending(r.server.config.RUDPMaxInFlight)
	if err != nil {
		return err
	}

	defer peer.clearPending(seq)

	frame, err := encodeRUDPData(seq, packetBytes)
	if err != nil {
		return err
	}

	backoff := r.server.config.RUDPInitialRetransmitTimeout

	for attempt := 0; attempt < r.server.config.RUDPMaxRetries; attempt++ {
		err := r.writeToPeer(remoteAddr, frame)
		if err != nil {
			if isTimeout(err) {
				continue
			}

			if errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("%w: %v", ErrRUDPSessionClosed, err)
			}

			return fmt.Errorf("aegis-rudp: failed to send response frame: %w", err)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ackCh:
			if !timer.Stop() {
				<-timer.C
			}

			return nil

		case <-timer.C:
			backoff = nextBackoff(backoff, r.server.config.RUDPMaxRetransmitTimeout)

		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return ctx.Err()

		case <-r.server.quit:
			if !timer.Stop() {
				<-timer.C
			}

			return ErrServerClosed
		}
	}

	return fmt.Errorf("%w: sequence %d", ErrRUDPRetryExhausted, seq)
}

// Close closes the RUDP socket.
func (r *RUDPTransport) Close() error {
	if r == nil {
		return nil
	}

	r.shuttingDown.Store(true)

	r.workerMu.Lock()
	r.workersClosing = true
	r.workerMu.Unlock()

	var closeErr error
	if r.conn != nil {
		closeErr = r.conn.Close()
	}

	r.responseWG.Wait()

	if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
		return closeErr
	}

	return nil
}

func isClosedNetworkError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
