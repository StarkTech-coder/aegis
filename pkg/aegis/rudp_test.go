package aegis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rudpTimeoutErr struct{}

func (rudpTimeoutErr) Error() string   { return "timeout" }
func (rudpTimeoutErr) Timeout() bool   { return true }
func (rudpTimeoutErr) Temporary() bool { return true }

type scriptedRUDPConn struct {
	mu sync.Mutex

	readCh chan []byte
	closed chan struct{}

	readDeadline  time.Time
	writeDeadline time.Time

	writes    [][]byte
	writeHook func(data []byte, call int)
	writeCall int
}

type concurrentWriteDetectConn struct {
	mu sync.Mutex

	readCh chan []byte

	activeOps       int32
	overlapDetected int32

	writeSeq uint32
}

func newConcurrentWriteDetectConn() *concurrentWriteDetectConn {
	return &concurrentWriteDetectConn{readCh: make(chan []byte, 256)}
}

func (c *concurrentWriteDetectConn) beginOp() {
	if atomic.AddInt32(&c.activeOps, 1) > 1 {
		atomic.StoreInt32(&c.overlapDetected, 1)
	}
}

func (c *concurrentWriteDetectConn) endOp() {
	atomic.AddInt32(&c.activeOps, -1)
}

func (c *concurrentWriteDetectConn) Read(p []byte) (int, error) {
	data := <-c.readCh
	n := copy(p, data)
	return n, nil
}

func (c *concurrentWriteDetectConn) Write(b []byte) (int, error) {
	c.beginOp()
	defer c.endOp()

	time.Sleep(2 * time.Millisecond)

	frame, err := decodeRUDPFrame(b)
	if err == nil && frame.typ == rudpTypeData {
		ack, _ := encodeRUDPAck(frame.seq)
		c.readCh <- ack
	}

	return len(b), nil
}

func (c *concurrentWriteDetectConn) Close() error         { return nil }
func (c *concurrentWriteDetectConn) LocalAddr() net.Addr  { return testAddr("local") }
func (c *concurrentWriteDetectConn) RemoteAddr() net.Addr { return testAddr("remote") }
func (c *concurrentWriteDetectConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}
func (c *concurrentWriteDetectConn) SetReadDeadline(t time.Time) error { _ = t; return nil }
func (c *concurrentWriteDetectConn) SetWriteDeadline(t time.Time) error {
	c.beginOp()
	time.Sleep(2 * time.Millisecond)
	c.endOp()
	_ = t
	return nil
}

func (c *concurrentWriteDetectConn) OverlapDetected() bool {
	return atomic.LoadInt32(&c.overlapDetected) == 1
}

func newScriptedRUDPConn() *scriptedRUDPConn {
	return &scriptedRUDPConn{
		readCh: make(chan []byte, 64),
		closed: make(chan struct{}),
	}
}

func (c *scriptedRUDPConn) Read(p []byte) (int, error) {
	for {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case data := <-c.readCh:
			n := copy(p, data)
			return n, nil
		default:
		}

		c.mu.Lock()
		deadline := c.readDeadline
		c.mu.Unlock()

		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, rudpTimeoutErr{}
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (c *scriptedRUDPConn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}

	c.mu.Lock()
	c.writeCall++
	call := c.writeCall
	copied := append([]byte(nil), b...)
	c.writes = append(c.writes, copied)
	hook := c.writeHook
	c.mu.Unlock()

	if hook != nil {
		hook(copied, call)
	}

	return len(b), nil
}

func (c *scriptedRUDPConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}

	return nil
}

func (c *scriptedRUDPConn) LocalAddr() net.Addr  { return testAddr("local") }
func (c *scriptedRUDPConn) RemoteAddr() net.Addr { return testAddr("remote") }
func (c *scriptedRUDPConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}
func (c *scriptedRUDPConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}
func (c *scriptedRUDPConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *scriptedRUDPConn) enqueue(frame []byte) {
	c.readCh <- append([]byte(nil), frame...)
}

func (c *scriptedRUDPConn) writesSnapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([][]byte, len(c.writes))
	for i := range c.writes {
		out[i] = append([]byte(nil), c.writes[i]...)
	}

	return out
}

func ackWriteCount(writes [][]byte) int {
	count := 0
	for _, w := range writes {
		frame, err := decodeRUDPFrame(w)
		if err == nil && frame.typ == rudpTypeAck {
			count++
		}
	}

	return count
}

func newRUDPTestClient(t *testing.T) *Client {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.MaxPayloadSize = maxRUDPPayloadSize
	cfg.RUDPInitialRetransmitTimeout = 20 * time.Millisecond
	cfg.RUDPMaxRetransmitTimeout = 80 * time.Millisecond
	cfg.RUDPMaxRetries = 4
	cfg.RUDPMaxInFlight = 8
	cfg.RUDPReceiveQueueSize = 8

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	return client
}

func serializedPayload(t *testing.T, payload []byte) []byte {
	t.Helper()

	p := NewPacket(payload)
	data, err := p.Serialize()
	if err != nil {
		t.Fatalf("serialize payload: %v", err)
	}

	return data
}

func TestRUDPSuccessfulDataAckExchange(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	err := client.SendPacket(context.Background(), conn, []byte("RUDP_OK"))
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
}

func TestRUDPLostDataPacketRetransmits(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	conn.writeHook = func(data []byte, call int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		if call == 1 {
			return
		}

		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("LOSS_DATA")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if len(conn.writesSnapshot()) < 2 {
		t.Fatalf("expected retransmission after dropped data")
	}
}

func TestRUDPLostACKRetransmits(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	seenSeq := uint32(0)
	conn.writeHook = func(data []byte, call int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		if seenSeq == 0 {
			seenSeq = frame.seq
		}

		if call == 1 {
			return
		}

		ack, _ := encodeRUDPAck(seenSeq)
		conn.enqueue(ack)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("LOSS_ACK")); err != nil {
		t.Fatalf("send failed: %v", err)
	}
}

func TestRUDPDelayedACK(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		go func(seq uint32) {
			time.Sleep(5 * time.Millisecond)
			ack, _ := encodeRUDPAck(seq)
			conn.enqueue(ack)
		}(frame.seq)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("DELAY_ACK")); err != nil {
		t.Fatalf("send failed: %v", err)
	}
}

func TestRUDPDuplicateDataAndACKHandling(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	payload := []byte("HELLO")
	dataFrame, err := encodeRUDPData(1, serializedPayload(t, payload))
	if err != nil {
		t.Fatalf("encode data frame: %v", err)
	}

	var ackCount atomic.Int32
	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err == nil && frame.typ == rudpTypeAck {
			ackCount.Add(1)
		}
	}

	conn.enqueue(dataFrame)
	conn.enqueue(dataFrame)

	recv, err := client.ReceivePacket(context.Background(), conn)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	if string(recv) != "HELLO" {
		t.Fatalf("unexpected payload: %q", recv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.ReceivePacket(ctx, conn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no duplicate delivery, got: %v", err)
	}

	if ackCount.Load() < 2 {
		t.Fatalf("expected ACK for duplicate data frames")
	}
}

func TestRUDPLateAndInvalidACKIgnored(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	writeSeqs := make([]uint32, 0, 2)
	conn.writeHook = func(data []byte, call int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		writeSeqs = append(writeSeqs, frame.seq)
		if call == 1 {
			badAck, _ := encodeRUDPAck(frame.seq + 100)
			conn.enqueue(badAck)
			return
		}

		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("ACK_CHECK")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if len(writeSeqs) < 2 {
		t.Fatalf("expected retry after invalid ACK")
	}
}

func TestRUDPReorderedDataRejectedUntilExpected(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	p1 := serializedPayload(t, []byte("ONE"))
	p2 := serializedPayload(t, []byte("TWO"))
	f2, _ := encodeRUDPData(2, p2)
	f1, _ := encodeRUDPData(1, p1)

	conn.enqueue(f2)
	conn.enqueue(f1)

	recv, err := client.ReceivePacket(context.Background(), conn)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	if string(recv) != "ONE" {
		t.Fatalf("unexpected payload: %q", recv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.ReceivePacket(ctx, conn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected reordered frame to be rejected; got %v", err)
	}
}

func TestRUDPRetryExhaustion(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	err := client.SendPacket(context.Background(), conn, []byte("NEVER_ACK"))
	if err == nil {
		t.Fatal("expected retry exhaustion")
	}

	if !errors.Is(err, ErrRUDPRetryExhausted) {
		t.Fatalf("expected ErrRUDPRetryExhausted, got: %v", err)
	}
}

func TestRUDPContextCancellationDuringSend(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	err := client.SendPacket(ctx, conn, []byte("CANCEL"))
	if err == nil {
		t.Fatal("expected context cancellation")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got: %v", err)
	}
}

func TestRUDPConnectionClosureDuringRetransmission(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = conn.Close()
	}()

	err := client.SendPacket(context.Background(), conn, []byte("CLOSE"))
	if err == nil {
		t.Fatal("expected session closed error")
	}
}

func TestRUDPReceiveMalformedHeader(t *testing.T) {
	_, err := decodeRUDPFrame([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected malformed header error")
	}

	if !errors.Is(err, ErrRUDPInvalidPacket) {
		t.Fatalf("expected ErrRUDPInvalidPacket, got: %v", err)
	}
}

func TestRUDPInvalidPayloadLength(t *testing.T) {
	valid, err := encodeRUDPData(1, []byte("abc"))
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	binary := append([]byte(nil), valid...)
	binary[14] = 0x00
	binary[15] = 0x04

	_, err = decodeRUDPFrame(binary)
	if err == nil {
		t.Fatal("expected payload length mismatch error")
	}
}

func TestRUDPOversizedPayloadRejected(t *testing.T) {
	payload := bytes.Repeat([]byte("X"), int(maxRUDPPayloadSize)+1)
	_, err := encodeRUDPData(1, payload)
	if err == nil {
		t.Fatal("expected oversized payload error")
	}

	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("expected ErrPacketTooLarge, got: %v", err)
	}
}

func TestRUDPConcurrentSendsNoSequenceCollision(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	var mu sync.Mutex
	seqs := map[uint32]struct{}{}
	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		mu.Lock()
		seqs[frame.seq] = struct{}{}
		mu.Unlock()

		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- client.SendPacket(context.Background(), conn, []byte("A")) }()
	go func() { errCh <- client.SendPacket(context.Background(), conn, []byte("B")) }()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 2 {
		t.Fatalf("expected 2 unique sequence numbers, got %d", len(seqs))
	}
}

func TestRUDPPendingStateCleanup(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}
		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("CLEANUP")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	state := client.getOrCreateRUDPSession(conn)
	state.mu.Lock()
	pending := len(state.pending)
	state.mu.Unlock()

	if pending != 0 {
		t.Fatalf("expected no pending ACK entries, got %d", pending)
	}
}

func TestRUDPSequenceWraparoundHelpers(t *testing.T) {
	if !seqLess(0, 1) {
		t.Fatal("expected seqLess(0,1) true")
	}

	if seqLess(0, ^uint32(0)) {
		t.Fatal("expected wrap-aware comparison to consider 0 newer than max uint32")
	}

	if nextBackoff(20*time.Millisecond, 80*time.Millisecond) != 40*time.Millisecond {
		t.Fatal("unexpected backoff step")
	}

	if nextBackoff(60*time.Millisecond, 80*time.Millisecond) != 80*time.Millisecond {
		t.Fatal("backoff must clamp to max")
	}
}

func TestRUDPInFlightCapacityExhausted(t *testing.T) {
	client := newRUDPTestClient(t)
	client.config.RUDPMaxInFlight = 1

	conn := newScriptedRUDPConn()
	defer conn.Close()

	started := make(chan struct{})
	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err == nil && frame.typ == rudpTypeData {
			select {
			case <-started:
			default:
				close(started)
			}
		}
	}

	errCh := make(chan error, 2)
	go func() { errCh <- client.SendPacket(context.Background(), conn, []byte("first")) }()
	<-started
	go func() { errCh <- client.SendPacket(context.Background(), conn, []byte("second")) }()

	err1 := <-errCh
	err2 := <-errCh

	if !errors.Is(err1, ErrRUDPInFlightCapacity) && !errors.Is(err2, ErrRUDPInFlightCapacity) {
		t.Fatalf("expected one send to fail with ErrRUDPInFlightCapacity, got %v / %v", err1, err2)
	}
}

func TestRUDPDecodeACKValidation(t *testing.T) {
	ack, err := encodeRUDPAck(0)
	if err != nil {
		t.Fatalf("failed to encode zero ack frame: %v", err)
	}

	frame, err := decodeRUDPFrame(ack)
	if err != nil {
		t.Fatalf("zero ACK decode should be valid, got: %v", err)
	}

	if frame.typ != rudpTypeAck || frame.ack != 0 {
		t.Fatalf("unexpected ack decode result: type=%d ack=%d", frame.typ, frame.ack)
	}
}

func TestRUDPReceivePacketClosedSession(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	state := client.getOrCreateRUDPSession(conn)
	state.closeWithError(fmt.Errorf("closed"))

	_, err := client.ReceivePacket(context.Background(), conn)
	if err == nil {
		t.Fatal("expected closed session error")
	}
}

func TestRUDPWraparoundTransitionMaxToZeroToOne(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	state := client.getOrCreateRUDPSession(conn)
	state.mu.Lock()
	state.nextSeq = ^uint32(0) - 1
	state.mu.Unlock()

	seqCh := make(chan uint32, 2)
	conn.writeHook = func(data []byte, _ int) {
		frame, err := decodeRUDPFrame(data)
		if err != nil || frame.typ != rudpTypeData {
			return
		}

		seqCh <- frame.seq
		ack, _ := encodeRUDPAck(frame.seq)
		conn.enqueue(ack)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("WRAP_A")); err != nil {
		t.Fatalf("first send failed: %v", err)
	}

	if err := client.SendPacket(context.Background(), conn, []byte("WRAP_B")); err != nil {
		t.Fatalf("second send failed: %v", err)
	}

	first := <-seqCh
	second := <-seqCh

	if first != ^uint32(0) {
		t.Fatalf("expected first sequence MaxUint32, got %d", first)
	}

	if second != 0 {
		t.Fatalf("expected second sequence 0 after wrap, got %d", second)
	}

	state.mu.Lock()
	state.recvInit = true
	state.recvLast = ^uint32(0) - 1
	state.window.add(^uint32(0) - 1)
	state.mu.Unlock()

	decision, err := state.classifyIncomingData(^uint32(0))
	if err != nil || decision != rudpIncomingDeliver {
		t.Fatalf("expected deliver for first wrap receive, got decision=%v err=%v", decision, err)
	}
	state.markDelivered(^uint32(0))

	decision, err = state.classifyIncomingData(0)
	if err != nil || decision != rudpIncomingDeliver {
		t.Fatalf("expected deliver for wrapped zero sequence, got decision=%v err=%v", decision, err)
	}
	state.markDelivered(0)

	decision, err = state.classifyIncomingData(1)
	if err != nil || decision != rudpIncomingDeliver {
		t.Fatalf("expected deliver for post-wrap sequence one, got decision=%v err=%v", decision, err)
	}
}

func TestRUDPMalformedDataNotAcknowledgedClientSide(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newScriptedRUDPConn()
	defer conn.Close()

	bad := []byte{0x00, 0x01, 0x02}
	frame, err := encodeRUDPData(1, bad)
	if err != nil {
		t.Fatalf("encode frame failed: %v", err)
	}

	conn.enqueue(frame)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = client.ReceivePacket(ctx, conn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout with malformed payload, got: %v", err)
	}

	if ackWriteCount(conn.writesSnapshot()) != 0 {
		t.Fatal("malformed packet must not be acknowledged")
	}
}

func TestRUDPPeerCleanupAndCapacityRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.MaxConnections = 1
	cfg.MaxPayloadSize = maxRUDPPayloadSize
	cfg.ReadTimeout = time.Second

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	transport := server.transport.(*RUDPTransport)

	peerA := transport.getOrCreatePeer(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20001})
	if peerA == nil {
		t.Fatal("expected first peer to be created")
	}

	peerA.mu.Lock()
	peerA.lastSeen = time.Now().Add(-5 * time.Second)
	peerA.mu.Unlock()

	transport.cleanupExpiredPeers()

	peerB := transport.getOrCreatePeer(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20002})
	if peerB == nil {
		t.Fatal("expected capacity recovery after peer cleanup")
	}
}

func TestRUDPPeerCleanupSkipsActivePending(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.MaxPayloadSize = maxRUDPPayloadSize
	cfg.ReadTimeout = time.Second

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	transport := server.transport.(*RUDPTransport)
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 21001}
	peer := transport.getOrCreatePeer(addr)
	if peer == nil {
		t.Fatal("expected peer to be created")
	}

	peer.mu.Lock()
	peer.lastSeen = time.Now().Add(-5 * time.Second)
	peer.pending[1] = make(chan struct{})
	peer.mu.Unlock()

	transport.cleanupExpiredPeers()

	if got := transport.getPeer(addr); got == nil {
		t.Fatal("peer with pending reliable send must not be removed")
	}
}

func TestRUDPAckUnknownDuplicateLateUnmatchedValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.MaxPayloadSize = maxRUDPPayloadSize

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	transport := server.transport.(*RUDPTransport)
	unknown := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 22001}
	if transport.getPeer(unknown) != nil {
		t.Fatal("unknown peer should not exist")
	}

	peerAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 22002}
	peer := transport.getOrCreatePeer(peerAddr)
	if peer == nil {
		t.Fatal("expected peer to be created")
	}

	peer.mu.Lock()
	peer.pending[42] = make(chan struct{})
	peer.mu.Unlock()

	if !peer.ackReceived(42) {
		t.Fatal("expected matched ACK to be accepted")
	}

	if peer.ackReceived(42) {
		t.Fatal("duplicate ACK must not match pending after first completion")
	}

	if peer.ackReceived(7) {
		t.Fatal("unmatched ACK must not match pending state")
	}

	if peer.ackReceived(41) {
		t.Fatal("late ACK for non-pending sequence must not match")
	}
}

func TestRUDPClientConcurrentWritesAndDeadlinesSerialized(t *testing.T) {
	client := newRUDPTestClient(t)
	conn := newConcurrentWriteDetectConn()

	errCh := make(chan error, 2)
	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("SER_A"))
	}()
	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("SER_B"))
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent send failed: %v", err)
		}
	}

	if conn.OverlapDetected() {
		t.Fatal("detected concurrent RUDP SetWriteDeadline/Write operations")
	}
}

func TestRUDPNoNewResponseWorkerAfterShutdownBegins(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.MaxPayloadSize = maxRUDPPayloadSize

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	transport := server.transport.(*RUDPTransport)
	if err := transport.Close(); err != nil {
		t.Fatalf("transport close failed: %v", err)
	}

	peer := newRUDPServerPeerState(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 23001})
	started := transport.startResponseWorker(peer.addr, []byte("X"), peer)
	if started {
		t.Fatal("response worker must not start after shutdown begins")
	}
}
