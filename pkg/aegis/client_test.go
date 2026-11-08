package aegis

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type writeStep struct {
	n       int
	err     error
	blockCh <-chan struct{}
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

type mockConn struct {
	mu sync.Mutex

	writeSteps       []writeStep
	writeStepIndex   int
	writeCalls       int
	writes           [][]byte
	writeDeadline    time.Time
	setWriteDeadline int
	onWrite          func(call int)
}

func (m *mockConn) Read(_ []byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (m *mockConn) Write(b []byte) (int, error) {
	m.mu.Lock()
	m.writeCalls++
	call := m.writeCalls
	copied := make([]byte, len(b))
	copy(copied, b)
	m.writes = append(m.writes, copied)

	var step writeStep
	hasStep := false

	if m.writeStepIndex < len(m.writeSteps) {
		step = m.writeSteps[m.writeStepIndex]
		m.writeStepIndex++
		hasStep = true
	}

	onWrite := m.onWrite
	m.mu.Unlock()

	if onWrite != nil {
		onWrite(call)
	}

	if hasStep {
		if step.blockCh != nil {
			<-step.blockCh
		}

		n := step.n
		if n > len(b) {
			n = len(b)
		}

		return n, step.err
	}

	return len(b), nil
}

func (m *mockConn) Close() error { return nil }

func (m *mockConn) LocalAddr() net.Addr {
	return testAddr("local")
}

func (m *mockConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}

func (m *mockConn) SetDeadline(_ time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setWriteDeadline++
	m.writeDeadline = t
	return nil
}

func (m *mockConn) writeCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.writeCalls
}

func (m *mockConn) writesSnapshot() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([][]byte, len(m.writes))
	for i := range m.writes {
		out[i] = append([]byte(nil), m.writes[i]...)
	}

	return out
}

type observedConn struct {
	net.Conn
	writeStarted chan struct{}
	once         sync.Once
}

func (o *observedConn) Write(b []byte) (int, error) {
	o.once.Do(func() {
		close(o.writeStarted)
	})

	return o.Conn.Write(b)
}

type readTimeoutErr struct{}

func (readTimeoutErr) Error() string   { return "read timeout" }
func (readTimeoutErr) Timeout() bool   { return true }
func (readTimeoutErr) Temporary() bool { return true }

type readTimeoutConn struct{}

func (readTimeoutConn) Read(_ []byte) (int, error)         { return 0, readTimeoutErr{} }
func (readTimeoutConn) Write(_ []byte) (int, error)        { return 0, errors.New("not implemented") }
func (readTimeoutConn) Close() error                       { return nil }
func (readTimeoutConn) LocalAddr() net.Addr                { return testAddr("local") }
func (readTimeoutConn) RemoteAddr() net.Addr               { return testAddr("remote") }
func (readTimeoutConn) SetDeadline(_ time.Time) error      { return nil }
func (readTimeoutConn) SetReadDeadline(_ time.Time) error  { return nil }
func (readTimeoutConn) SetWriteDeadline(_ time.Time) error { return nil }

type panicReadConn struct{}

func (panicReadConn) Read(_ []byte) (int, error)         { panic("unexpected read call") }
func (panicReadConn) Write(_ []byte) (int, error)        { return 0, errors.New("not implemented") }
func (panicReadConn) Close() error                       { return nil }
func (panicReadConn) LocalAddr() net.Addr                { return testAddr("local") }
func (panicReadConn) RemoteAddr() net.Addr               { return testAddr("remote") }
func (panicReadConn) SetDeadline(_ time.Time) error      { return nil }
func (panicReadConn) SetReadDeadline(_ time.Time) error  { return nil }
func (panicReadConn) SetWriteDeadline(_ time.Time) error { return nil }

func newUDPClientAndSocket(t *testing.T) (*Client, net.Conn, *net.UDPConn) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create UDP client: %v", err)
	}

	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to create UDP server socket: %v", err)
	}

	conn, err := client.Connect(serverConn.LocalAddr().String())
	if err != nil {
		_ = serverConn.Close()
		t.Fatalf("failed to connect UDP client: %v", err)
	}

	return client, conn, serverConn
}

type writeOverlapConn struct {
	mu sync.Mutex

	blockFirstWrite chan struct{}
	firstWriteSeen  chan struct{}
	activeWrites    int32
	overlapDetected int32

	writes []byte
}

func (c *writeOverlapConn) Read(_ []byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (c *writeOverlapConn) Write(b []byte) (int, error) {
	if atomic.AddInt32(&c.activeWrites, 1) > 1 {
		atomic.StoreInt32(&c.overlapDetected, 1)
	}
	defer atomic.AddInt32(&c.activeWrites, -1)

	if c.firstWriteSeen != nil {
		select {
		case <-c.firstWriteSeen:
		default:
			close(c.firstWriteSeen)
			if c.blockFirstWrite != nil {
				<-c.blockFirstWrite
			}
		}
	}

	c.mu.Lock()
	c.writes = append(c.writes, b...)
	c.mu.Unlock()

	return len(b), nil
}

func (c *writeOverlapConn) Close() error                       { return nil }
func (c *writeOverlapConn) LocalAddr() net.Addr                { return testAddr("local") }
func (c *writeOverlapConn) RemoteAddr() net.Addr               { return testAddr("remote") }
func (c *writeOverlapConn) SetDeadline(_ time.Time) error      { return nil }
func (c *writeOverlapConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *writeOverlapConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *writeOverlapConn) OverlapDetected() bool {
	return atomic.LoadInt32(&c.overlapDetected) == 1
}

type readOverlapConn struct {
	mu sync.Mutex

	blockFirstRead  chan struct{}
	firstReadSeen   chan struct{}
	activeReads     int32
	overlapDetected int32

	data   []byte
	offset int
}

func (c *readOverlapConn) Read(p []byte) (int, error) {
	if atomic.AddInt32(&c.activeReads, 1) > 1 {
		atomic.StoreInt32(&c.overlapDetected, 1)
	}
	defer atomic.AddInt32(&c.activeReads, -1)

	if c.firstReadSeen != nil {
		select {
		case <-c.firstReadSeen:
		default:
			close(c.firstReadSeen)
			if c.blockFirstRead != nil {
				<-c.blockFirstRead
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.offset >= len(c.data) {
		return 0, errors.New("no more data")
	}

	n := copy(p, c.data[c.offset:])
	c.offset += n
	return n, nil
}

func (c *readOverlapConn) Write(_ []byte) (int, error)        { return 0, errors.New("not implemented") }
func (c *readOverlapConn) Close() error                       { return nil }
func (c *readOverlapConn) LocalAddr() net.Addr                { return testAddr("local") }
func (c *readOverlapConn) RemoteAddr() net.Addr               { return testAddr("remote") }
func (c *readOverlapConn) SetDeadline(_ time.Time) error      { return nil }
func (c *readOverlapConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *readOverlapConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *readOverlapConn) OverlapDetected() bool {
	return atomic.LoadInt32(&c.overlapDetected) == 1
}

func TestSendPacketTCPCompleteWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	conn := &mockConn{}
	payload := []byte("HELLO_TCP_FULL_WRITE")

	if err := client.SendPacket(context.Background(), conn, payload); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if conn.writeCallCount() != 1 {
		t.Fatalf("expected exactly 1 TCP write call, got %d", conn.writeCallCount())
	}

	writes := conn.writesSnapshot()
	if len(writes) != 1 {
		t.Fatalf("expected one captured write, got %d", len(writes))
	}

	expected, err := NewPacket(payload).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize expected packet: %v", err)
	}

	if string(writes[0]) != string(expected) {
		t.Fatalf("written bytes mismatch with serialized packet")
	}
}

func TestSendPacketTCPPartialWrites(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	payload := []byte("PARTIAL_WRITE_PAYLOAD_FOR_TCP")
	expected, err := NewPacket(payload).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize expected packet: %v", err)
	}

	conn := &mockConn{
		writeSteps: []writeStep{
			{n: 4, err: nil},
			{n: 3, err: nil},
			{n: len(expected) - 7, err: nil},
		},
	}

	if err := client.SendPacket(context.Background(), conn, payload); err != nil {
		t.Fatalf("expected no error for partial writes, got: %v", err)
	}

	if conn.writeCallCount() != 3 {
		t.Fatalf("expected 3 write calls for partial writes, got %d", conn.writeCallCount())
	}

	writes := conn.writesSnapshot()

	if string(writes[0]) != string(expected) {
		t.Fatalf("first write should contain full initial remaining stream")
	}

	if string(writes[1]) != string(expected[4:]) {
		t.Fatalf("second write should start from remaining offset 4")
	}

	if string(writes[2]) != string(expected[7:]) {
		t.Fatalf("third write should start from remaining offset 7")
	}
}

func TestSendPacketTCPPartialWriteWithError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	writeErr := errors.New("partial write failure")
	conn := &mockConn{
		writeSteps: []writeStep{
			{n: 5, err: writeErr},
		},
	}

	err = client.SendPacket(context.Background(), conn, []byte("PARTIAL_ERR"))
	if err == nil {
		t.Fatal("expected error when write returns n>0 with error, got nil")
	}

	if !errors.Is(err, writeErr) {
		t.Fatalf("expected wrapped original write error, got: %v", err)
	}

	if conn.writeCallCount() != 1 {
		t.Fatalf("expected exactly 1 write call after partial error, got %d", conn.writeCallCount())
	}
}

func TestSendPacketTCPZeroByteWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	conn := &mockConn{
		writeSteps: []writeStep{
			{n: 0, err: nil},
		},
	}

	err = client.SendPacket(context.Background(), conn, []byte("ZERO_WRITE"))
	if err == nil {
		t.Fatal("expected error for zero-byte write, got nil")
	}

	if !strings.Contains(err.Error(), "short write") {
		t.Fatalf("expected short write error, got: %v", err)
	}
}

func TestSendPacketTCPWriteError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	writeErr := errors.New("socket broken")
	conn := &mockConn{
		writeSteps: []writeStep{
			{n: 0, err: writeErr},
		},
	}

	err = client.SendPacket(context.Background(), conn, []byte("ERR_WRITE"))
	if err == nil {
		t.Fatal("expected write error, got nil")
	}

	if !errors.Is(err, writeErr) {
		t.Fatalf("expected wrapped write error, got: %v", err)
	}
}

func TestSendPacketTCPCancelBeforeWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	conn := &mockConn{}
	err = client.SendPacket(ctx, conn, []byte("CANCELLED_BEFORE"))
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if conn.writeCallCount() != 0 {
		t.Fatalf("expected no write calls when context is canceled before write, got %d", conn.writeCallCount())
	}
}

func TestSendPacketTCPCancelBetweenPartialWrites(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &mockConn{
		writeSteps: []writeStep{
			{n: 3, err: nil},
		},
		onWrite: func(call int) {
			if call == 1 {
				cancel()
			}
		},
	}

	err = client.SendPacket(ctx, conn, []byte("CANCEL_BETWEEN_WRITES"))
	if err == nil {
		t.Fatal("expected cancellation error between partial writes, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if conn.writeCallCount() != 1 {
		t.Fatalf("expected exactly 1 write call before cancellation, got %d", conn.writeCallCount())
	}
}

func TestSendPacketTCPCancelDuringBlockedWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	baseConn, peerConn := net.Pipe()
	defer func() {
		_ = baseConn.Close()
		_ = peerConn.Close()
	}()

	observed := &observedConn{
		Conn:         baseConn,
		writeStarted: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := bytes.Repeat([]byte("A"), 1024)

	done := make(chan error, 1)
	go func() {
		done <- client.SendPacket(ctx, observed, payload)
	}()

	select {
	case <-observed.writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for write to start")
	}

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error for blocked write, got nil")
		}

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("blocked write did not return after context cancellation")
	}
}

func TestSendPacketUDPSingleDatagramWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	conn := &mockConn{}
	payload := []byte("UDP_SINGLE_DATAGRAM")

	if err := client.SendPacket(context.Background(), conn, payload); err != nil {
		t.Fatalf("expected no UDP send error, got: %v", err)
	}

	if conn.writeCallCount() != 1 {
		t.Fatalf("expected single UDP datagram write call, got %d", conn.writeCallCount())
	}

	expected, err := NewPacket(payload).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize expected packet: %v", err)
	}

	writes := conn.writesSnapshot()

	if len(writes) != 1 || string(writes[0]) != string(expected) {
		t.Fatalf("UDP write payload mismatch")
	}
}

func TestSendPacketUDPSingleDatagramNetworkBoundary(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	payloadA := []byte("UDP_NET_A")
	payloadB := []byte("UDP_NET_B")

	expectedA, err := NewPacket(payloadA).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet A: %v", err)
	}

	expectedB, err := NewPacket(payloadB).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet B: %v", err)
	}

	if err := client.SendPacket(context.Background(), conn, payloadA); err != nil {
		t.Fatalf("send packet A failed: %v", err)
	}

	if err := client.SendPacket(context.Background(), conn, payloadB); err != nil {
		t.Fatalf("send packet B failed: %v", err)
	}

	buf := make([]byte, maxUDPDatagramSize)

	if err := serverConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	n1, _, err := serverConn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("failed to read first UDP datagram: %v", err)
	}
	first := append([]byte(nil), buf[:n1]...)

	n2, _, err := serverConn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("failed to read second UDP datagram: %v", err)
	}
	second := append([]byte(nil), buf[:n2]...)

	if !bytes.Equal(first, expectedA) {
		t.Fatalf("first UDP datagram mismatch")
	}

	if !bytes.Equal(second, expectedB) {
		t.Fatalf("second UDP datagram mismatch")
	}
}

func TestSendPacketUDPEmptyPayload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	conn := &mockConn{}
	if err := client.SendPacket(context.Background(), conn, []byte{}); err != nil {
		t.Fatalf("expected empty UDP payload to be supported, got: %v", err)
	}

	if conn.writeCallCount() != 1 {
		t.Fatalf("expected one UDP datagram write for empty payload, got %d", conn.writeCallCount())
	}

	writes := conn.writesSnapshot()
	if len(writes) != 1 || len(writes[0]) != HeaderSize {
		t.Fatalf("expected exactly header-size UDP packet for empty payload")
	}
}

func TestSendPacketUDPMaxPayload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	payload := bytes.Repeat([]byte("X"), int(maxUDPPayloadSize))
	conn := &mockConn{}

	if err := client.SendPacket(context.Background(), conn, payload); err != nil {
		t.Fatalf("expected max UDP payload send to succeed, got: %v", err)
	}

	writes := conn.writesSnapshot()
	if len(writes) != 1 || len(writes[0]) != maxUDPDatagramSize {
		t.Fatalf("expected one max-size UDP datagram")
	}
}

func TestSendPacketUDPOversizedPayloadRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	payload := bytes.Repeat([]byte("Y"), int(maxUDPPayloadSize)+1)
	err = client.SendPacket(context.Background(), &mockConn{}, payload)
	if err == nil {
		t.Fatal("expected ErrPacketTooLarge for oversized UDP payload, got nil")
	}

	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("expected ErrPacketTooLarge, got: %v", err)
	}
}

func TestSendPacketUDPWriteErrorPropagation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	writeErr := errors.New("udp write failed")
	conn := &mockConn{writeSteps: []writeStep{{n: 0, err: writeErr}}}

	err = client.SendPacket(context.Background(), conn, []byte("UDP_FAIL"))
	if err == nil {
		t.Fatal("expected UDP write error, got nil")
	}

	if !errors.Is(err, writeErr) {
		t.Fatalf("expected wrapped UDP write error, got: %v", err)
	}
}

func TestReceivePacketUDPSuccess(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	payload := []byte("UDP_RECEIVE_SUCCESS")
	packet, err := NewPacket(payload).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize UDP packet: %v", err)
	}

	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	if _, err := serverConn.WriteToUDP(packet, clientAddr); err != nil {
		t.Fatalf("failed to write UDP datagram to client: %v", err)
	}

	received, err := client.ReceivePacket(context.Background(), conn)
	if err != nil {
		t.Fatalf("expected UDP receive success, got: %v", err)
	}

	if !bytes.Equal(received, payload) {
		t.Fatalf("received UDP payload mismatch")
	}
}

func TestReceivePacketUDPMultipleDatagrams(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	payloadA := []byte("UDP_RCV_A")
	payloadB := []byte("UDP_RCV_B")

	packetA, err := NewPacket(payloadA).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet A: %v", err)
	}

	packetB, err := NewPacket(payloadB).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet B: %v", err)
	}

	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	if _, err := serverConn.WriteToUDP(packetA, clientAddr); err != nil {
		t.Fatalf("failed to send packet A: %v", err)
	}

	if _, err := serverConn.WriteToUDP(packetB, clientAddr); err != nil {
		t.Fatalf("failed to send packet B: %v", err)
	}

	receivedA, err := client.ReceivePacket(context.Background(), conn)
	if err != nil {
		t.Fatalf("receive A failed: %v", err)
	}

	receivedB, err := client.ReceivePacket(context.Background(), conn)
	if err != nil {
		t.Fatalf("receive B failed: %v", err)
	}

	if !bytes.Equal(receivedA, payloadA) || !bytes.Equal(receivedB, payloadB) {
		t.Fatalf("UDP datagram boundary/content mismatch")
	}
}

func TestReceivePacketUDPEmptyDatagram(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	if _, err := serverConn.WriteToUDP([]byte{}, clientAddr); err != nil {
		t.Fatalf("failed to send empty UDP datagram: %v", err)
	}

	_, err := client.ReceivePacket(context.Background(), conn)
	if err == nil {
		t.Fatal("expected empty UDP datagram to be rejected")
	}

	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("expected ErrInvalidPacket, got: %v", err)
	}
}

func TestReceivePacketUDPTruncatedDatagram(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	payload := []byte("UDP_TRUNCATE")
	packet, err := NewPacket(payload).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet: %v", err)
	}

	truncated := packet[:len(packet)-3]
	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	if _, err := serverConn.WriteToUDP(truncated, clientAddr); err != nil {
		t.Fatalf("failed to send truncated UDP datagram: %v", err)
	}

	_, err = client.ReceivePacket(context.Background(), conn)
	if err == nil {
		t.Fatal("expected truncated UDP datagram to fail")
	}

	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("expected ErrInvalidPacket, got: %v", err)
	}
}

func TestReceivePacketUDPOversizedLengthRejected(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	defer func() {
		_ = conn.Close()
		_ = serverConn.Close()
	}()

	header := make([]byte, HeaderSize)
	copy(header[:2], MagicBytes[:])
	binary.BigEndian.PutUint32(header[2:6], maxUDPPayloadSize+1)
	binary.BigEndian.PutUint32(header[6:10], 0)

	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	if _, err := serverConn.WriteToUDP(header, clientAddr); err != nil {
		t.Fatalf("failed to send oversized-length UDP datagram: %v", err)
	}

	_, err := client.ReceivePacket(context.Background(), conn)
	if err == nil {
		t.Fatal("expected oversized UDP datagram length to fail")
	}

	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("expected ErrPacketTooLarge, got: %v", err)
	}
}

func TestReceivePacketUDPCancelledContext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.ReceivePacket(ctx, panicReadConn{})
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestReceivePacketUDPTimeoutPropagation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.ReceivePacket(context.Background(), readTimeoutConn{})
	if err == nil {
		t.Fatal("expected UDP timeout error, got nil")
	}

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout wrapping, got: %v", err)
	}
}

func TestReceivePacketUDPConnectionClosed(t *testing.T) {
	client, conn, serverConn := newUDPClientAndSocket(t)
	_ = serverConn.Close()

	_ = conn.Close()

	_, err := client.ReceivePacket(context.Background(), conn)
	if err == nil {
		t.Fatal("expected closed UDP connection error, got nil")
	}

	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got: %v", err)
	}
}

func TestReceivePacketTCPSuccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	baseConn, peerConn := net.Pipe()
	defer func() {
		_ = baseConn.Close()
		_ = peerConn.Close()
	}()

	payload := []byte("TCP_RECEIVE_SUCCESS")

	packet := NewPacket(payload)
	data, err := packet.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet: %v", err)
	}

	go func() {
		_, _ = peerConn.Write(data)
	}()

	received, err := client.ReceivePacket(context.Background(), baseConn)
	if err != nil {
		t.Fatalf("expected successful receive, got: %v", err)
	}

	if string(received) != string(payload) {
		t.Fatalf("received payload mismatch. got %q want %q", string(received), string(payload))
	}
}

func TestReceivePacketTCPMultipleConsecutivePackets(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	baseConn, peerConn := net.Pipe()
	defer func() {
		_ = baseConn.Close()
		_ = peerConn.Close()
	}()

	payloadA := []byte("PACKET_A")
	payloadB := []byte("PACKET_B")

	packetA, err := NewPacket(payloadA).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize first packet: %v", err)
	}

	packetB, err := NewPacket(payloadB).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize second packet: %v", err)
	}

	go func() {
		_, _ = peerConn.Write(append(packetA, packetB...))
	}()

	receivedA, err := client.ReceivePacket(context.Background(), baseConn)
	if err != nil {
		t.Fatalf("failed to receive first packet: %v", err)
	}

	receivedB, err := client.ReceivePacket(context.Background(), baseConn)
	if err != nil {
		t.Fatalf("failed to receive second packet: %v", err)
	}

	if string(receivedA) != string(payloadA) {
		t.Fatalf("first payload mismatch. got %q want %q", string(receivedA), string(payloadA))
	}

	if string(receivedB) != string(payloadB) {
		t.Fatalf("second payload mismatch. got %q want %q", string(receivedB), string(payloadB))
	}
}

func TestReceivePacketTCPCancelledContext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.ReceivePacket(ctx, panicReadConn{})
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestReceivePacketTCPTimeoutPropagation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.ReceivePacket(context.Background(), readTimeoutConn{})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout wrapping, got: %v", err)
	}
}

func TestReceivePacketTCPConnectionClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	baseConn, peerConn := net.Pipe()
	defer func() {
		_ = baseConn.Close()
	}()

	_ = peerConn.Close()

	_, err = client.ReceivePacket(context.Background(), baseConn)
	if err == nil {
		t.Fatal("expected ErrConnectionClosed, got nil")
	}

	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got: %v", err)
	}
}

func TestTCPClientBidirectionalCommunication(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Protocol = ProtocolTCP

	clientA, err := NewClient(cfgA)
	if err != nil {
		t.Fatalf("failed to create client A: %v", err)
	}

	cfgB := DefaultConfig()
	cfgB.Protocol = ProtocolTCP

	clientB, err := NewClient(cfgB)
	if err != nil {
		t.Fatalf("failed to create client B: %v", err)
	}

	connA, connB := net.Pipe()
	defer func() {
		_ = connA.Close()
		_ = connB.Close()
	}()

	requestPayload := []byte("REQUEST_FROM_A")
	responsePayload := []byte("RESPONSE_FROM_B")

	serverDone := make(chan error, 1)
	go func() {
		req, err := clientB.ReceivePacket(context.Background(), connB)
		if err != nil {
			serverDone <- err
			return
		}

		if string(req) != string(requestPayload) {
			serverDone <- errors.New("unexpected request payload")
			return
		}

		serverDone <- clientB.SendPacket(context.Background(), connB, responsePayload)
	}()

	if err := clientA.SendPacket(context.Background(), connA, requestPayload); err != nil {
		t.Fatalf("client A send failed: %v", err)
	}

	response, err := clientA.ReceivePacket(context.Background(), connA)
	if err != nil {
		t.Fatalf("client A receive failed: %v", err)
	}

	if string(response) != string(responsePayload) {
		t.Fatalf("response payload mismatch. got %q want %q", string(response), string(responsePayload))
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server side flow failed: %v", err)
	}
}

func TestSendPacketTCPConcurrentWritesSerialized(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	conn := &writeOverlapConn{
		blockFirstWrite: make(chan struct{}),
		firstWriteSeen:  make(chan struct{}),
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("WRITE_ONE"))
	}()

	select {
	case <-conn.firstWriteSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first write to start")
	}

	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("WRITE_TWO"))
	}()

	time.Sleep(50 * time.Millisecond)

	if conn.OverlapDetected() {
		t.Fatal("detected concurrent TCP writes on same connection")
	}

	close(conn.blockFirstWrite)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
}

func TestReceivePacketTCPConcurrentReadsSerialized(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Protocol = ProtocolTCP

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	packetA, err := NewPacket([]byte("READ_ONE")).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet A: %v", err)
	}

	packetB, err := NewPacket([]byte("READ_TWO")).Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet B: %v", err)
	}

	conn := &readOverlapConn{
		blockFirstRead: make(chan struct{}),
		firstReadSeen:  make(chan struct{}),
		data:           append(packetA, packetB...),
	}

	type recvResult struct {
		payload []byte
		err     error
	}

	resultCh := make(chan recvResult, 2)
	go func() {
		payload, err := client.ReceivePacket(context.Background(), conn)
		resultCh <- recvResult{payload: payload, err: err}
	}()

	select {
	case <-conn.firstReadSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first read to start")
	}

	go func() {
		payload, err := client.ReceivePacket(context.Background(), conn)
		resultCh <- recvResult{payload: payload, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	if conn.OverlapDetected() {
		t.Fatal("detected concurrent TCP reads on same connection")
	}

	close(conn.blockFirstRead)

	res1 := <-resultCh
	res2 := <-resultCh

	if res1.err != nil {
		t.Fatalf("first receive failed: %v", res1.err)
	}

	if res2.err != nil {
		t.Fatalf("second receive failed: %v", res2.err)
	}

	if string(res1.payload) != "READ_ONE" || string(res2.payload) != "READ_TWO" {
		t.Fatalf("unexpected payload order/content: got %q and %q", string(res1.payload), string(res2.payload))
	}
}
