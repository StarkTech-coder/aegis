package aegis

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type rudpEchoHandler struct {
	mu       sync.Mutex
	received [][]byte
}

func (h *rudpEchoHandler) HandlePacket(_ []byte) error { return nil }

func (h *rudpEchoHandler) HandleRUDPPacket(payload []byte, _ *net.UDPAddr) ([]byte, error) {
	h.mu.Lock()
	h.received = append(h.received, append([]byte(nil), payload...))
	h.mu.Unlock()

	return append([]byte("RACK:"), payload...), nil
}

type dropFirstRUDPWriteConn struct {
	net.Conn
	mu      sync.Mutex
	dropped bool
}

func (c *dropFirstRUDPWriteConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if !c.dropped {
		c.dropped = true
		c.mu.Unlock()
		return len(b), nil
	}
	c.mu.Unlock()

	return c.Conn.Write(b)
}

func startRUDPServer(t *testing.T, handler PacketHandler) (*Server, string, <-chan error) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.Address = reserveUDPAddress(t)
	cfg.MaxPayloadSize = 2048
	cfg.RUDPInitialRetransmitTimeout = 30 * time.Millisecond
	cfg.RUDPMaxRetransmitTimeout = 120 * time.Millisecond
	cfg.RUDPMaxRetries = 5
	cfg.RUDPMaxInFlight = 32
	cfg.RUDPReceiveQueueSize = 32

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create RUDP server: %v", err)
	}

	server.SetHandler(handler)

	done := make(chan error, 1)
	go func() {
		done <- server.Start()
	}()

	time.Sleep(50 * time.Millisecond)

	return server, cfg.Address, done
}

func newRUDPClientConn(t *testing.T, addr string) (*Client, net.Conn) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolRUDP
	cfg.Address = addr
	cfg.MaxPayloadSize = 2048
	cfg.RUDPInitialRetransmitTimeout = 30 * time.Millisecond
	cfg.RUDPMaxRetransmitTimeout = 120 * time.Millisecond
	cfg.RUDPMaxRetries = 5
	cfg.RUDPMaxInFlight = 16
	cfg.RUDPReceiveQueueSize = 16

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create RUDP client: %v", err)
	}

	conn, err := client.Connect(addr)
	if err != nil {
		t.Fatalf("failed to connect RUDP client: %v", err)
	}

	return client, conn
}

func TestRUDPClientServerRequestResponse(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newRUDPClientConn(t, addr)
	defer func() { _ = conn.Close() }()

	messages := [][]byte{[]byte("ONE"), []byte("TWO"), []byte("THREE")}
	for _, msg := range messages {
		if err := client.SendPacket(context.Background(), conn, msg); err != nil {
			t.Fatalf("send failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}

		expected := append([]byte("RACK:"), msg...)
		if !bytes.Equal(resp, expected) {
			t.Fatalf("response mismatch: got %q want %q", resp, expected)
		}
	}
}

func TestRUDPRetransmissionAfterControlledDataLoss(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newRUDPClientConn(t, addr)
	defer func() { _ = conn.Close() }()

	lossy := &dropFirstRUDPWriteConn{Conn: conn}

	if err := client.SendPacket(context.Background(), lossy, []byte("LOSSY")); err != nil {
		t.Fatalf("send failed with controlled loss: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.ReceivePacket(ctx, lossy)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	if string(resp) != "RACK:LOSSY" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestRUDPMultipleIndependentClients(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	clientA, connA := newRUDPClientConn(t, addr)
	defer func() { _ = connA.Close() }()
	clientB, connB := newRUDPClientConn(t, addr)
	defer func() { _ = connB.Close() }()

	if err := clientA.SendPacket(context.Background(), connA, []byte("A")); err != nil {
		t.Fatalf("client A send failed: %v", err)
	}
	if err := clientB.SendPacket(context.Background(), connB, []byte("B")); err != nil {
		t.Fatalf("client B send failed: %v", err)
	}

	ctxA, cancelA := context.WithTimeout(context.Background(), 1*time.Second)
	respA, err := clientA.ReceivePacket(ctxA, connA)
	cancelA()
	if err != nil {
		t.Fatalf("client A receive failed: %v", err)
	}

	ctxB, cancelB := context.WithTimeout(context.Background(), 1*time.Second)
	respB, err := clientB.ReceivePacket(ctxB, connB)
	cancelB()
	if err != nil {
		t.Fatalf("client B receive failed: %v", err)
	}

	if string(respA) != "RACK:A" || string(respB) != "RACK:B" {
		t.Fatalf("unexpected responses A=%q B=%q", respA, respB)
	}
}

func TestRUDPConcurrentSendReceive(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newRUDPClientConn(t, addr)
	defer func() { _ = conn.Close() }()

	msgs := [][]byte{[]byte("C1"), []byte("C2")}
	errCh := make(chan error, 2)
	for _, msg := range msgs {
		msgCopy := append([]byte(nil), msg...)
		go func() {
			errCh <- client.SendPacket(context.Background(), conn, msgCopy)
		}()
	}

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent send failed: %v", err)
		}
	}

	got := map[string]struct{}{}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		got[string(resp)] = struct{}{}
	}

	if _, ok := got["RACK:C1"]; !ok {
		t.Fatalf("missing response RACK:C1: %v", got)
	}
	if _, ok := got["RACK:C2"]; !ok {
		t.Fatalf("missing response RACK:C2: %v", got)
	}
}

func TestRUDPServerShutdownWithPendingSends(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)

	client, conn := newRUDPClientConn(t, addr)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("SHUTDOWN")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("server stop failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop in time")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = client.ReceivePacket(ctx, conn)
}

func TestRUDPMalformedDataNotAcknowledgedServerSide(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	raw, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("failed to dial raw UDP: %v", err)
	}
	defer raw.Close()

	badDataFrame, err := encodeRUDPData(1, []byte{0x00, 0x01, 0x02})
	if err != nil {
		t.Fatalf("failed to encode bad RUDP frame: %v", err)
	}

	if _, err := raw.Write(badDataFrame); err != nil {
		t.Fatalf("failed to write malformed data frame: %v", err)
	}

	_ = raw.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1024)
	_, err = raw.Read(buf)
	if err == nil {
		t.Fatal("expected no ACK for malformed DATA payload")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout waiting for ACK, got: %v", err)
	}
}

func TestRUDPAckFromUnknownPeerDoesNotCreatePeer(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	raw, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("failed to dial raw UDP: %v", err)
	}
	defer raw.Close()

	ack, err := encodeRUDPAck(100)
	if err != nil {
		t.Fatalf("failed to encode ACK: %v", err)
	}

	if _, err := raw.Write(ack); err != nil {
		t.Fatalf("failed to write ACK: %v", err)
	}

	time.Sleep(80 * time.Millisecond)

	transport := server.transport.(*RUDPTransport)
	transport.mu.RLock()
	peerCount := len(transport.peers)
	transport.mu.RUnlock()

	if peerCount != 0 {
		t.Fatalf("unknown ACK must not create peer state, got %d peers", peerCount)
	}
}

func TestRUDPServerConcurrentWritesRemainStable(t *testing.T) {
	handler := &rudpEchoHandler{}
	server, addr, done := startRUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	clientA, connA := newRUDPClientConn(t, addr)
	defer func() { _ = connA.Close() }()
	clientB, connB := newRUDPClientConn(t, addr)
	defer func() { _ = connB.Close() }()

	errCh := make(chan error, 4)
	go func() { errCh <- clientA.SendPacket(context.Background(), connA, []byte("SA1")) }()
	go func() { errCh <- clientA.SendPacket(context.Background(), connA, []byte("SA2")) }()
	go func() { errCh <- clientB.SendPacket(context.Background(), connB, []byte("SB1")) }()
	go func() { errCh <- clientB.SendPacket(context.Background(), connB, []byte("SB2")) }()

	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent send failed: %v", err)
		}
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if _, err := clientA.ReceivePacket(ctx, connA); err != nil {
			cancel()
			t.Fatalf("client A receive failed: %v", err)
		}
		cancel()

		ctxB, cancelB := context.WithTimeout(context.Background(), time.Second)
		if _, err := clientB.ReceivePacket(ctxB, connB); err != nil {
			cancelB()
			t.Fatalf("client B receive failed: %v", err)
		}
		cancelB()
	}
}
