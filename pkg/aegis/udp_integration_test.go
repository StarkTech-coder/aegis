package aegis

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type udpTestHandler struct {
	mu sync.Mutex

	received [][]byte

	dropResponseFor map[string]struct{}
}

func (h *udpTestHandler) HandlePacket(_ []byte) error {
	return nil
}

func (h *udpTestHandler) HandleUDPPacket(payload []byte, _ *net.UDPAddr) ([]byte, error) {
	h.mu.Lock()
	h.received = append(h.received, append([]byte(nil), payload...))
	h.mu.Unlock()

	if h.dropResponseFor != nil {
		if _, ok := h.dropResponseFor[string(payload)]; ok {
			return nil, nil
		}
	}

	return append([]byte("ACK:"), payload...), nil
}

func reserveUDPAddress(t *testing.T) string {
	t.Helper()

	l, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to reserve UDP address: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	return l.LocalAddr().String()
}

func startUDPServer(t *testing.T, handler PacketHandler) (*Server, string, <-chan error) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.Address = reserveUDPAddress(t)
	cfg.MaxPayloadSize = maxUDPPayloadSize

	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create UDP server: %v", err)
	}

	server.SetHandler(handler)

	done := make(chan error, 1)
	go func() {
		done <- server.Start()
	}()

	// Give the listener a brief window to bind deterministically.
	time.Sleep(50 * time.Millisecond)

	return server, cfg.Address, done
}

func newUDPClientConn(t *testing.T, address string) (*Client, net.Conn) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Protocol = ProtocolUDP
	cfg.Address = address
	cfg.MaxPayloadSize = maxUDPPayloadSize

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create UDP client: %v", err)
	}

	conn, err := client.Connect(address)
	if err != nil {
		t.Fatalf("failed to connect UDP client: %v", err)
	}

	return client, conn
}

func TestUDPClientServerRequestResponse(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newUDPClientConn(t, address)
	defer func() {
		_ = conn.Close()
	}()

	messages := [][]byte{
		[]byte("MSG_1"),
		[]byte("MSG_2"),
		[]byte("MSG_3"),
	}

	for _, msg := range messages {
		if err := client.SendPacket(context.Background(), conn, msg); err != nil {
			t.Fatalf("send failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}

		expected := append([]byte("ACK:"), msg...)
		if !bytes.Equal(resp, expected) {
			t.Fatalf("response mismatch. got %q want %q", string(resp), string(expected))
		}
	}
}

func TestUDPServerMultipleClients(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	clientA, connA := newUDPClientConn(t, address)
	defer func() { _ = connA.Close() }()

	clientB, connB := newUDPClientConn(t, address)
	defer func() { _ = connB.Close() }()

	if err := clientA.SendPacket(context.Background(), connA, []byte("CLIENT_A")); err != nil {
		t.Fatalf("client A send failed: %v", err)
	}
	if err := clientB.SendPacket(context.Background(), connB, []byte("CLIENT_B")); err != nil {
		t.Fatalf("client B send failed: %v", err)
	}

	ctxA, cancelA := context.WithTimeout(context.Background(), 500*time.Millisecond)
	respA, err := clientA.ReceivePacket(ctxA, connA)
	cancelA()
	if err != nil {
		t.Fatalf("client A receive failed: %v", err)
	}

	ctxB, cancelB := context.WithTimeout(context.Background(), 500*time.Millisecond)
	respB, err := clientB.ReceivePacket(ctxB, connB)
	cancelB()
	if err != nil {
		t.Fatalf("client B receive failed: %v", err)
	}

	if string(respA) != "ACK:CLIENT_A" {
		t.Fatalf("unexpected response for client A: %q", string(respA))
	}

	if string(respB) != "ACK:CLIENT_B" {
		t.Fatalf("unexpected response for client B: %q", string(respB))
	}
}

func TestUDPServerRecoversAfterMalformedDatagram(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	rawConn, err := net.Dial("udp", address)
	if err != nil {
		t.Fatalf("failed to create raw UDP dial: %v", err)
	}
	_, _ = rawConn.Write([]byte{0x00, 0x01, 0x02})
	_ = rawConn.Close()

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("VALID_AFTER_MALFORMED")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.ReceivePacket(ctx, conn)
	if err != nil {
		t.Fatalf("receive failed after malformed datagram: %v", err)
	}

	if string(resp) != "ACK:VALID_AFTER_MALFORMED" {
		t.Fatalf("unexpected response after malformed recovery: %q", string(resp))
	}
}

func TestUDPServerRecoversAfterOversizedLengthDatagram(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	header := make([]byte, HeaderSize)
	copy(header[:2], MagicBytes[:])
	binary.BigEndian.PutUint32(header[2:6], maxUDPPayloadSize+1)
	binary.BigEndian.PutUint32(header[6:10], 0)

	rawConn, err := net.Dial("udp", address)
	if err != nil {
		t.Fatalf("failed to create raw UDP dial: %v", err)
	}
	_, _ = rawConn.Write(header)
	_ = rawConn.Close()

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("VALID_AFTER_OVERSIZED")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.ReceivePacket(ctx, conn)
	if err != nil {
		t.Fatalf("receive failed after oversized datagram: %v", err)
	}

	if string(resp) != "ACK:VALID_AFTER_OVERSIZED" {
		t.Fatalf("unexpected response after oversized recovery: %q", string(resp))
	}
}

func TestUDPControlledDroppedResponse(t *testing.T) {
	handler := &udpTestHandler{
		dropResponseFor: map[string]struct{}{"DROP": {}},
	}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("DROP")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := client.ReceivePacket(ctx, conn)
	if err == nil {
		t.Fatal("expected receive timeout/cancellation due to dropped response")
	}

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected deadline/timeout error, got: %v", err)
	}
}

func TestUDPServerShutdownDuringActiveCommunication(t *testing.T) {
	handler := &udpTestHandler{
		dropResponseFor: map[string]struct{}{"NO_REPLY": {}},
	}
	server, address, done := startUDPServer(t, handler)

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("NO_REPLY")); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("server stop failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server start loop returned error: %v", err)
		}

	case <-time.After(1 * time.Second):
		t.Fatal("server start loop did not exit after shutdown")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := client.ReceivePacket(ctx, conn)
	if err == nil {
		t.Fatal("expected receive error after server shutdown")
	}
}

func TestUDPConcurrentSends(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	errCh := make(chan error, 2)
	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("C_SEND_1"))
	}()
	go func() {
		errCh <- client.SendPacket(context.Background(), conn, []byte("C_SEND_2"))
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent send failed: %v", err)
		}
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := client.ReceivePacket(ctx1, conn)
	cancel1()
	if err != nil {
		t.Fatalf("first concurrent-send response receive failed: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = client.ReceivePacket(ctx2, conn)
	cancel2()
	if err != nil {
		t.Fatalf("second concurrent-send response receive failed: %v", err)
	}
}

func TestUDPConcurrentReceives(t *testing.T) {
	handler := &udpTestHandler{}
	server, address, done := startUDPServer(t, handler)
	defer func() {
		_ = server.Stop()
		<-done
	}()

	client, conn := newUDPClientConn(t, address)
	defer func() { _ = conn.Close() }()

	if err := client.SendPacket(context.Background(), conn, []byte("R_ONE")); err != nil {
		t.Fatalf("send one failed: %v", err)
	}
	if err := client.SendPacket(context.Background(), conn, []byte("R_TWO")); err != nil {
		t.Fatalf("send two failed: %v", err)
	}

	type recvResult struct {
		payload []byte
		err     error
	}

	resultCh := make(chan recvResult, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		payload, err := client.ReceivePacket(ctx, conn)
		resultCh <- recvResult{payload: payload, err: err}
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		payload, err := client.ReceivePacket(ctx, conn)
		resultCh <- recvResult{payload: payload, err: err}
	}()

	res1 := <-resultCh
	res2 := <-resultCh

	if res1.err != nil {
		t.Fatalf("first concurrent receive failed: %v", res1.err)
	}
	if res2.err != nil {
		t.Fatalf("second concurrent receive failed: %v", res2.err)
	}

	got := map[string]struct{}{
		string(res1.payload): {},
		string(res2.payload): {},
	}

	if _, ok := got["ACK:R_ONE"]; !ok {
		t.Fatalf("missing response ACK:R_ONE; got %v", got)
	}
	if _, ok := got["ACK:R_TWO"]; !ok {
		t.Fatalf("missing response ACK:R_TWO; got %v", got)
	}
}
