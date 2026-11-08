package aegis

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type chunkedReader struct {
	data      []byte
	chunkSize int
	offset    int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	size := r.chunkSize
	if size <= 0 {
		size = 1
	}

	remaining := len(r.data) - r.offset
	if remaining < size {
		size = remaining
	}

	if len(p) < size {
		size = len(p)
	}

	n := copy(p, r.data[r.offset:r.offset+size])
	r.offset += n
	return n, nil
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "read timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

type timeoutReader struct{}

func (timeoutReader) Read(_ []byte) (int, error) {
	return 0, timeoutErr{}
}

// TestPacketSerializationAndDecoding verifies that a packet can be correctly
// serialized to binary format and decoded back without data loss.
func TestPacketSerializationAndDecoding(t *testing.T) {
	originalPayload := []byte("DRONE_STATUS:ACTIVE_BATTERY:88%")
	packet := NewPacket(originalPayload)

	// Test Serialization
	serialized, err := packet.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	// Test Stream Decoding (Valid State with default payload limit fallback)
	reader := bytes.NewReader(serialized)
	decodedPacket, err := ReadPacket(reader, DefaultMaxPayloadSize)
	if err != nil {
		t.Fatalf("Failed to decode valid packet stream: %v", err)
	}

	if string(decodedPacket.Payload) != string(originalPayload) {
		t.Errorf("Payload mismatch. Got %s, want %s", string(decodedPacket.Payload), string(originalPayload))
	}
}

// TestReadPacketCorruptedIntegrity verifies that CRC32 checksum validation
// catches payload bit-rot and corrupted data in transit.
func TestReadPacketCorruptedIntegrity(t *testing.T) {
	originalPayload := []byte("SECURE_DATA")
	packet := NewPacket(originalPayload)
	serialized, _ := packet.Serialize()

	// Corrupt the last byte to break the checksum
	serialized[len(serialized)-1] ^= 0xFF

	reader := bytes.NewReader(serialized)
	_, err := ReadPacket(reader, DefaultMaxPayloadSize)

	if err == nil {
		t.Fatal("Expected error due to packet corruption, but got nil")
	}

	if !errors.Is(err, ErrInvalidPacket) {
		t.Errorf("Expected wrapped ErrInvalidPacket, got: %v", err)
	}
}

// TestReadPacketInvalidMagicBytes verifies that incoming byte streams
// with unknown magic bytes are rejected at the security gate.
func TestReadPacketInvalidMagicBytes(t *testing.T) {
	originalPayload := []byte("HELLO")
	packet := NewPacket(originalPayload)
	serialized, _ := packet.Serialize()

	// Alter magic byte header
	serialized[0] = 0x00

	reader := bytes.NewReader(serialized)
	_, err := ReadPacket(reader, DefaultMaxPayloadSize)

	if err == nil {
		t.Fatal("Expected magic byte validation error, got nil")
	}
	if !errors.Is(err, ErrInvalidPacket) {
		t.Errorf("Expected ErrInvalidPacket, got: %v", err)
	}
}

// TestReadPacketPayloadTooLargeSafetyLimit verifies that the engine blocks
// oversized payload headers to prevent Memory Exhaustion (OOM/DDoS) attacks.
func TestReadPacketPayloadTooLargeSafetyLimit(t *testing.T) {
	// Header declaring a payload of 100,000 bytes (0x00, 0x01, 0x86, 0x9F)
	maliciousHeader := []byte{0x41, 0x47, 0x00, 0x01, 0x86, 0x9F, 0x00, 0x00, 0x00, 0x00}

	// Set a strict custom limit of 2,000 bytes for testing enforceability
	var customMaxLimit uint32 = 2000

	reader := bytes.NewReader(maliciousHeader)
	_, err := ReadPacket(reader, customMaxLimit)

	if err == nil {
		t.Fatal("Expected OOM prevention limit error, got nil")
	}
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Errorf("Expected ErrPacketTooLarge, got: %v", err)
	}
}

// TestReadPacketFragmentedStream verifies that TCP stream fragmentation
// is handled correctly and complete frames are reconstructed.
func TestReadPacketFragmentedStream(t *testing.T) {
	payload := []byte("FRAGMENTED_STREAM_PAYLOAD")
	packet := NewPacket(payload)

	serialized, err := packet.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet: %v", err)
	}

	reader := &chunkedReader{
		data:      serialized,
		chunkSize: 2,
	}

	decoded, err := ReadPacket(reader, DefaultMaxPayloadSize)
	if err != nil {
		t.Fatalf("expected successful packet decode, got: %v", err)
	}

	if string(decoded.Payload) != string(payload) {
		t.Fatalf("payload mismatch. got %q want %q", string(decoded.Payload), string(payload))
	}
}

// TestReadPacketIncompleteHeaderConnectionClosed verifies that incomplete
// headers are treated as connection closure events.
func TestReadPacketIncompleteHeaderConnectionClosed(t *testing.T) {
	reader := bytes.NewReader([]byte{0x41, 0x47, 0x00})

	_, err := ReadPacket(reader, DefaultMaxPayloadSize)
	if err == nil {
		t.Fatal("expected connection closed error for incomplete header, got nil")
	}

	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got: %v", err)
	}
}

// TestReadPacketIncompletePayloadConnectionClosed verifies that payload
// truncation on stream close is treated as a closed connection.
func TestReadPacketIncompletePayloadConnectionClosed(t *testing.T) {
	payload := []byte("INCOMPLETE_PAYLOAD")
	packet := NewPacket(payload)

	serialized, err := packet.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize packet: %v", err)
	}

	truncated := serialized[:len(serialized)-2]
	reader := bytes.NewReader(truncated)

	_, err = ReadPacket(reader, DefaultMaxPayloadSize)
	if err == nil {
		t.Fatal("expected connection closed error for truncated payload, got nil")
	}

	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got: %v", err)
	}
}

// TestReadPacketTimeoutPropagation verifies that timeout read errors remain
// detectable as timeout conditions.
func TestReadPacketTimeoutPropagation(t *testing.T) {
	_, err := ReadPacket(timeoutReader{}, DefaultMaxPayloadSize)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !isTimeout(err) {
		t.Fatalf("expected timeout error to be preserved, got: %v", err)
	}
}
