package aegis

import (
	"bytes"
	"errors"
	"testing"
)

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

	// Test Stream Decoding (Valid State)
	reader := bytes.NewReader(serialized)
	decodedPacket, err := ReadPacket(reader)
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
	_, err := ReadPacket(reader)

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
	_, err := ReadPacket(reader)

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
	// which exceeds the 65,535 byte safety threshold.
	maliciousHeader := []byte{0x41, 0x47, 0x00, 0x01, 0x86, 0x9F, 0x00, 0x00, 0x00, 0x00}

	reader := bytes.NewReader(maliciousHeader)
	_, err := ReadPacket(reader)

	if err == nil {
		t.Fatal("Expected OOM prevention limit error, got nil")
	}
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Errorf("Expected ErrPacketTooLarge, got: %v", err)
	}
}
