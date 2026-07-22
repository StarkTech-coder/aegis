package tests

import (
	"bytes"
	"errors"
	"testing"

	// go.mod dosmandaki modül adı neyse onunla başla (örn: "aegis" veya "github.com/tony/aegis")
	"aegis/pkg/aegis" 
)

func TestPacketSerializationAndDecoding(t *testing.T) {
	originalPayload := []byte("DRONE_STATUS:ACTIVE_BATTERY:88%")
	packet := aegis.NewPacket(originalPayload) // aegis. prefix'i eklendi

	// Test Serialization
	serialized, err := packet.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize packet: %v", err)
	}

	// Test Stream Decoding (Valid State)
	reader := bytes.NewReader(serialized)
	decodedPacket, err := aegis.ReadPacket(reader) // aegis. prefix'i eklendi
	if err != nil {
		t.Fatalf("Failed to decode valid packet stream: %v", err)
	}

	if string(decodedPacket.Payload) != string(originalPayload) {
		t.Errorf("Payload mismatch. Got %s, want %s", string(decodedPacket.Payload), string(originalPayload))
	}
}

func TestReadPacketCorruptedIntegrity(t *testing.T) {
	originalPayload := []byte("SECURE_DATA")
	packet := aegis.NewPacket(originalPayload)
	serialized, _ := packet.Serialize()

	serialized[len(serialized)-1] ^= 0xFF

	reader := bytes.NewReader(serialized)
	_, err := aegis.ReadPacket(reader)

	if err == nil {
		t.Fatal("Expected error due to packet corruption, but got nil")
	}

	if !errors.Is(err, aegis.ErrInvalidPacket) { // aegis. prefix'i eklendi
		t.Errorf("Expected wrapped ErrInvalidPacket, got: %v", err)
	}
}

func TestReadPacketInvalidMagicBytes(t *testing.T) {
	originalPayload := []byte("HELLO")
	packet := aegis.NewPacket(originalPayload)
	serialized, _ := packet.Serialize()

	serialized[0] = 0x00

	reader := bytes.NewReader(serialized)
	_, err := aegis.ReadPacket(reader)

	if err == nil {
		t.Fatal("Expected magic byte validation error, got nil")
	}
	if !errors.Is(err, aegis.ErrInvalidPacket) {
		t.Errorf("Expected ErrInvalidPacket, got: %v", err)
	}
}

func TestReadPacketPayloadTooLargeSafetyLimit(t *testing.T) {
	maliciousHeader := []byte{0x41, 0x47, 0x00, 0x01, 0x86, 0x9F, 0x00, 0x00, 0x00, 0x00}
	
	reader := bytes.NewReader(maliciousHeader)
	_, err := aegis.ReadPacket(reader)

	if err == nil {
		t.Fatal("Expected OOM prevention limit error, got nil")
	}
	if !errors.Is(err, aegis.ErrPacketTooLarge) {
		t.Errorf("Expected ErrPacketTooLarge, got: %v", err)
	}
}