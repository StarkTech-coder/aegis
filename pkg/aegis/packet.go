package aegis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Magic Bytes to identify the Aegis Protocol ("AG")
var MagicBytes = [2]byte{0x41, 0x47}

// HeaderSize = MagicBytes(2) + Length(4) + Checksum(4) = 10 Bytes
const HeaderSize = 10

// Packet represents the custom structured binary framing protocol for Aegis.
type Packet struct {
	Magic    [2]byte
	Length   uint32
	Checksum uint32
	Payload  []byte
}

// NewPacket constructs a valid Packet and calculates its CRC32 checksum automatically.
func NewPacket(payload []byte) *Packet {
	return &Packet{
		Magic:    MagicBytes,
		Length:   uint32(len(payload)),
		Checksum: crc32.ChecksumIEEE(payload),
		Payload:  payload,
	}
}

// Serialize converts the Packet struct into a raw byte slice ready to be transmitted over the wire.
func (p *Packet) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, p.Magic); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to serialize magic bytes: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, p.Length); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to serialize length: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, p.Checksum); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to serialize checksum: %w", err)
	}
	if _, err := buf.Write(p.Payload); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to serialize payload: %w", err)
	}

	return buf.Bytes(), nil
}

// Deserialize parses a raw byte slice into a valid structural Packet safely.
func Deserialize(data []byte, maxPayloadLimit uint32) (*Packet, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("%w: packet header is too short, got %d bytes", ErrInvalidPacket, len(data))
	}

	if maxPayloadLimit == 0 {
		maxPayloadLimit = DefaultMaxPayloadSize
	}

	buf := bytes.NewReader(data)
	p := &Packet{}

	if err := binary.Read(buf, binary.BigEndian, &p.Magic); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to decode magic bytes: %w", err)
	}
	if p.Magic != MagicBytes {
		return nil, fmt.Errorf("%w: invalid protocol magic bytes detected", ErrInvalidPacket)
	}

	if err := binary.Read(buf, binary.BigEndian, &p.Length); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to decode length: %w", err)
	}

	if p.Length > maxPayloadLimit {
		return nil, fmt.Errorf("%w: packet length %d exceeds configured limit %d", ErrPacketTooLarge, p.Length, maxPayloadLimit)
	}

	// Exact datagram size boundary validation for UDP/RUDP frame isolation
	expectedSize := HeaderSize + int(p.Length)
	if len(data) != expectedSize {
		return nil, fmt.Errorf("%w: exact datagram size mismatch, expected %d bytes, got %d bytes", ErrInvalidPacket, expectedSize, len(data))
	}

	if err := binary.Read(buf, binary.BigEndian, &p.Checksum); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to decode checksum: %w", err)
	}

	p.Payload = make([]byte, p.Length)
	if _, err := io.ReadFull(buf, p.Payload); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to read complete payload: %w", err)
	}

	if crc32.ChecksumIEEE(p.Payload) != p.Checksum {
		return nil, fmt.Errorf("%w: critical checksum mismatch, corrupted data packet discarded", ErrInvalidPacket)
	}

	return p, nil
}

// ReadPacket isolates individual packets from a continuous TCP stream (Frame Decoder).
// It solves TCP fragmentation and packet coalescing issues fundamentally by using length-based parsing.
func ReadPacket(r io.Reader, maxPayloadLimit uint32) (*Packet, error) {
	if maxPayloadLimit == 0 {
		maxPayloadLimit = DefaultMaxPayloadSize
	}

	headerBuf := make([]byte, HeaderSize)

	// io.ReadFull blocks until EXACTLY 10 bytes (Header) are read, preventing partial reads
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	p := &Packet{}
	p.Magic = [2]byte{headerBuf[0], headerBuf[1]}
	if p.Magic != MagicBytes {
		return nil, fmt.Errorf("%w: invalid protocol magic bytes detected", ErrInvalidPacket)
	}

	p.Length = binary.BigEndian.Uint32(headerBuf[2:6])
	p.Checksum = binary.BigEndian.Uint32(headerBuf[6:10])

	if p.Length > maxPayloadLimit {
		return nil, fmt.Errorf("%w: payload size %d exceeds safety limit %d", ErrPacketTooLarge, p.Length, maxPayloadLimit)
	}

	// Read exactly the payload length specified in the header
	p.Payload = make([]byte, p.Length)
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, fmt.Errorf("aegis-packet: failed to read complete payload stream: %w", err)
	}

	// Integrity Verification
	if crc32.ChecksumIEEE(p.Payload) != p.Checksum {
		return nil, fmt.Errorf("%w: critical checksum mismatch, data corrupted", ErrInvalidPacket)
	}

	return p, nil
}