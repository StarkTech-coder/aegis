package aegis

import "errors"

var (
	// ErrConnectionClosed is returned when the remote node closes the connection.
	ErrConnectionClosed = errors.New("aegis: connection closed by remote node")

	// ErrInvalidPacket is returned when the incoming data does not match the binary protocol schema.
	ErrInvalidPacket    = errors.New("aegis: invalid or corrupted packet format")

	// ErrTimeout is returned when a network operation exceeds its allocated duration.
	ErrTimeout          = errors.New("aegis: network operation timed out")
)