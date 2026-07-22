package aegis

import "errors"

var (

	// ErrConnectionClosed is returned when the remote node closes the connection.
	ErrConnectionClosed = errors.New("aegis: connection closed by remote node")

	// ErrTimeout is returned when a network operation exceeds its allocated duration.
	ErrTimeout = errors.New("aegis: network operation timed out")

	// ErrInvalidPacket is returned when the incoming data does not match the binary protocol schema.
	ErrInvalidPacket = errors.New("aegis: invalid or corrupted packet format")

	// ErrBufferFull is returned when the internal ring buffer or channels are full, indicating heavy load.
	ErrBufferFull = errors.New("aegis: internal ring buffer is full")

	// ErrPacketTooLarge protects the system against DDoS and Out-Of-Memory (OOM) crashes.
	ErrPacketTooLarge = errors.New("aegis: packet size exceeds max allowed limit")

	// ErrServerClosed is returned when an operation is attempted while the server is shutting down.
	ErrServerClosed = errors.New("aegis: server is shutting down")

	// ErrMaxClientsReached limits concurrent connections to protect server resources from exhaustion.
	ErrMaxClientsReached = errors.New("aegis: max client connections reached")
)
