package aegis

import "errors"

var (
	// ErrConnectionClosed indicates that a remote node closed the connection.
	ErrConnectionClosed = errors.New(
		"aegis: connection closed by remote node",
	)

	// ErrTimeout indicates that a network operation exceeded its deadline.
	ErrTimeout = errors.New(
		"aegis: network operation timed out", 
	)

	// ErrInvalidPacket indicates that packet data failed protocol validation.
	ErrInvalidPacket = errors.New(
		"aegis: invalid or corrupted packet format",
	)

	// ErrBufferFull indicates that an internal capacity limit was reached.
	ErrBufferFull = errors.New(
		"aegis: internal ring buffer is full",
	)

	// ErrPacketTooLarge indicates that packet data exceeded the configured limit.
	ErrPacketTooLarge = errors.New(
		"aegis: packet size exceeds max allowed limit",  
	)

	// ErrServerClosed indicates that the server is shutting down.
	ErrServerClosed = errors.New(
		"aegis: server is shutting down",
	)

	// ErrMaxClientsReached indicates that the connection limit has been reached.
	ErrMaxClientsReached = errors.New(
		"aegis: max client connections reached",
	)
)