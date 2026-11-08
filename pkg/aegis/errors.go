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

	// ErrRUDPInvalidPacket indicates that an RUDP frame failed validation.
	ErrRUDPInvalidPacket = errors.New(
		"aegis-rudp: invalid packet",
	)

	// ErrRUDPInvalidSequence indicates an invalid or out-of-window sequence value.
	ErrRUDPInvalidSequence = errors.New(
		"aegis-rudp: invalid sequence",
	)

	// ErrRUDPInvalidAcknowledgement indicates an ACK that does not match pending state.
	ErrRUDPInvalidAcknowledgement = errors.New(
		"aegis-rudp: invalid acknowledgement",
	)

	// ErrRUDPRetryExhausted indicates reliable delivery retry budget was exhausted.
	ErrRUDPRetryExhausted = errors.New(
		"aegis-rudp: retransmission retries exhausted",
	)

	// ErrRUDPInFlightCapacity indicates the in-flight pending-ACK capacity was reached.
	ErrRUDPInFlightCapacity = errors.New(
		"aegis-rudp: in-flight capacity exhausted",
	)

	// ErrRUDPSessionClosed indicates the RUDP session/connection is closed.
	ErrRUDPSessionClosed = errors.New(
		"aegis-rudp: session closed",
	)
)
