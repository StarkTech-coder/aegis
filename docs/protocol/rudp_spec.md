# AEGIS RUDP Protocol Specification

## RUDP envelope

RUDP uses a fixed 16-byte header and then payload bytes.

- Bytes 0..1: magic (`AR`)
- Byte 2: version (`1`)
- Byte 3: type (`1=DATA`, `2=ACK`)
- Byte 4: flags (currently `0` only)
- Byte 5: reserved (`0`)
- Bytes 6..9: sequence number (`uint32`, big-endian)
- Bytes 10..13: acknowledgement number (`uint32`, big-endian)
- Bytes 14..15: payload length (`uint16`, big-endian)

DATA payload contains a serialized AEGIS packet (`pkg/aegis/packet.go`).

## Validation rules

- Reject short headers and invalid magic/version/type.
- Reject unsupported flags.
- Reject payload-length/datagram-size mismatch.
- Reject oversized payloads.
- DATA frames must carry `ack=0`.
- ACK frames must carry `payload_len=0` and `ack!=0`.

## Reliability behavior

- DATA packets use monotonically increasing `uint32` sequence numbers per session.
- Receiver ACKs valid DATA packets.
- Duplicate/stale DATA packets are ACKed but not delivered again.
- Out-of-order future sequence numbers are rejected.
- Sender retransmits with exponential backoff until ACK or retry exhaustion.

Backoff config:

- `RUDPInitialRetransmitTimeout`
- `RUDPMaxRetransmitTimeout`
- `RUDPMaxRetries`

Resource config:

- `RUDPMaxInFlight`
- `RUDPReceiveQueueSize`

## Delivery and ordering semantics

Current semantics are:

- ACK-based retransmission (at-least-once transmission attempts).
- Duplicate suppression at the receive-delivery boundary.
- In-order progression requirement for delivery acceptance.

## Shutdown and cancellation

Send operations terminate on:

- matching ACK
- context cancellation/deadline
- session/connection closure
- server shutdown
- retry exhaustion (`ErrRUDPRetryExhausted`)

## Limitations

- This is not TCP-equivalent reliability.
- No congestion-control algorithm is implemented.
- No cryptographic authenticity is provided by RUDP itself.
- CRC32 inside AEGIS packet checks accidental corruption only.
