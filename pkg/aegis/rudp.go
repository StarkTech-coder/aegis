package aegis

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	rudpHeaderSize     = 8
	rudpTypeData       = 1
	rudpTypeAck        = 2
	rudpMaxRetries     = 3
	maxUDPDatagramSize = 65507
	maxUDPPayloadSize  = maxUDPDatagramSize - HeaderSize
	maxRUDPPayloadSize = maxUDPDatagramSize - rudpHeaderSize - HeaderSize
)

var rudpMagic = [2]byte{'A', 'R'}

type rudpFrame struct {
	typ     byte
	seq     uint32
	payload []byte
}

func encodeRUDPData(
	seq uint32,
	payload []byte,
) []byte {
	frame := make(
		[]byte,
		rudpHeaderSize+len(payload),
	)

	copy(
		frame[:2],
		rudpMagic[:],
	)

	frame[2] = rudpTypeData

	binary.BigEndian.PutUint32(
		frame[4:8],
		seq,
	)

	copy(
		frame[rudpHeaderSize:],
		payload,
	)

	return frame
}

func encodeRUDPAck(seq uint32) []byte {
	frame := make(
		[]byte,
		rudpHeaderSize,
	)

	copy(
		frame[:2],
		rudpMagic[:],
	)

	frame[2] = rudpTypeAck

	binary.BigEndian.PutUint32(
		frame[4:8],
		seq,
	)

	return frame
}

func decodeRUDPFrame(
	data []byte,
) (rudpFrame, error) {
	if len(data) < rudpHeaderSize {
		return rudpFrame{}, fmt.Errorf(
			"%w: RUDP header is too short",
			ErrInvalidPacket,
		)
	}

	if data[0] != rudpMagic[0] ||
		data[1] != rudpMagic[1] {
		return rudpFrame{}, fmt.Errorf(
			"%w: invalid RUDP magic bytes",
			ErrInvalidPacket,
		)
	}

	frame := rudpFrame{
		typ: data[2],
		seq: binary.BigEndian.Uint32(
			data[4:8],
		),
	}

	switch frame.typ {
	case rudpTypeAck:
		if len(data) != rudpHeaderSize {
			return rudpFrame{}, fmt.Errorf(
				"%w: invalid RUDP ACK size",
				ErrInvalidPacket,
			)
		}

	case rudpTypeData:
		frame.payload = data[rudpHeaderSize:]

		if len(frame.payload) < HeaderSize {
			return rudpFrame{}, fmt.Errorf(
				"%w: RUDP payload is too short",
				ErrInvalidPacket,
			)
		}

	default:
		return rudpFrame{}, fmt.Errorf(
			"%w: unsupported RUDP frame type %d",
			ErrInvalidPacket,
			frame.typ,
		)
	}

	return frame, nil
}

func readRUDPAck(
	ctx context.Context,
	conn net.Conn,
	timeout time.Duration,
) (uint32, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	deadline := time.Now().Add(timeout)

	if ctxDeadline, ok := ctx.Deadline(); ok &&
		ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	if err := conn.SetReadDeadline(deadline); err != nil {
		return 0, err
	}

	buffer := make(
		[]byte,
		rudpHeaderSize,
	)

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		n, err := conn.Read(buffer)
		if err != nil {
			return 0, err
		}

		frame, err := decodeRUDPFrame(
			buffer[:n],
		)
		if err != nil {
			continue
		}

		if frame.typ == rudpTypeAck {
			return frame.seq, nil
		}
	}
}

type rudpPeerState struct {
	lastSeq  uint32
	lastSeen time.Time
}

const (
	rudpSequenceAccept = iota
	rudpSequenceDuplicate
	rudpSequenceFuture
	rudpSequenceRejected
)

// RUDPTransport provides reliable ordered datagram transport over UDP.
type RUDPTransport struct {
	server *Server
	conn   *net.UDPConn

	mu    sync.Mutex
	peers map[string]rudpPeerState
}

// NewRUDPTransport creates a RUDP transport for the server.
func NewRUDPTransport(s *Server) *RUDPTransport {
	return &RUDPTransport{
		server: s,
		peers:  make(map[string]rudpPeerState),
	}
}

// Start binds the RUDP socket and processes incoming frames.
func (r *RUDPTransport) Start(ctx context.Context) error {
	if r == nil || r.server == nil {
		return fmt.Errorf(
			"aegis-rudp: server is nil",
		)
	}

	if ctx.Err() != nil {
		return ErrServerClosed
	}

	addr, err := net.ResolveUDPAddr(
		"udp",
		r.server.config.Address,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-rudp: failed to resolve address %s: %w",
			r.server.config.Address,
			err,
		)
	}

	conn, err := net.ListenUDP(
		"udp",
		addr,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-rudp: failed to bind socket: %w",
			err,
		)
	}

	r.conn = conn

	defer func() {
		_ = conn.Close()
	}()

	r.server.logger.Info(
		"Aegis RUDP Engine successfully running",
		"address",
		r.server.config.Address,
	)

	buffer := make(
		[]byte,
		maxUDPDatagramSize,
	)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-r.server.quit:
			return nil

		default:
		}

		if r.server.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(
				time.Now().Add(
					r.server.config.ReadTimeout,
				),
			)
		}

		n, remoteAddr, err := conn.ReadFromUDP(buffer)

		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}

			if isTimeout(err) {
				continue
			}

			return fmt.Errorf(
				"aegis-rudp: failed to read datagram: %w",
				err,
			)
		}

		frame, err := decodeRUDPFrame(
			buffer[:n],
		)

		if err != nil {
			r.server.logger.Warn(
				"Failed to decode RUDP frame",
				"remote_addr",
				remoteAddr.String(),
				"error",
				err,
			)

			continue
		}

		if frame.typ != rudpTypeData {
			continue
		}

		sequenceStatus := r.sequenceStatus(
			remoteAddr.String(),
			frame.seq,
		)

		switch sequenceStatus {
		case rudpSequenceDuplicate:
			_, _ = conn.WriteToUDP(
				encodeRUDPAck(frame.seq),
				remoteAddr,
			)

			continue

		case rudpSequenceFuture,
			rudpSequenceRejected:
			continue
		}

		packet, err := Deserialize(
			frame.payload,
			r.server.config.MaxPayloadSize,
		)

		if err != nil {
			r.server.logger.Warn(
				"Failed to deserialize RUDP packet",
				"remote_addr",
				remoteAddr.String(),
				"error",
				err,
			)

			continue
		}

		if r.server.handler != nil {
			if err := r.server.handler.HandlePacket(
				packet.Payload,
			); err != nil {
				r.server.logger.Error(
					"RUDP packet handler failed",
					"remote_addr",
					remoteAddr.String(),
					"error",
					err,
				)
			}
		}

		r.markSequence(
			remoteAddr.String(),
			frame.seq,
		)

		if _, err := conn.WriteToUDP(
			encodeRUDPAck(frame.seq),
			remoteAddr,
		); err != nil {
			r.server.logger.Warn(
				"Failed to send RUDP acknowledgement",
				"remote_addr",
				remoteAddr.String(),
				"sequence",
				frame.seq,
				"error",
				err,
			)
		}

		r.server.logger.Debug(
			"Valid Aegis RUDP packet processed",
			"remote_addr",
			remoteAddr.String(),
			"sequence",
			frame.seq,
			"payload_len",
			packet.Length,
			"checksum",
			packet.Checksum,
		)
	}
}

func (r *RUDPTransport) sequenceStatus(
	remoteAddr string,
	seq uint32,
) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.peers[remoteAddr]

	if !exists {
		if len(r.peers) >= r.server.config.MaxConnections {
			r.server.logger.Warn(
				"RUDP peer limit reached",
				"remote_addr",
				remoteAddr,
			)

			return rudpSequenceRejected
		}

		return rudpSequenceAccept
	}

	if r.server.config.ReadTimeout > 0 &&
		time.Since(state.lastSeen) >
			r.server.config.ReadTimeout*2 {
		return rudpSequenceAccept
	}

	if seq == state.lastSeq+1 {
		return rudpSequenceAccept
	}

	if int32(seq-state.lastSeq) <= 0 {
		return rudpSequenceDuplicate
	}

	return rudpSequenceFuture
}

func (r *RUDPTransport) markSequence(
	remoteAddr string,
	seq uint32,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.peers[remoteAddr] = rudpPeerState{
		lastSeq:  seq,
		lastSeen: time.Now(),
	}
}

// Close closes the RUDP socket.
func (r *RUDPTransport) Close() error {
	if r == nil || r.conn == nil {
		return nil
	}

	return r.conn.Close()
}

func isClosedNetworkError(err error) bool {
	return err == net.ErrClosed
}