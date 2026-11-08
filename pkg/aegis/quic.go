package aegis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	quic "github.com/quic-go/quic-go"
)

const (
	quicCloseReason          = "aegis shutdown"
	quicAppErrCodeGraceful   = quic.ApplicationErrorCode(0)
	quicAppErrCodeMaxConn    = quic.ApplicationErrorCode(0x101)
	quicResponseQueueErrCode = quic.StreamErrorCode(0x100)
)

type managedQUICConn struct {
	owner *Client
	conn  *quic.Conn

	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *managedQUICConn) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("%w: direct Read is unsupported for QUIC, use ReceivePacket", ErrQUICStreamClosed)
}

func (c *managedQUICConn) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("%w: direct Write is unsupported for QUIC, use SendPacket", ErrQUICStreamClosed)
}

func (c *managedQUICConn) Close() error {
	if c.owner != nil {
		c.owner.deleteQUICSession(c)
	}

	if c.conn == nil {
		return nil
	}

	return c.conn.CloseWithError(quicAppErrCodeGraceful, quicCloseReason)
}

func (c *managedQUICConn) LocalAddr() net.Addr {
	if c.conn == nil {
		return nil
	}

	return c.conn.LocalAddr()
}

func (c *managedQUICConn) RemoteAddr() net.Addr {
	if c.conn == nil {
		return nil
	}

	return c.conn.RemoteAddr()
}

func (c *managedQUICConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}

	return c.SetWriteDeadline(t)
}

func (c *managedQUICConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *managedQUICConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *managedQUICConn) readDeadlineValue() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadline
}

func (c *managedQUICConn) writeDeadlineValue() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadline
}

type quicConnState struct {
	conn *managedQUICConn

	responseQueue chan *quic.Stream
	closed        chan struct{}

	closeOnce sync.Once

	mu       sync.Mutex
	closeErr error
}

func (s *quicConnState) closeWithError(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
		close(s.closed)
	})
}

func (s *quicConnState) sessionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeErr == nil {
		return ErrQUICSessionClosed
	}

	return fmt.Errorf("%w: %v", ErrQUICSessionClosed, s.closeErr)
}

func (c *Client) connectQUIC(ctx context.Context, targetAddress string) (net.Conn, error) {
	tlsConfig, err := buildQUICClientTLSConfig(c.config, targetAddress)
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, c.config.ConnectionTimeout)
	defer cancel()

	conn, err := quic.DialAddr(dialCtx, targetAddress, tlsConfig, buildQUICRuntimeConfig(c.config))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: QUIC dial timeout: %v", ErrTimeout, err)
		}

		return nil, fmt.Errorf("aegis-client: failed to connect QUIC target node %s: %w", targetAddress, err)
	}

	managed := &managedQUICConn{owner: c, conn: conn}
	_ = c.getOrCreateQUICSession(managed)
	return managed, nil
}

func (c *Client) getOrCreateQUICSession(conn net.Conn) *quicConnState {
	if c == nil || conn == nil {
		return nil
	}

	if state, ok := c.quicSessions.Load(conn); ok {
		return state.(*quicConnState)
	}

	managed, ok := conn.(*managedQUICConn)
	if !ok {
		return nil
	}

	newState := &quicConnState{
		conn:          managed,
		responseQueue: make(chan *quic.Stream, c.config.QUICResponseQueueSize),
		closed:        make(chan struct{}),
	}

	actual, _ := c.quicSessions.LoadOrStore(conn, newState)
	return actual.(*quicConnState)
}

func (c *Client) deleteQUICSession(conn net.Conn) {
	if c == nil || conn == nil {
		return
	}

	if stateRaw, ok := c.quicSessions.Load(conn); ok {
		state := stateRaw.(*quicConnState)
		state.closeWithError(ErrQUICSessionClosed)
	}

	c.quicSessions.Delete(conn)
}

func (c *Client) sendQUIC(ctx context.Context, conn net.Conn, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	state := c.getOrCreateQUICSession(conn)
	if state == nil || state.conn == nil || state.conn.conn == nil {
		return fmt.Errorf("%w: invalid QUIC connection", ErrQUICSessionClosed)
	}

	stream, err := state.conn.conn.OpenStreamSync(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		return fmt.Errorf("aegis-client: failed to open QUIC stream: %w", err)
	}

	if err := applyQUICWriteDeadline(ctx, c.config.WriteTimeout, state.conn.writeDeadlineValue(), stream); err != nil {
		stream.CancelWrite(quicResponseQueueErrCode)
		_ = stream.Close()
		return err
	}

	if err := writeFull(stream, data); err != nil {
		stream.CancelWrite(quicResponseQueueErrCode)
		_ = stream.Close()
		if errors.Is(err, quic.ErrStreamClosed) {
			return fmt.Errorf("%w: %v", ErrQUICStreamClosed, err)
		}

		return fmt.Errorf("aegis-client: failed to send QUIC packet: %w", err)
	}

	if err := stream.Close(); err != nil {
		stream.CancelWrite(quicResponseQueueErrCode)
		return fmt.Errorf("aegis-client: failed to close QUIC send stream: %w", err)
	}

	select {
	case <-ctx.Done():
		stream.CancelRead(quicResponseQueueErrCode)
		return ctx.Err()
	case <-state.closed:
		stream.CancelRead(quicResponseQueueErrCode)
		return state.sessionError()
	case state.responseQueue <- stream:
		return nil
	default:
		stream.CancelRead(quicResponseQueueErrCode)
		return fmt.Errorf("%w: QUIC response queue is full", ErrBufferFull)
	}
}

func (c *Client) receiveQUIC(ctx context.Context, conn net.Conn) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	state := c.getOrCreateQUICSession(conn)
	if state == nil || state.conn == nil {
		return nil, fmt.Errorf("%w: invalid QUIC connection", ErrQUICSessionClosed)
	}

	var stream *quic.Stream
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-state.closed:
		return nil, state.sessionError()
	case stream = <-state.responseQueue:
	}

	if stream == nil {
		return nil, ErrQUICNoPendingResponse
	}

	defer func() {
		stream.CancelRead(quicResponseQueueErrCode)
		_ = stream.Close()
	}()

	if err := applyQUICReadDeadline(ctx, c.config.ReadTimeout, state.conn.readDeadlineValue(), stream); err != nil {
		return nil, err
	}

	packet, err := ReadPacket(stream, c.config.MaxPayloadSize)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		if isTimeout(err) {
			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}

		if errors.Is(err, io.EOF) || errors.Is(err, quic.ErrStreamClosed) {
			return nil, fmt.Errorf("%w: %v", ErrQUICStreamClosed, err)
		}

		return nil, fmt.Errorf("aegis-client: failed to receive QUIC packet: %w", err)
	}

	return packet.Payload, nil
}

func writeFull(w io.Writer, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := w.Write(data[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}

	return nil
}

func applyQUICReadDeadline(ctx context.Context, timeout time.Duration, connDeadline time.Time, stream *quic.Stream) error {
	deadline, err := effectiveDeadline(ctx, timeout, connDeadline)
	if err != nil {
		return err
	}

	if deadline.IsZero() {
		return nil
	}

	return stream.SetReadDeadline(deadline)
}

func applyQUICWriteDeadline(ctx context.Context, timeout time.Duration, connDeadline time.Time, stream *quic.Stream) error {
	deadline, err := effectiveDeadline(ctx, timeout, connDeadline)
	if err != nil {
		return err
	}

	if deadline.IsZero() {
		return nil
	}

	return stream.SetWriteDeadline(deadline)
}

func effectiveDeadline(ctx context.Context, timeout time.Duration, connDeadline time.Time) (time.Time, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	if !connDeadline.IsZero() {
		if deadline.IsZero() || connDeadline.Before(deadline) {
			deadline = connDeadline
		}
	}

	if ctx != nil {
		if d, ok := ctx.Deadline(); ok {
			if deadline.IsZero() || d.Before(deadline) {
				deadline = d
			}
		}
	}

	return deadline, nil
}

// QUICTransport provides reliable multiplexed stream transport over QUIC.
type QUICTransport struct {
	server   *Server
	listener *quic.Listener

	mu          sync.Mutex
	connections map[*quic.Conn]struct{}
}

// NewQUICTransport creates a QUIC transport for the server.
func NewQUICTransport(s *Server) *QUICTransport {
	return &QUICTransport{
		server:      s,
		connections: make(map[*quic.Conn]struct{}),
	}
}

// Start binds the QUIC listener and accepts incoming connections and streams.
func (t *QUICTransport) Start(ctx context.Context) error {
	if t == nil || t.server == nil {
		return fmt.Errorf("aegis-quic: server is nil")
	}

	if ctx.Err() != nil {
		return ErrServerClosed
	}

	tlsConfig, err := buildQUICServerTLSConfig(t.server.config)
	if err != nil {
		return err
	}

	listener, err := quic.ListenAddr(t.server.config.Address, tlsConfig, buildQUICRuntimeConfig(t.server.config))
	if err != nil {
		return fmt.Errorf("aegis-quic: failed to bind address %s: %w", t.server.config.Address, err)
	}

	t.listener = listener
	defer func() {
		_ = listener.Close()
	}()

	t.server.logger.Info("Aegis QUIC Engine successfully running", "address", t.server.config.Address, "max_conn", t.server.config.MaxConnections)

	for {
		conn, acceptErr := listener.Accept(ctx)
		if acceptErr != nil {
			if ctx.Err() != nil || errors.Is(acceptErr, quic.ErrServerClosed) {
				return nil
			}

			t.server.logger.Warn("QUIC connection acceptance failed", "error", acceptErr)
			continue
		}

		select {
		case t.server.semaphore <- struct{}{}:
			t.addConnection(conn)
			t.server.wg.Add(1)
			go t.handleQUICConnection(ctx, conn)
		case <-ctx.Done():
			_ = conn.CloseWithError(quicAppErrCodeGraceful, quicCloseReason)
			return nil
		case <-t.server.quit:
			_ = conn.CloseWithError(quicAppErrCodeGraceful, quicCloseReason)
			return nil
		default:
			t.server.logger.Warn("Max connection limit reached", "remote_addr", conn.RemoteAddr().String())
			_ = conn.CloseWithError(quicAppErrCodeMaxConn, "max connections reached")
		}
	}
}

func (t *QUICTransport) handleQUICConnection(ctx context.Context, conn *quic.Conn) {
	defer t.server.wg.Done()
	defer func() {
		t.removeConnection(conn)
		<-t.server.semaphore
		_ = conn.CloseWithError(quicAppErrCodeGraceful, quicCloseReason)
	}()

	remoteAddr := conn.RemoteAddr().String()
	t.server.logger.Debug("Inbound QUIC node connected", "remote_addr", remoteAddr)

	for {
		if ctx.Err() != nil {
			return
		}

		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			if conn.Context().Err() != nil {
				return
			}

			t.server.logger.Debug("QUIC connection stream accept ended", "remote_addr", remoteAddr, "error", err)
			return
		}

		t.server.wg.Add(1)
		go t.handleQUICStream(ctx, conn, stream)
	}
}

func (t *QUICTransport) handleQUICStream(ctx context.Context, conn *quic.Conn, stream *quic.Stream) {
	defer t.server.wg.Done()
	defer func() {
		stream.CancelRead(quicResponseQueueErrCode)
		_ = stream.Close()
	}()

	remoteAddr := conn.RemoteAddr().String()

	for {
		if ctx.Err() != nil {
			return
		}

		if t.server.config.ReadTimeout > 0 {
			if err := stream.SetReadDeadline(time.Now().Add(t.server.config.ReadTimeout)); err != nil {
				t.server.logger.Warn("Failed setting QUIC stream read deadline", "remote_addr", remoteAddr, "error", err)
				return
			}
		}

		packet, err := ReadPacket(stream, t.server.config.MaxPayloadSize)
		if err != nil {
			if isExpectedQUICStreamClosure(err) {
				return
			}
			if isTimeout(err) {
				t.server.logger.Debug("QUIC stream read timed out", "remote_addr", remoteAddr)
				return
			}
			t.server.logger.Warn("Failed to decode QUIC packet", "remote_addr", remoteAddr, "error", err)
			return
		}

		if t.server.handler == nil {
			continue
		}

		if quicHandler, ok := t.server.handler.(QUICPacketHandler); ok {
			responsePayload, handlerErr := quicHandler.HandleQUICPacket(packet.Payload, conn.RemoteAddr())
			if handlerErr != nil {
				t.server.logger.Error("QUIC packet handler failed", "remote_addr", remoteAddr, "error", handlerErr)
				continue
			}

			if responsePayload == nil {
				continue
			}

			if uint64(len(responsePayload)) > uint64(t.server.config.MaxPayloadSize) {
				t.server.logger.Warn("QUIC response payload exceeds configured limit", "remote_addr", remoteAddr, "payload_len", len(responsePayload), "max_payload", t.server.config.MaxPayloadSize)
				continue
			}

			responseData, serErr := NewPacket(responsePayload).Serialize()
			if serErr != nil {
				t.server.logger.Warn("Failed to serialize QUIC response packet", "remote_addr", remoteAddr, "error", serErr)
				continue
			}

			if t.server.config.WriteTimeout > 0 {
				if err := stream.SetWriteDeadline(time.Now().Add(t.server.config.WriteTimeout)); err != nil {
					t.server.logger.Warn("Failed setting QUIC stream write deadline", "remote_addr", remoteAddr, "error", err)
					continue
				}
			}

			if writeErr := writeFull(stream, responseData); writeErr != nil {
				t.server.logger.Warn("Failed to write QUIC response packet", "remote_addr", remoteAddr, "error", writeErr)
				return
			}

			continue
		}

		if err := t.server.handler.HandlePacket(packet.Payload); err != nil {
			t.server.logger.Error("QUIC packet handler failed", "remote_addr", remoteAddr, "error", err)
		}
	}
}

func isExpectedQUICStreamClosure(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, ErrConnectionClosed) ||
		errors.Is(err, ErrQUICStreamClosed) ||
		errors.Is(err, quic.ErrStreamClosed)
}

// Close closes the QUIC listener and active connections.
func (t *QUICTransport) Close() error {
	if t == nil {
		return nil
	}

	var closeErr error
	if t.listener != nil {
		if err := t.listener.Close(); err != nil {
			closeErr = err
		}
	}

	t.mu.Lock()
	for conn := range t.connections {
		if err := conn.CloseWithError(quicAppErrCodeGraceful, quicCloseReason); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	t.mu.Unlock()

	return closeErr
}

func (t *QUICTransport) addConnection(conn *quic.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connections[conn] = struct{}{}
}

func (t *QUICTransport) removeConnection(conn *quic.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.connections, conn)
}

func buildQUICRuntimeConfig(cfg *Config) *quic.Config {
	if cfg == nil {
		return &quic.Config{}
	}

	return &quic.Config{
		HandshakeIdleTimeout: cfg.QUICHandshakeTimeout,
		MaxIdleTimeout:       cfg.QUICIdleTimeout,
		KeepAlivePeriod:      cfg.QUICKeepAlivePeriod,
		MaxIncomingStreams:   cfg.QUICMaxIncomingStreams,
	}
}

func buildQUICServerTLSConfig(cfg *Config) (*tls.Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil configuration", ErrQUICTLSConfiguration)
	}

	tlsConfig, err := cloneOrCreateTLSConfig(cfg.QUICTLSConfig)
	if err != nil {
		return nil, err
	}

	if len(tlsConfig.Certificates) == 0 {
		if cfg.QUICCertFile == "" || cfg.QUICKeyFile == "" {
			return nil, fmt.Errorf("%w: QUIC server certificate and key are required", ErrQUICTLSConfiguration)
		}

		cert, certErr := tls.LoadX509KeyPair(cfg.QUICCertFile, cfg.QUICKeyFile)
		if certErr != nil {
			return nil, fmt.Errorf("%w: failed loading QUIC server keypair: %v", ErrQUICTLSConfiguration, certErr)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if cfg.QUICRequireClientCert {
		if cfg.QUICClientCAFile == "" {
			return nil, fmt.Errorf("%w: QUIC client CA file is required when client cert verification is enabled", ErrQUICTLSConfiguration)
		}

		pool, poolErr := loadCertPoolFromFile(nil, cfg.QUICClientCAFile)
		if poolErr != nil {
			return nil, poolErr
		}

		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return enforceQUICTLSDefaults(tlsConfig, cfg.QUICALPN)
}

func buildQUICClientTLSConfig(cfg *Config, targetAddress string) (*tls.Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil configuration", ErrQUICTLSConfiguration)
	}

	tlsConfig, err := cloneOrCreateTLSConfig(cfg.QUICTLSConfig)
	if err != nil {
		return nil, err
	}

	if tlsConfig.ServerName == "" {
		if cfg.QUICServerName != "" {
			tlsConfig.ServerName = cfg.QUICServerName
		} else {
			host, splitErr := hostFromAddress(targetAddress)
			if splitErr != nil {
				return nil, fmt.Errorf("%w: failed to derive server name: %v", ErrQUICTLSConfiguration, splitErr)
			}
			tlsConfig.ServerName = host
		}
	}

	if tlsConfig.RootCAs == nil {
		systemPool, poolErr := x509.SystemCertPool()
		if poolErr != nil {
			return nil, fmt.Errorf("%w: failed loading system CA pool: %v", ErrQUICTLSConfiguration, poolErr)
		}
		tlsConfig.RootCAs = systemPool
	}

	if cfg.QUICCACertFile != "" {
		pool, poolErr := loadCertPoolFromFile(tlsConfig.RootCAs, cfg.QUICCACertFile)
		if poolErr != nil {
			return nil, poolErr
		}
		tlsConfig.RootCAs = pool
	}

	return enforceQUICTLSDefaults(tlsConfig, cfg.QUICALPN)
}

func cloneOrCreateTLSConfig(base *tls.Config) (*tls.Config, error) {
	if base == nil {
		return &tls.Config{}, nil
	}

	clone := base.Clone()
	if clone.InsecureSkipVerify {
		return nil, fmt.Errorf("%w: InsecureSkipVerify must not be enabled", ErrQUICTLSConfiguration)
	}

	return clone, nil
}

func enforceQUICTLSDefaults(cfg *tls.Config, alpn string) (*tls.Config, error) {
	if cfg == nil {
		cfg = &tls.Config{}
	}

	if cfg.InsecureSkipVerify {
		return nil, fmt.Errorf("%w: InsecureSkipVerify must not be enabled", ErrQUICTLSConfiguration)
	}

	cfg.MinVersion = tls.VersionTLS13

	if len(cfg.NextProtos) == 0 {
		cfg.NextProtos = []string{alpn}
	}

	return cfg, nil
}

func loadCertPoolFromFile(base *x509.CertPool, path string) (*x509.CertPool, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read certificate file %s: %v", ErrQUICTLSConfiguration, path, err)
	}

	pool := base
	if pool == nil {
		pool = x509.NewCertPool()
	}

	if ok := pool.AppendCertsFromPEM(pemData); !ok {
		return nil, fmt.Errorf("%w: failed to append CA certificates from %s", ErrQUICTLSConfiguration, path)
	}

	return pool, nil
}

func hostFromAddress(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host, nil
	}

	if ip := net.ParseIP(addr); ip != nil {
		return addr, nil
	}

	return "", err
}
