# Aegis Core Engine - System Architecture

## Overview
Aegis Core is a high-performance, concurrent network SDK written in Go. It provides a robust, zero-OOM binary communication layer designed for real-time telemetry and IoT streaming platforms.

## Core Architecture

### Concurrency Model
Aegis uses Go's lightweight concurrency primitives (`goroutines` and `channels`) to achieve high throughput under heavy network loads:
- **Listener Thread:** Accepts incoming TCP connections and spawns a dedicated worker goroutine per connection.
- **Connection Isolation:** Every client session operates independently, ensuring a single bad connection cannot block the engine event loop.

### Memory & OOM Protection
- **Framed Reading (`io.ReadFull`):** Reads exact binary payload lengths specified in the header to avoid unbounded buffer allocations.
- **Max Payload Cap:** Packets exceeding the configured threshold (`MaxPacketSize`) are discarded immediately to protect against Memory Exhaustion (OOM) attacks.

### Lifecycle & Graceful Shutdown
- **Context Handling (`context.Context`):** Propagates cancellation signals across active goroutines.
- **Resource Cleanup (`sync.WaitGroup`):** Tracks running workers and guarantees all active sockets flush their data and close gracefully before the process exits.

## Strategic Roadmap
# Aegis Core Engine - System Architecture

## Overview
Aegis Core is a high-performance, concurrent network SDK written in Go. It provides a robust, zero-OOM binary communication layer designed for real-time telemetry and IoT streaming platforms.

## Core Architecture

### Concurrency Model
Aegis uses Go's lightweight concurrency primitives (`goroutines` and `channels`) to achieve high throughput under heavy network loads:
- **Listener Thread:** Accepts incoming TCP connections and spawns a dedicated worker goroutine per connection.
- **Connection Isolation:** Every client session operates independently, ensuring a single bad connection cannot block the engine event loop.

### Memory & OOM Protection
- **Framed Reading (`io.ReadFull`):** Reads exact binary payload lengths specified in the header to avoid unbounded buffer allocations.
- **Max Payload Cap:** Packets exceeding the configured threshold (`MaxPacketSize`) are discarded immediately to protect against Memory Exhaustion (OOM) attacks.

### Lifecycle & Graceful Shutdown
- **Context Handling (`context.Context`):** Propagates cancellation signals across active goroutines.
- **Resource Cleanup (`sync.WaitGroup`):** Tracks running workers and guarantees all active sockets flush their data and close gracefully before the process exits.

## Strategic Roadmap

### Phase 1: Core Engine (Current - v0.2)
- [x] High-performance TCP Server & Client
- [x] Binary Frame Encoder/Decoder
- [x] CRC32 Data Integrity Verification
- [x] Structured Logging via `log/slog`

### Phase 2: Transport & Reliability (v0.3 - v0.5)
- [ ] Reliable UDP Transport Layer
- [ ] Heartbeat & Connection Keep-Alive System
- [ ] Message Routing Engine & Compression

### Phase 3: Enterprise Integration (v1.0)
- [ ] TLS/mTLS Security Layer
- [ ] NATS / gRPC Streaming Bridges
- [ ] Real-time Prometheus Metrics Integration
