AEGIS — GitHub Copilot Project Instructions
1. Project identity
AEGIS is an independent networking and distributed-systems SDK developed by StarkTech and written in Go.

Its purpose is to provide reusable communication infrastructure for distributed machines, autonomous nodes, robotics, UAVs, industrial systems, servers, and edge devices.

AEGIS is infrastructure, not an end-user application. It must remain modular and usable independently of any one application, AI model, hardware vendor, or operating system.

AEGIS is intended to become a communication and coordination foundation that the future S.I.M.A. platform may use. AEGIS must not depend on S.I.M.A.
2. Source-of-truth rules
- The checked-out repository and its actual source code are authoritative for current implementation status.
- This file and README.md describe project intent and navigation; they do not prove that a feature is implemented.
- Inspect the repository tree before relying on any file map below. Paths and package boundaries may have changed.
- Verify technical claims against code, tests, configuration, and call sites.
- Label uncertain conclusions as unverified or potential risks.
- Do not invent functions, files, protocol behavior, test results, or performance claims.
3. Intended architecture
Keep the following responsibilities clearly separated. This is the intended design direction, not a claim that every layer currently exists.
1. Transport: moving bytes or datagrams between endpoints (for example TCP, UDP, and potentially QUIC).
2. Packet protocol: framing, header fields, packet types, flags, lengths, encoding, decoding, and versioning.
3. Reliability: acknowledgments, sequence tracking, duplicate detection, ordering, retransmission, timeout policy, and delivery semantics.
4. Security: encrypted transport, peer identity, authentication, authorization, integrity, replay resistance, and key lifecycle.
5. Distributed coordination: node discovery, membership, health/failure detection, leader election, failover, and recovery.
6. Public SDK/API: stable, documented interfaces that allow applications to use the infrastructure without coupling to internal implementations.
Avoid duplicating capabilities already provided by a selected transport. Keep transport implementations interchangeable where that is justified by the actual requirements.
4. Engineering objectives to evaluate
These are project objectives. Their implementation status must always be checked in the repository.
Transport
- TCP
- UDP
- Reliable UDP (RUDP), if justified
- QUIC integration, if justified
- Connection/session lifecycle
- Deadlines, cancellation, graceful shutdown, and reconnection
Protocol and reliability
- Binary packet framing and validation
- Packet types and flags
- ACK generation and processing
- Sequence numbers and wraparound
- Duplicate detection and message ordering
- Retransmission, timeout, and retry limits
- Delivery guarantees and bounded queues
- Behavior under loss, duplication, reordering, and failure
Distributed coordination
- Heartbeat and node health monitoring
- Node discovery and membership
- Leader election
- Failover and recovery
- Network partition and split-brain handling
Heartbeat is a failure-detection signal, not a consensus algorithm. Any leader-election design must state its membership and consistency assumptions and explain how it prevents conflicting leaders.
Security and operations
- TLS or mutual TLS (mTLS), where appropriate
- Node identity, authentication, and authorization
- Message integrity and replay protection
- Secure credential and key management
- Limits for connections, packet sizes, buffers, queues, and retries
- Structured logging, metrics, tracing, and observability
- Unit, integration, protocol, security, concurrency, and failure tests
- Race detection and meaningful benchmarks
5. Protocol and security principles
- Prefer standard, reviewed cryptographic protocols and libraries. Never design custom cryptographic algorithms.
- CRC32 can detect accidental corruption; it does not authenticate a sender or protect against malicious modification.
- A successful ACK exchange alone does not prove complete reliable transport semantics.
- Validate all untrusted network input, including packet lengths, flags, types, and version fields.
- Consider integer overflow, malformed packets, oversized datagrams, replay, spoofing, resource exhaustion, and denial-of-service conditions.
- Make resource limits and failure behavior explicit.
- Do not claim security or reliability guarantees that are not demonstrated by the implementation and tests.
6. Repository navigation
The following paths have appeared in the project structure and are useful starting points. They are not an exhaustive or guaranteed-current inventory; inspect the actual tree first.
- pkg/aegis/ — core SDK area
- pkg/aegis/client.go — client-related implementation, if still present
- pkg/aegis/server.go — server lifecycle and connection handling, if still present
- pkg/aegis/transport.go — transport-related implementation, if still present
- pkg/aegis/rudp.go — RUDP-related implementation, if still present
- pkg/aegis/packet.go — packet framing/encoding area, if still present
- pkg/aegis/config.go — configuration and validation area, if still present
- pkg/aegis/errors.go — shared errors, if still present
- cmd/ — executable commands and demos
- internal/ — private implementation packages
- examples/ — integration examples
- docs/architecture/ — architecture documentation
- docs/protocol/ — protocol specifications
- docs/diagrams/ — diagrams and sequence flows
- docs/research/ — research and benchmark notes
- .github/workflows/ — CI workflows
- go.mod and go.sum — module and dependency declarations
- Makefile — project automation, if present
- README.md — project overview and entry point
When discussing a component, locate its actual implementation and tests rather than assuming the path above is current.
7. Go implementation expectations
- Use idiomatic Go and follow existing package conventions.
- Prefer standard-library solutions where appropriate.
- Keep interfaces small and meaningful.
- Avoid unnecessary abstractions and dependencies.
- Use context-aware APIs for cancellable operations.
- Define goroutine ownership and shutdown behavior.
- Prevent data races and avoid goroutine, connection, and buffer leaks.
- Ensure all queues and resource-consuming operations have deliberate limits.
- Preserve public API compatibility unless a breaking change is explicitly approved.
- Explain trade-offs when choosing between TCP, UDP, RUDP, and QUIC.
8. Audit and implementation workflow
When asked to audit, review, or investigate:
- Work read-only.
- Inspect the repository tree, real source code, tests, module files, CI, and relevant documentation.
- Do not create or modify files, install dependencies, format code, or run write-producing commands.
- Do not claim tests pass unless they were actually executed and observed.
- Produce evidence-based findings with severity, verification status, file path, symbol, behavior, impact, recommendation, and required tests.
- Separate verified defects from potential risks and missing features.
When asked to implement:
- First summarize the proposed scope and affected components.
- Wait for explicit approval if the requested change or its architectural implications are not already explicit.
- Make the smallest coherent change.
- Add or update tests and documentation as appropriate.
- Run relevant verification commands only when authorized and safe.
- Report exactly what changed and which checks were actually run.
9. Decision priorities
Use this priority order when evaluating trade-offs:
1. Correctness and protocol clarity
2. Security and safe failure behavior
3. Concurrency safety and resource lifecycle
4. Reliability and recovery
5. Maintainability and API stability
6. Performance, supported by reproducible measurements
Do not prioritize advanced features such as QUIC, RUDP, heartbeat, or leader election merely because they are listed as goals. Recommend implementation order based on the actual audit findings, dependencies, and product requirements.