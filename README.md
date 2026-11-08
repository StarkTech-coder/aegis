# AEGIS — Autonomous Distributed Networking Infrastructure

![Go](https://img.shields.io/badge/Go-1.26.2%2B-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Project](https://img.shields.io/badge/Project-StarkTech-111827)
![Status](https://img.shields.io/badge/Development-Active-orange)

**AEGIS** is an independent, modular, high-performance distributed networking SDK developed by StarkTech and written in Go.

It is designed to provide reliable, secure, and low-latency communication infrastructure for distributed machines, autonomous systems, robotics, UAVs, industrial environments, servers, and edge devices.

AEGIS focuses on the networking and coordination infrastructure required to connect independent systems and enable them to communicate reliably.

> **AEGIS is infrastructure, not an application.**
>
> It provides the communication, reliability, security, and distributed coordination foundations upon which higher-level autonomous systems can be built.

AEGIS is also intended to serve as a core communication infrastructure component of StarkTech's future **S.I.M.A. (Star Intelligent Machine / Autonomous)** platform.

---

## 1. Vision

The long-term objective of AEGIS is to become a reusable distributed networking infrastructure that can operate across different hardware platforms, operating environments, and application domains.

The architecture must not depend on a particular application, AI model, hardware vendor, or industry.

Potential application domains include:

- Autonomous robotics
- UAV and unmanned systems
- Defense and aerospace infrastructure
- Industrial automation
- Maritime and distributed marine systems
- IoT and telemetry networks
- Edge computing
- Distributed servers and autonomous nodes
- Future space exploration infrastructure

These are target application domains, not claims that all integrations are currently implemented.

---

## 2. Core Engineering Objectives

AEGIS is being developed around the following engineering capabilities.

### Networking and Transport

- TCP communication
- UDP communication
- Reliable UDP (RUDP)
- QUIC transport integration
- Interchangeable transport interfaces
- Connection lifecycle management
- Graceful shutdown and reconnection

### Reliable Communication

- ACK (Acknowledgment) mechanisms
- Packet sequencing
- Duplicate packet detection
- Retransmission management
- Timeout handling
- Message delivery guarantees
- Packet ordering
- Bounded message queues
- Failure recovery

### Distributed Coordination

- Heartbeat monitoring
- Node health monitoring
- Node discovery
- Leader election
- Failover
- Distributed node coordination
- Network partition handling

### Security

- TLS and mutual TLS (mTLS)
- Node identity
- Authentication and authorization
- Message integrity
- Replay protection
- Secure key management
- Resource exhaustion protection

### Engineering and Operations

- Concurrency safety
- Context-aware operations
- Structured logging
- Metrics and observability
- Performance benchmarks
- Unit and integration testing
- Race detection

**Important:** These are engineering objectives. Their implementation status must be verified against the actual source code.

---

## 3. System Architecture

AEGIS should maintain a clear separation between its major engineering responsibilities.

```text
                  APPLICATION LAYER
       Robotics / UAV / Industrial / S.I.M.A.
                         |
                         |
                  AEGIS PUBLIC API
                         |
             +-----------+-----------+
             |                       |
       COMMUNICATION             COORDINATION
          LAYER                      LAYER
             |                       |
       +-----+------+          +-----+------+
       |            |          |            |
    Transport   Reliability  Heartbeat   Node State
       |            |          |            |
   +---+---+     +--+---+      |        Leader Election
   |   |   |     |      |      |            |
  TCP UDP QUIC  ACK  Retry  Discovery    Failover
       |
      RUDP
       |
       +--------------------------------+
       |                                |
       |        SECURITY LAYER          |
       |                                |
       | TLS / mTLS / Identity / Auth   |
       | Message Integrity / Policies  |
       +--------------------------------+
                         |
                  NETWORK INTERFACE
                         |
              Distributed Infrastructure
```

This diagram represents the intended architectural separation, not a statement that every component is already implemented.

### Architectural Principles

1. Transport and application logic must remain independent.
2. Reliability mechanisms must be separated from basic packet transport.
3. Distributed coordination must not be coupled to a particular transport.
4. Security must be designed as a foundational layer.
5. Interfaces should permit different transport implementations.
6. Resource usage must be bounded.
7. Failure handling must be explicit and testable.
8. Public APIs should remain stable unless a breaking change is approved.

---

## 4. Transport Layer

The transport layer is responsible for moving data between distributed nodes.

### TCP

TCP provides connection-oriented communication and reliable, ordered byte-stream delivery.

The implementation must correctly handle:

- Connection lifecycle
- Partial reads and writes
- Packet framing
- Context cancellation
- Timeouts
- Graceful shutdown
- Reconnection policies

### UDP

UDP provides connectionless datagram communication.

The implementation must account for:

- Packet loss
- Packet duplication
- Packet reordering
- Datagram size limits
- Malformed packets
- Resource exhaustion

### RUDP

Reliable UDP is intended to provide reliability mechanisms above UDP.

Its engineering responsibilities include:

- ACK processing
- Sequence number management
- Duplicate detection
- Retransmission
- Timeout management
- Delivery tracking
- Queue management
- Session lifecycle
- Recovery behavior

RUDP must be evaluated carefully. A basic ACK implementation alone does not constitute a complete reliable transport protocol.

### QUIC

QUIC is a target transport integration.

It should be evaluated using established Go implementations rather than a custom cryptographic or transport implementation.

Its relationship with TCP, UDP, and RUDP must be decided based on architecture, performance, reliability, and security requirements.

---

## 5. Reliability and Packet Protocol

AEGIS uses a custom binary packet protocol.

The protocol implementation is responsible for defining:

- Packet structure
- Header fields
- Payload length
- Packet types
- Packet flags
- Sequence identifiers
- Integrity verification
- Message validation

The existing protocol specification is documented in:

`docs/protocol/binary_spec.md`

The packet implementation is located in:

`pkg/aegis/packet.go`

The protocol must be evaluated for:

- Malformed packet handling
- Invalid lengths
- Integer overflow
- Sequence number wraparound
- Duplicate messages
- Packet reordering
- Version compatibility
- Resource exhaustion

CRC32 may be used for accidental corruption detection, but it must not be treated as cryptographic authentication.

---

## 6. Distributed Coordination

Distributed coordination is one of the long-term objectives of AEGIS.

It is intended to provide infrastructure for systems containing multiple independent nodes.

### Heartbeat

Heartbeat mechanisms are intended to monitor node availability and detect communication failures.

The design must consider:

- Heartbeat intervals
- Failure detection timeouts
- Missed heartbeat thresholds
- Network latency
- Temporary disconnections
- Node recovery
- False failure detection

#### Current implementation (Phase 1)

AEGIS now includes a heartbeat and node health monitoring foundation in [pkg/aegis/heartbeat.go](pkg/aegis/heartbeat.go).

- Configurable `HeartbeatConfig`:
   - `Interval`
   - `Timeout`
   - `SuspectThreshold`
   - `UnhealthyThreshold`
   - `LocalNodeID`
- Health states:
   - `healthy`
   - `suspect`
   - `unhealthy`
- Public monitor API:
   - `NewHeartbeatMonitor(...)`
   - `AddNode(...)`, `RemoveNode(...)`
   - `Start(ctx)`, `Stop()`
   - `GetNodeHealth(nodeID)`
   - `ListNodeHealth()`

The monitor is transport-agnostic and uses a `HeartbeatProbe` interface for heartbeat exchange, allowing future integration with node discovery, leader election, and failover without coupling this phase to a specific transport.

Minimal working example:

- [examples/heartbeat/main.go](examples/heartbeat/main.go)

### Node Discovery

Node discovery is intended to identify available nodes and maintain information about the distributed network.

#### Current implementation (Phase 2)

AEGIS now includes a transport-agnostic node discovery foundation in [pkg/aegis/discovery.go](pkg/aegis/discovery.go).

- `DiscoverySource` interface for pluggable discovery mechanisms.
- `DiscoverySource` currently requires a concrete backend implementation provided by the application (AEGIS does not ship a network discovery protocol by default).
- `DiscoveryManager` lifecycle APIs:
   - `Start(ctx)` / `Stop()`
   - `RegisterNode(...)` / `UpdateNode(...)` / `RemoveNode(...)`
   - `GetNode(...)` / `ListNodes()`
- `DiscoveredNode` snapshot fields:
   - node ID
   - address
   - discovery timestamp
   - last seen timestamp
   - health state
   - metadata
- Duplicate registration is rejected.
- Node expiration and cleanup are configurable.
- Health state synchronization integrates with heartbeat monitoring.

Minimal example:

- [examples/discovery/main.go](examples/discovery/main.go)

### Leader Election

Leader election is intended to select a coordinator among participating nodes.

The design must account for:

- Leader selection
- Election timeouts
- Leader failure
- Re-election
- Network partitions
- Split-brain prevention
- Node membership changes

Heartbeat and leader election are separate responsibilities.

Heartbeat can help detect failures, but it does not provide distributed consensus by itself.

A production-oriented leader election design must define its consistency and failure assumptions explicitly.

#### Current implementation (Phase 2)

AEGIS now includes a leader election foundation in [pkg/aegis/leader_election.go](pkg/aegis/leader_election.go).

- `LeaderElector` APIs:
   - `Start(ctx)` / `Stop()`
   - `Join(ctx)` / `Leave(ctx)`
   - `CurrentLeader()` / `IsLeader()`
   - `Resign(ctx)`
   - `LeadershipChanges()`
- `LeaderStore` interface provides a transport-agnostic coordination backend.
- `InMemoryLeaderStore` is included for tests/examples and process-local coordination.
- Election uses term and lease-based records and deterministic candidate ordering among eligible participants.
- Heartbeat/discovery health is used to avoid retaining unhealthy leaders.

Consistency model and limitations:

- Strong consistency is **not** guaranteed by default.
- Safety and split-brain resistance depend on the chosen `LeaderStore` backend.
- `InMemoryLeaderStore` is process-local and intended for development/testing only.
- Production deployments require a fault-tolerant shared coordination backend implementing `LeaderStore` semantics.

Minimal example:

- [examples/leader-election/main.go](examples/leader-election/main.go)

### Failover and Recovery

Failover mechanisms are intended to maintain service continuity when nodes or network connections fail.

Recovery behavior must be deterministic, observable, and testable.

#### Current implementation (Phase 3)

AEGIS now includes a transport-agnostic failover manager in [pkg/aegis/failover.go](pkg/aegis/failover.go).

- `FailoverManager` APIs:
   - `Start(ctx)` / `Stop()`
   - `ActiveLeader()`
   - `FailoverChanges()`
   - `RegisterHandler(...)` / `UnregisterHandler(...)`
- Failure detection sources:
   - leader lease expiration from `LeaderRecord.LeaseExpiresAt`
   - leader health from discovery and heartbeat state
   - optional suspect-as-failure policy via `FailoverConfig.TreatSuspectAsFailure`
- Automatic failover behavior:
   - clears stale/failed leader records with term-incremented CAS where appropriate
   - triggers local election reconciliation after failover actions
   - prevents overlapping failover attempts in a single manager instance
- Routing integration:
   - emits serialized failover events (`FailoverEvent`)
   - invokes registered handlers sequentially (non-concurrent per transition)
   - handler errors are logged and do not crash the SDK

Leader recovery behavior:

- A recovered previous leader does not automatically reclaim leadership.
- Recovery flows through normal election semantics and latest `LeaderRecord` term.
- `FailoverEvent.FencingToken` is the observed monotonic election term and can be used by applications to reject stale actions.

Consistency and split-brain limitations:

- Failover safety is bounded by `LeaderStore` guarantees.
- `InMemoryLeaderStore` is process-local and cannot provide cross-process consensus, quorum, or durable fencing on its own.
- Production deployments require a fault-tolerant, atomic shared coordination backend (for example, one supporting linearizable CAS semantics and durable term progression).

Transport behavior note:

- Failover updates leadership and routing signals only; AEGIS does not silently migrate existing TCP/UDP/RUDP/QUIC connections.

Minimal example:

- [examples/failover/example.go](examples/failover/example.go)

Run:

```bash
go run ./examples/failover
```

---

## 7. Security Architecture

Security is a foundational requirement.

AEGIS must use established cryptographic standards and reviewed implementations.

Security objectives include:

- TLS and mutual TLS
- Node authentication
- Authorization policies
- Secure node identity
- Message integrity
- Replay protection
- Secure credential storage
- Key rotation
- Connection limits
- Bounded buffers and queues
- Protection against malformed and oversized packets

Custom cryptographic algorithms must not be introduced.

CRC32 is not a replacement for message authentication.

---

## 8. Repository Structure

The following structure describes the main areas of the AEGIS repository.

```text
AEGIS/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── cmd/
│   └── demo/
│       └── main.go
│
├── docs/
│   ├── architecture/
│   │   └── system_architecture.md
│   │
│   ├── diagrams/
│   │   └── sequence_diagram.md
│   │
│   ├── protocol/
│   │   └── binary_spec.md
│   │
│   └── research/
│       └── performance_benchmarks.md
│
├── examples/
│
├── internal/
│
├── pkg/
│   └── aegis/
│       ├── client.go
│       ├── config.go
│       ├── errors.go
│       ├── packet.go
│       ├── packet_test.go
│       ├── rudp.go
│       ├── server.go
│       ├── server_test.go
│       └── transport.go
│
├── .gitignore
├── go.mod
├── LICENSE
├── Makefile
└── README.md
```

The repository structure may evolve as the architecture develops.

### Core Package Responsibilities

| File | Responsibility |
|---|---|
| `client.go` | Outbound client and client lifecycle |
| `server.go` | Server lifecycle and connection handling |
| `transport.go` | Transport interfaces and implementations |
| `rudp.go` | Reliable UDP mechanisms |
| `packet.go` | Binary packet encoding and decoding |
| `config.go` | Configuration and validation |
| `errors.go` | Shared error definitions |

### Documentation Responsibilities

| Location | Purpose |
|---|---|
| `docs/architecture/` | System architecture and component boundaries |
| `docs/protocol/` | Packet format and protocol specifications |
| `docs/diagrams/` | Communication and sequence diagrams |
| `docs/research/` | Performance research and benchmarks |

**Implementation rule:** Do not assume that a component exists simply because its intended location is described here. Inspect the source code before making conclusions.

---

## 9. Performance and Benchmarking

Performance is an important engineering objective, but correctness, reliability, and security take priority.

Existing benchmark documentation:

`docs/research/performance_benchmarks.md`

Previously reported local benchmark figures include approximately 21.39 million packet-processing operations per second on Apple Silicon M2 under a local loopback benchmark.

These figures are historical reported results, not independently verified production network throughput.

Benchmark claims must always identify:

- Hardware
- Operating system
- Go version
- Packet size
- Number of workers
- Test duration
- Allocation metrics
- Benchmark methodology
- Whether the test uses loopback or a real network

Future performance evaluations should include:

- `ns/op`
- `B/op`
- `allocs/op`
- Throughput
- Latency percentiles
- CPU utilization
- Memory usage
- Concurrent workload behavior

---

## 10. Testing and Quality

All future implementations must be supported by tests.

Required testing categories:

- Unit tests
- Integration tests
- Protocol tests
- Transport tests
- Reliability tests
- Security tests
- Concurrency tests
- Failure recovery tests
- Benchmarks

Where applicable, use:

```bash
go