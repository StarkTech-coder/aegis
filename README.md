# Aegis Core Engine 🛡️

![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-orange)
![Performance](https://img.shields.io/badge/Packet_Processing-21M%2B/sec-brightgreen)

**Aegis Core** is a reusable networking SDK for building high-performance TCP-based telemetry, IoT, UAV, and edge communication systems.

It provides a custom binary protocol, packet framing, integrity verification, connection lifecycle management, and a flexible packet processing pipeline while remaining completely independent of application-specific business logic.

> **Aegis Core is not an application.**
>
> It is a reusable networking SDK designed for telemetry, IoT, UAV, and edge computing systems.
>
> **Aegis Tracker** is the companion application that demonstrates how the SDK can be integrated into a real-world telemetry ingestion platform, including packet processing, REST APIs, data persistence, and a live monitoring dashboard.

---

## 🚀 Key Features

- **Custom Binary Protocol** – Lightweight binary frame format with magic bytes, payload length prefixes, and CRC32 integrity verification.
- **Application Agnostic** – Networking infrastructure remains completely separated from business logic through the `PacketHandler` abstraction.
- **Decoupled Architecture** – Any application can plug into Aegis Core by implementing the `PacketHandler` interface.
- **Resource Exhaustion Protection** – Buffered semaphore-based connection limiting with configurable `MaxConnections`.
- **Low-Allocation Buffering** – Optimized `bufio.Reader` buffering minimizes kernel syscalls and memory allocations.
- **Context-Aware Lifecycles** – Graceful startup, shutdown, and connection management using Go's standard `context.Context`.
- **Thread-Safe by Design** – Built for highly concurrent TCP workloads using idiomatic Go concurrency patterns.

---

## 🏗️ System Architecture

```text
                 Application
          (IoT / UAV / Telemetry)

                PacketHandler
                      ▲
                      │
          ┌──────────────────────┐
          │    Aegis Core SDK    │
          └──────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
        ▼             ▼             ▼
 TCP Server      Binary Protocol   TCP Client
                      │
      Magic + Length + CRC32 + Payload
```

---

## ⚡ Performance

> ⚠️ **Benchmark Disclaimer**
>
> Benchmark results were obtained on **Apple Silicon M2** using a **local loopback (`127.0.0.1`)** environment with a mock packet generator.
>
> The reported figures represent **packet processing throughput** of the SDK (serialization, framing, CRC32 validation, and packet handling pipeline) under loopback conditions.
>
> They should **not** be interpreted as real-world network throughput.

### Local Loopback Benchmark

- **Packet Processing Throughput:** **~21.39 Million Packets/sec**
- **Packets Processed:** **1,000,000**
- **Concurrent Workers:** **50**
- **Execution Time:** **~46.74 ms**
- **Data Integrity:** **100%** (CRC32 Verification)
- **Error Rate:** **0.00%**

```text
================ STREAM LOAD TEST RESULTS ================
Total Packets Sent : 1000000
Time Elapsed       : 46.745208ms
Throughput         : 21392567.13 packets/sec
==========================================================
```

---

## 📦 Typical Use Cases

- UAV Telemetry Networks
- IoT Device Gateways
- Industrial Edge Computing
- Autonomous Robotics
- Custom Binary Communication Protocols
- High-Performance TCP Services
- Telemetry Collection Pipelines

---

## 🎯 Design Goals

- High Throughput
- Low Latency
- Modular Architecture
- Clean API Surface
- Idiomatic Go
- Minimal Memory Allocations
- Easy Integration
- Production-Oriented Design