# Aegis Core Engine 🛡️

![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-orange)

**Aegis** is a high-performance, asynchronous TCP network SDK and custom binary protocol framework written in idiomatic Go. Designed for ultra-low latency, high-throughput scenarios such as telemetry ingestion, IoT hubs, and custom RPC transport layers.

---

## Key Features 🚀

* **Custom Binary Protocol**: Minimal frame overhead with magic byte detection, payload size prefixes, and hardware-friendly CRC32 data integrity verification.
* **Decoupled Architecture**: Exposes a polymorphic `PacketHandler` interface, allowing any business logic layer to consume decoded payloads seamlessly.
* **Resource Exhaustion Safeguards**: Dynamic connection pool isolation using buffered semaphore channels (`MaxConnections` limit enforcement).
* **Zero-Allocation Buffering**: Leverages `bufio.Reader` (64KB chunks) to drastically decrease kernel syscall overhead under high load.
* **Context-Aware Lifecycles**: Fully supports graceful shutdowns and connection flushes via standard `context.Context` signals.
![alt text](image.png)
---

## System Architecture

```text
 Client (TCP Stream) ──> [ Magic Bytes + CRC32 Check ] ──> Aegis Engine ──> PacketHandler Interface ──> Application Domain
Quick Start 🛠️
Usage Example
Go
package main

import (
	"fmt"
	"log"
	"aegis/pkg/aegis"
)

func main() {
	cfg := aegis.DefaultConfig()
	cfg.Address = ":9090"

	server, err := aegis.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize Aegis: %v", err)
	}

	// Register event listener callback or interface
	server.OnPacket(func(payload []byte) error {
		fmt.Printf("Received payload (%d bytes)\n", len(payload))
		return nil
	})

	log.Println("Starting Aegis Engine...")
	if err := server.Start(); err != nil {
		log.Fatalf("Runtime error: %v", err)
	}
}
License
Distributed under the MIT License. See LICENSE for more information.