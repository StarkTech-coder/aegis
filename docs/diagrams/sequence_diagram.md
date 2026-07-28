# Aegis Protocol Sequence Diagrams

## 1. TCP Connection & Binary Packet Lifecycle

The diagram below illustrates the end-to-end packet transmission flow between an Aegis Client and the Aegis Core Engine:

```mermaid
sequenceDiagram
    autonumber
    participant Client as Aegis Client
    participant Server as Aegis Engine
    participant Handler as Connection Handler

    Client->>Server: Establish TCP Connection
    Server->>Handler: Spawn Worker Goroutine
    
    rect rgb(240, 240, 240)
        note over Client, Handler: Binary Protocol Frame Exchange
        Client->>Handler: Send Packet Header (Magic Bytes + Length + CRC32)
        Handler->>Handler: Validate Magic Bytes ('AG') & Memory Bounds
        Client->>Handler: Send Payload
        Handler->>Handler: Compute & Verify CRC32 Checksum
    end

    alt Valid Packet
        Handler->>Handler: Dispatch Telemetry Payload to Application
    else Integrity Failure / OOM Breach
        Handler->>Client: Close Socket / Log Error
    end

    Client->>Server: Initiate Graceful Close
    Server->>Handler: Signal Context Cancellation
    Handler-->>Client: Socket Closed Cleanly

