# Aegis Core Engineering & Performance Design

## Key Design Decisions

### 1. Zero-Allocation Framing & Stream Reading
Traditional socket implementations use naive `conn.Read()` calls which lead to packet fragmentation and buffer bloat. Aegis uses `io.ReadFull` matched directly with the header's length prefix. This guarantees zero unnecessary heap allocations during frame reconstruction.

### 2. OOM (Out-Of-Memory) Mitigation
To protect production deployments from malicious or malformed packets (e.g., a header claiming a 4GB payload):
- `MaxPacketSize` limit is enforced BEFORE allocating any byte slices.
- If `Length > MaxPacketSize`, the engine drops the frame instantly without reading into RAM.

### 3. IEEE CRC32 Integrity Checking
Every frame is verified at the network boundary using hardware-accelerated CRC32 polynomial calculation. Damaged packets from noisy cellular or satellite networks (IoT/GPS tracking) are rejected at zero cost to the database layer.