package main

import (
	"aegis/pkg/aegis"
	"bufio"
	"fmt"
	"log"
	"time"
)

func main() {
	fmt.Println("=== AEGIS CORE v0.1 SIMULATION MAIN ENGINE ===")

	// 1. Load the default network configuration parameters
	cfg := aegis.DefaultConfig()

	// 2. Initialize the core network server instance via the SDK
	server := aegis.NewServer(cfg)

	// Spin up the server engine inside a concurrent goroutine since Start() is a blocking call
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("[-] Critical: Server failure: %v", err)
		}
	}()

	// Allow a brief propagation delay for the server socket to bind cleanly to the OS port
	time.Sleep(500 * time.Millisecond)

	fmt.Println("[SIMULATION] Initializing outbound node connection...")

	// 3. Initialize the core network client instance via the SDK
	client := aegis.NewClient(cfg)

	// 4. Establish a secure outbound connection to the target node address
	conn, err := client.Connect(cfg.Address)
	if err != nil {
		log.Fatalf("[-] Critical: Client connection failure: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 5. Read the mandatory protocol-level handshake packet sent by the remote host
	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("[-] Critical: Failed to read handshake packet: %v", err)
	}

	// 6. Verify and display the successful handshake result
	fmt.Printf("[SIMULATION] Handshake Protocol Verification Success!\n Received Packet: %s", message)

	// 7. Gracefully terminate active network engine components to preserve clean state
	fmt.Println("[SIMULATION] Terminating active engine components...")
	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Critical: Failed to stop server gracefully: %v", err)
	}

	fmt.Println("=== SIMULATION TERMINATED CLEANLY ===")
}
