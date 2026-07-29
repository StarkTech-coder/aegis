package main

import (
	"aegis/pkg/aegis"
	"fmt"
	"log"
	"time"
)

func main() {
	fmt.Println("=== Aegis SDK: TCP Server & Client Example ===")

	// 1. Initialize custom runtime configuration
	cfg := aegis.DefaultConfig()
	cfg.Address = "127.0.0.1:9090"

	// 2. Instantiate and launch the Aegis TCP engine in a background goroutine
	server, err := aegis.NewServer(cfg)
	if err != nil {
		log.Fatalf("[-] Critical: Failed to initialize server: %v", err)
	}

	go func() {
		if err := server.Start(); err != nil {
			log.Printf("[-] Server runtime warning/failure: %v", err)
		}
	}()

	// Allow a brief buffer window for the listener socket to bind properly
	time.Sleep(100 * time.Millisecond)

	// 3. Initialize the Aegis client and open a network socket connection
	client, err := aegis.NewClient(cfg)
	if err != nil {
		log.Fatalf("[-] Critical: Failed to initialize client: %v", err)
	}

	conn, err := client.Connect(cfg.Address)
	if err != nil {
		log.Fatalf("[-] Critical: Client connection failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()
	// 4. Construct, serialize, and transmit a binary protocol packet over TCP
	telemetryData := []byte("DEVICE_ID:AEGIS_01|LAT:41.0082|LON:28.9784|STATUS:OK")
	packet := aegis.NewPacket(telemetryData)

	serialized, err := packet.Serialize()
	if err != nil {
		log.Fatalf("[-] Critical: Packet serialization failed: %v", err)
	}

	_, err = conn.Write(serialized)
	if err != nil {
		log.Fatalf("[-] Critical: Failed to write packet to network socket: %v", err)
	}

	fmt.Println("[+] Binary packet transmitted successfully to Aegis Engine!")

	// Wait briefly for the server connection loop to process and log the packet frame
	time.Sleep(200 * time.Millisecond)

	// 5. Initiate a graceful shutdown sequence to clean up resources and close listener
	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Critical: Failed to gracefully stop the engine: %v", err)
	}

	fmt.Println("=== Aegis TCP Simulation Completed Cleanly ===")
}
