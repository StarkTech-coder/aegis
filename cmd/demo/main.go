package main

import (
	"aegis/pkg/aegis"
	"fmt"
	"log"
	"time"
)

func main() {
	fmt.Println("=== AEGIS CORE v0.2 PRO-ARCHITECTURE ENGINE ===")

	cfg := aegis.DefaultConfig()

	server, err := aegis.NewServer(cfg)
	if err != nil {
		log.Fatalf("[-] Critical: Server initialization failed: %v", err)
	}

	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("[-] Critical: Server failure: %v", err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	client, err := aegis.NewClient(cfg)
	if err != nil {
		log.Fatalf("[-] Critical: Client initialization failed: %v", err)
	}

	// 4. Connect and send actual binary packets instead of plain strings!
	conn, err := client.Connect(cfg.Address)
	if err != nil {
		log.Fatalf("[-] Critical: Client connection failure: %v", err)
	}

	// Create a real protocol packet to fire at our new server engine
	telemetryPacket := aegis.NewPacket([]byte("DRONE_ALTITUDE:450m_SPEED:22knots"))
	serializedData, err := telemetryPacket.Serialize()
	if err != nil {
		log.Fatalf("[-] Critical: Serialization failed: %v", err)
	}

	_, err = conn.Write(serializedData)
	if err != nil {
		log.Fatalf("[-] Critical: Failed to transmit packet: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	_ = conn.Close()

	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Critical: Failed to stop server gracefully: %v", err)
	}

	fmt.Println("=== SIMULATION TERMINATED CLEANLY WITH SLOG PROFILES ===")
}
