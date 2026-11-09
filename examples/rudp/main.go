package main

import (
	"context"
	"fmt"
	"github.com/StarkTech-coder/aegis/pkg/aegis"
	"log"
	"net"
	"time"
)

type rudpExampleHandler struct{}

func (rudpExampleHandler) HandlePacket(_ []byte) error {
	return nil
}

func (rudpExampleHandler) HandleRUDPPacket(payload []byte, _ *net.UDPAddr) ([]byte, error) {
	return append([]byte("RACK:"), payload...), nil
}

func main() {
	fmt.Println("=== Aegis SDK: RUDP Server & Client Example ===")

	serverCfg := aegis.DefaultConfig()
	serverCfg.Protocol = aegis.ProtocolRUDP
	serverCfg.Address = "127.0.0.1:9092"
	serverCfg.MaxPayloadSize = 2048
	serverCfg.RUDPInitialRetransmitTimeout = 40 * time.Millisecond
	serverCfg.RUDPMaxRetransmitTimeout = 300 * time.Millisecond
	serverCfg.RUDPMaxRetries = 5
	serverCfg.RUDPMaxInFlight = 32
	serverCfg.RUDPReceiveQueueSize = 32

	server, err := aegis.NewServer(serverCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create RUDP server: %v", err)
	}
	server.SetHandler(rudpExampleHandler{})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start()
	}()

	time.Sleep(120 * time.Millisecond)

	clientCfg := aegis.DefaultConfig()
	clientCfg.Protocol = aegis.ProtocolRUDP
	clientCfg.Address = serverCfg.Address
	clientCfg.MaxPayloadSize = 2048
	clientCfg.RUDPInitialRetransmitTimeout = 40 * time.Millisecond
	clientCfg.RUDPMaxRetransmitTimeout = 300 * time.Millisecond
	clientCfg.RUDPMaxRetries = 5
	clientCfg.RUDPMaxInFlight = 32
	clientCfg.RUDPReceiveQueueSize = 32

	client, err := aegis.NewClient(clientCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create RUDP client: %v", err)
	}

	conn, err := client.Connect(serverCfg.Address)
	if err != nil {
		log.Fatalf("[-] RUDP client connect failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	messages := [][]byte{
		[]byte("RUDP_REQ_1"),
		[]byte("RUDP_REQ_2"),
		[]byte("RUDP_REQ_3"),
	}

	for _, msg := range messages {
		if err := client.SendPacket(context.Background(), conn, msg); err != nil {
			log.Fatalf("[-] Failed to send RUDP message %q: %v", string(msg), err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			log.Fatalf("[-] Failed to receive RUDP response for %q: %v", string(msg), err)
		}

		fmt.Printf("[+] RUDP request: %s | response: %s\n", string(msg), string(resp))
	}

	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Failed to stop RUDP server: %v", err)
	}

	if err := <-serverDone; err != nil {
		log.Fatalf("[-] RUDP server loop exited with error: %v", err)
	}

	fmt.Println("=== RUDP example completed cleanly ===")
}
