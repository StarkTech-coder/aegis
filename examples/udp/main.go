package main

import (
	"context"
	"fmt"
	"github.com/StarkTech-coder/aegis/pkg/aegis"
	"log"
	"net"
	"time"
)

type udpEchoHandler struct{}

func (udpEchoHandler) HandlePacket(_ []byte) error {
	return nil
}

func (udpEchoHandler) HandleUDPPacket(payload []byte, _ *net.UDPAddr) ([]byte, error) {
	return append([]byte("ACK:"), payload...), nil
}

func main() {
	fmt.Println("=== Aegis SDK: UDP Server & Client Example ===")

	serverCfg := aegis.DefaultConfig()
	serverCfg.Protocol = aegis.ProtocolUDP
	serverCfg.Address = "127.0.0.1:9091"
	serverCfg.MaxPayloadSize = 1024

	server, err := aegis.NewServer(serverCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create UDP server: %v", err)
	}

	server.SetHandler(udpEchoHandler{})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	clientCfg := aegis.DefaultConfig()
	clientCfg.Protocol = aegis.ProtocolUDP
	clientCfg.Address = serverCfg.Address
	clientCfg.MaxPayloadSize = 1024

	client, err := aegis.NewClient(clientCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create UDP client: %v", err)
	}

	conn, err := client.Connect(serverCfg.Address)
	if err != nil {
		log.Fatalf("[-] UDP client connect failed: %v", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	messages := [][]byte{
		[]byte("REQUEST_1"),
		[]byte("REQUEST_2"),
		[]byte("REQUEST_3"),
	}

	for _, msg := range messages {
		if err := client.SendPacket(context.Background(), conn, msg); err != nil {
			log.Fatalf("[-] Failed to send UDP message %q: %v", string(msg), err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			log.Fatalf("[-] Failed to receive UDP response for %q: %v", string(msg), err)
		}

		fmt.Printf("[+] UDP request: %s | response: %s\n", string(msg), string(resp))
	}

	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Failed to stop UDP server: %v", err)
	}

	if err := <-serverDone; err != nil {
		log.Fatalf("[-] UDP server loop exited with error: %v", err)
	}

	fmt.Println("=== UDP example completed cleanly ===")
}
