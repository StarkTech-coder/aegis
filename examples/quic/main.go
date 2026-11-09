package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/StarkTech-coder/aegis/pkg/aegis"
)

type quicEchoHandler struct{}

func (quicEchoHandler) HandlePacket(_ []byte) error {
	return nil
}

func (quicEchoHandler) HandleQUICPacket(payload []byte, _ net.Addr) ([]byte, error) {
	return append([]byte("QACK:"), payload...), nil
}

func printCertInstructions() {
	fmt.Println("Set these environment variables before running:")
	fmt.Println("  AEGIS_QUIC_CERT=/path/to/server.crt")
	fmt.Println("  AEGIS_QUIC_KEY=/path/to/server.key")
	fmt.Println("  AEGIS_QUIC_CA=/path/to/ca.crt")
	fmt.Println()
	fmt.Println("Example development certificate generation:")
	fmt.Println("  openssl req -x509 -newkey rsa:2048 -sha256 -nodes \\")
	fmt.Println("    -keyout quic-dev.key -out quic-dev.crt -days 365 \\")
	fmt.Println("    -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'")
}

func main() {
	fmt.Println("=== Aegis SDK: QUIC Server & Client Example ===")

	certFile := os.Getenv("AEGIS_QUIC_CERT")
	keyFile := os.Getenv("AEGIS_QUIC_KEY")
	caFile := os.Getenv("AEGIS_QUIC_CA")

	if certFile == "" || keyFile == "" || caFile == "" {
		fmt.Println("[-] Missing required TLS environment variables.")
		printCertInstructions()
		os.Exit(1)
	}

	serverCfg := aegis.DefaultConfig()
	serverCfg.Protocol = aegis.ProtocolQUIC
	serverCfg.Address = "127.0.0.1:9093"
	serverCfg.MaxPayloadSize = 2048
	serverCfg.QUICCertFile = certFile
	serverCfg.QUICKeyFile = keyFile
	serverCfg.QUICALPN = "aegis-quic-v1"

	server, err := aegis.NewServer(serverCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create QUIC server: %v", err)
	}
	server.SetHandler(quicEchoHandler{})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start()
	}()
	defer func() {
		if err := server.Stop(); err != nil {
			log.Printf("[-] Failed to stop QUIC server cleanly: %v", err)
		}
		if err := <-serverDone; err != nil {
			log.Printf("[-] QUIC server loop exited with error: %v", err)
		}
	}()

	time.Sleep(150 * time.Millisecond)

	clientCfg := aegis.DefaultConfig()
	clientCfg.Protocol = aegis.ProtocolQUIC
	clientCfg.Address = serverCfg.Address
	clientCfg.MaxPayloadSize = 2048
	clientCfg.QUICCACertFile = caFile
	clientCfg.QUICServerName = "localhost"
	clientCfg.QUICALPN = "aegis-quic-v1"

	client, err := aegis.NewClient(clientCfg)
	if err != nil {
		log.Fatalf("[-] Failed to create QUIC client: %v", err)
	}

	conn, err := client.Connect(serverCfg.Address)
	if err != nil {
		log.Fatalf("[-] QUIC client connect failed: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("[-] Failed to close QUIC client connection: %v", err)
		}
	}()

	messages := [][]byte{
		[]byte("QUIC_REQ_1"),
		[]byte("QUIC_REQ_2"),
	}

	for _, msg := range messages {
		if err := client.SendPacket(context.Background(), conn, msg); err != nil {
			log.Fatalf("[-] Failed to send QUIC message %q: %v", string(msg), err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.ReceivePacket(ctx, conn)
		cancel()
		if err != nil {
			log.Fatalf("[-] Failed to receive QUIC response for %q: %v", string(msg), err)
		}

		fmt.Printf("[+] QUIC request: %s | response: %s\n", string(msg), string(resp))
	}

	fmt.Println("=== QUIC example completed cleanly ===")
}
