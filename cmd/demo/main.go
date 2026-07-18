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

	// 1. Standart ağ konfigürasyonunu çekiyoruz
	cfg := aegis.DefaultConfig()

	// 2. Çekirdek sunucumuzu (Server) SDK üzerinden başlatıyoruz
	server := aegis.NewServer(cfg)

	// Sunucu bloklayan (blocking) bir yapıda olduğu için onu ayrı bir iş parçacığında (Goroutine) ayağa kaldırıyoruz
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("[-] Critical: Server failure: %v", err)
		}
	}()

	// Sunucunun işletim sistemi portuna tamamen yerleşmesi için yarım saniye bekliyoruz
	time.Sleep(500 * time.Millisecond)

	fmt.Println("[SIMULATION] Initializing outbound node connection...")

	// 3. İstemci (Client) motorumuzu oluşturuyoruz
	client := aegis.NewClient(cfg)

	// 4. Sunucumuzun dinlediği adrese bağlanıyoruz
	conn, err := client.Connect(cfg.Address)
	if err != nil {
		log.Fatalf("[-] Critical: Client connection failure: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 5. Sunucunun gönderdiği el sıkışma (handshake) mesajını hattan okuyoruz
	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("[-] Critical: Failed to read handshake packet: %v", err)
	}

	// 6. Kutsal el sıkışma sonucunu ekrana yazdırıyoruz
	fmt.Printf("[SIMULATION] Handshake Protocol Verification Success!\n Received Packet: %s", message)

	// 7. Simülasyon bitti, sunucuyu güvenli bir şekilde kapatıyoruz
	fmt.Println("[SIMULATION] Terminating active engine components...")
	if err := server.Stop(); err != nil {
		log.Fatalf("[-] Critical: Failed to stop server gracefully: %v", err)
	}

	fmt.Println("=== SIMULATION TERMINATED CLEANLY ===")
}
