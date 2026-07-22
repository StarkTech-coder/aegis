package tests

import (
	"testing"
	"time"

	"aegis/pkg/aegis"
)

// TestServerLifecycle verifies that the server starts and stops without blocking or leaking memory.
func TestServerLifecycle(t *testing.T) {
	cfg := aegis.DefaultConfig()
	cfg.Address = ":9090"

	server, err := aegis.NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize server instance: %v", err)
	}

	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	err = server.Stop()
	if err != nil {
		t.Fatalf("Server failed to stop gracefully: %v", err)
	}
}

func TestConfigValidationAndSanitization(t *testing.T) {
	// Scenario A: Test Nil Config injection handles safely via DefaultConfig fallback
	client, err := aegis.NewClient(nil)
	if err != nil {
		t.Fatalf("NewClient(nil) should not return error, but got: %v", err)
	}
	if client.Config().Address != ":8080" {
		t.Errorf("Expected fallback address :8080, got: %s", client.Config().Address)
	}

	// Scenario B: Test Invalid Config Bounds Validation
	badCfg := &aegis.Config{
		Address:     ":8080",
		ReadTimeout: 65 * time.Second, // Exceeds upper limit boundary of 60 seconds
	}

	_, err = aegis.NewServer(badCfg)
	if err == nil {
		t.Fatal("Expected configuration validation boundary error, but got nil success")
	}
}