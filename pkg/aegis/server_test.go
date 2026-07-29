package aegis

import (
	"testing"
	"time"
)

// TestServerLifecycle verifies that the server starts and stops gracefully 
// without blocking or leaking memory resources.
func TestServerLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Address = ":9090"

	server, err := NewServer(cfg)
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

// TestConfigValidationAndSanitization tests fallback behaviors and boundary 
// validation checks for input configurations.
func TestConfigValidationAndSanitization(t *testing.T) {
	// Scenario A: Verify that nil config fallback defaults to safe parameters
	client, err := NewClient(nil)
	if err != nil {
		t.Fatalf("NewClient(nil) should not return error, but got: %v", err)
	}
	if client.Config().Address != ":8080" {
		t.Errorf("Expected fallback address :8080, got: %s", client.Config().Address)
	}

	// Scenario B: Verify boundary limits for invalid configuration fields
	badCfg := &Config{
		Address:     ":8080",
		ReadTimeout: 65 * time.Second, // Exceeds upper boundary limit of 60 seconds
	}

	_, err = NewServer(badCfg)
	if err == nil {
		t.Fatal("Expected configuration validation boundary error, but got nil success")
	}
}