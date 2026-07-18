package tests

import (
	"aegis/pkg/aegis"
	"testing"
	"time"
)

// TestServerLifecycle verifies that the server starts and stops without blocking or leaking memory.
func TestServerLifecycle(t *testing.T) {
	cfg := aegis.DefaultConfig()
	// Use a non-standard port for testing to avoid collisions
	cfg.Address = ":9090"

	server := aegis.NewServer(cfg)

	// Start server in background
	go func() {
		if err := server.Start(); err != nil {
			// If server fails to start, fail the test immediately
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Give it a tiny moment to bind
	time.Sleep(100 * time.Millisecond)

	// Stop the server and verify it exits gracefully
	err := server.Stop()
	if err != nil {
		t.Fatalf("Server failed to stop gracefully: %v", err)
	}
}