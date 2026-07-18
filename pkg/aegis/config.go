package aegis

import "time"

// Config defines the network engine parameters for the Aegis node.
type Config struct {
	// Address is the target host and port (e.g., ":8080" or "127.0.0.1:8080")
	Address      string        

	// ReadTimeout limits the maximum duration allowed to read an incoming packet.
	ReadTimeout  time.Duration 

	// WriteTimeout limits the maximum duration allowed to send an outgoing packet.
	WriteTimeout time.Duration 
}

// DefaultConfig initializes the engine with reliable, standard network values.
func DefaultConfig() *Config {
	return &Config{
		Address:      ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}