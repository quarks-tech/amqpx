package amqpx

import (
	"testing"
	"time"
)

func TestConfigCompleteDefaultsDrainTimeout(t *testing.T) {
	cfg := &Config{}
	cfg.complete()
	if cfg.DrainTimeout != 30*time.Second {
		t.Fatalf("DrainTimeout = %v, want 30s default", cfg.DrainTimeout)
	}
}

func TestConfigCompleteKeepsNegativeDrainTimeout(t *testing.T) {
	cfg := &Config{DrainTimeout: -1}
	cfg.complete()
	if cfg.DrainTimeout != -1 {
		t.Fatalf("DrainTimeout = %v, want -1 (wait forever) preserved", cfg.DrainTimeout)
	}
}

func TestConfigCompleteKeepsExplicitDrainTimeout(t *testing.T) {
	cfg := &Config{DrainTimeout: 5 * time.Second}
	cfg.complete()
	if cfg.DrainTimeout != 5*time.Second {
		t.Fatalf("DrainTimeout = %v, want 5s preserved", cfg.DrainTimeout)
	}
}
