package amqpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	benchmarkBool     bool
	benchmarkConfig   amqp.Config
	benchmarkError    error
	benchmarkDuration time.Duration
)

func benchmarkClassificationErrors() []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{
		{name: "application", err: errors.New("application failure")},
		{name: "EOF", err: io.EOF},
		{name: "wrapped-EOF", err: fmt.Errorf("level 1: %w", fmt.Errorf("level 2: %w", io.EOF))},
		{name: "timeout", err: testTimeoutError{}},
		{name: "AMQP", err: &amqp.Error{Code: amqp.ConnectionForced}},
	}
}

func BenchmarkShouldRetry(b *testing.B) {
	for _, benchmark := range benchmarkClassificationErrors() {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkBool = shouldRetry(benchmark.err)
			}
		})
	}
}

func BenchmarkIsBadConnErr(b *testing.B) {
	for _, benchmark := range benchmarkClassificationErrors() {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkBool = isBadConnErr(benchmark.err)
			}
		})
	}
}

func BenchmarkCloneAMQPConfig(b *testing.B) {
	cfg := amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{Username: "guest", Password: "guest"},
		},
		Properties: amqp.Table{
			"product": "amqpx",
			"version": "benchmark",
		},
		TLSClientConfig: &tls.Config{ServerName: "rabbitmq.example"},
	}

	b.ReportAllocs()
	for b.Loop() {
		benchmarkConfig = cloneAMQPConfig(cfg)
	}
}

func BenchmarkRunCommandWithContext(b *testing.B) {
	b.Run("background", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkError = runCommandWithContext(
				context.Background(),
				func() error { return nil },
				func(time.Time) error { return nil },
			)
		}
	})

	b.Run("cancelable", func(b *testing.B) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b.ReportAllocs()
		for b.Loop() {
			benchmarkError = runCommandWithContext(
				ctx,
				func() error { return nil },
				func(time.Time) error { return nil },
			)
		}
	})
}

func BenchmarkRetryBackoff(b *testing.B) {
	for _, retry := range []int{0, 4, 16} {
		b.Run(fmt.Sprintf("retry-%d", retry), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkDuration = retryBackoff(retry, 8*time.Millisecond, 512*time.Millisecond)
			}
		})
	}
}
