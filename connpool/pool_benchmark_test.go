package connpool

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var benchmarkStatsSink *Stats

type benchmarkPoolMode struct {
	name string
	fifo bool
}

type benchmarkIdleSize struct {
	name string
	size int
}

var benchmarkPoolModes = []benchmarkPoolMode{
	{name: "LIFO", fifo: false},
	{name: "FIFO", fifo: true},
}

var benchmarkIdleSizes = []benchmarkIdleSize{
	{name: "idle=1", size: 1},
	{name: "idle=32", size: 32},
	{name: "idle=256", size: 256},
}

func newBenchmarkPool(b *testing.B, size int, fifo bool) *ConnPool {
	b.Helper()

	p := New(&Options{
		PoolFIFO:    fifo,
		PoolSize:    size,
		PoolTimeout: time.Hour,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	ctx := context.Background()
	conns := make([]*Conn, size)
	for i := range conns {
		cn, err := p.Get(ctx)
		if err != nil {
			b.Fatalf("prewarm Get() error = %v", err)
		}
		cn.closeFunc = func() error { return nil }
		conns[i] = cn
	}
	for _, cn := range conns {
		p.Put(ctx, cn)
	}
	b.Cleanup(func() { _ = p.Close() })
	return p
}

func BenchmarkConnPoolGetPut(b *testing.B) {
	ctx := context.Background()
	for _, mode := range benchmarkPoolModes {
		b.Run(mode.name, func(b *testing.B) {
			for _, idle := range benchmarkIdleSizes {
				b.Run(idle.name, func(b *testing.B) {
					p := newBenchmarkPool(b, idle.size, mode.fifo)
					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						cn, err := p.Get(ctx)
						if err != nil {
							b.Fatalf("Get() error = %v", err)
						}
						p.Put(ctx, cn)
					}
				})
			}
		})
	}
}

func BenchmarkConnPoolGetPutParallel(b *testing.B) {
	ctx := context.Background()
	for _, mode := range benchmarkPoolModes {
		b.Run(mode.name, func(b *testing.B) {
			p := newBenchmarkPool(b, 256, mode.fifo)
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := p.Get(ctx)
					if err != nil {
						b.Errorf("Get() error = %v", err)
						return
					}
					p.Put(ctx, cn)
				}
			})
		})
	}
}

func BenchmarkConnPoolStats(b *testing.B) {
	p := newBenchmarkPool(b, 256, false)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		benchmarkStatsSink = p.Stats()
	}
}
