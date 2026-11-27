//go:build linux

package block

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Each worker owns its pages; contention comes from the shared tracker mutex.
func BenchmarkTrackerParallel(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		for _, sample := range []bool{false, true} {
			b.Run(fmt.Sprintf("workers=%d/sampling=%t", workers, sample), func(b *testing.B) {
				tr := NewMemoryTracker(4096)
				benchmarkTrackerParallel(b, tr, func() { tr.MemoryUsage() }, workers, sample)
			})
		}
	}
}

func benchmarkTrackerParallel(b *testing.B, tr *Tracker, snapshot func(), workers int, sample bool) {
	b.Helper()
	b.StopTimer()
	const residentPages = uint32(1 << 17)
	const cyclingPages = uint32(1 << 17)
	tr.SetRange(0, residentPages, Clean)
	tr.SetRange(residentPages, residentPages+cyclingPages, Removed)
	pagesPerWorker := cyclingPages / uint32(workers)
	start := make(chan struct{})
	stop := make(chan struct{})
	sampled := make(chan struct{})
	var samples int64
	var sampleTime time.Duration
	var maxSampleTime time.Duration
	if sample {
		go func() {
			defer close(sampled)
			<-start
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					begun := time.Now()
					snapshot()
					took := time.Since(begun)
					sampleTime += took
					maxSampleTime = max(maxSampleTime, took)
					samples++
				}
			}
		}()
	} else {
		close(sampled)
	}
	var wg sync.WaitGroup
	for worker := range workers {
		iterations := b.N / workers
		if worker < b.N%workers {
			iterations++
		}
		firstPage := residentPages + uint32(worker)*pagesPerWorker
		wg.Go(func() {
			<-start
			for i := range iterations {
				page := firstPage + uint32(i)%pagesPerWorker
				tr.MarkInstalled(page, page+1, Clean)
				tr.MarkWritten(page)
				tr.SetRange(page, page+1, Removed)
			}
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	close(stop)
	<-sampled
	if sample {
		b.ReportMetric(float64(samples), "samples")
		if samples > 0 {
			b.ReportMetric(float64(sampleTime.Nanoseconds())/float64(samples), "ns/sample")
			b.ReportMetric(float64(maxSampleTime.Nanoseconds()), "max-ns/sample")
		}
	}
}
