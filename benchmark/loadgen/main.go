// Minimal HTTP load generator (stdlib only) for benchmarking the OpenResty
// endpoints that sit in front of the gRPC module.
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/unary", "target URL")
	conc := flag.Int("c", 16, "concurrency")
	dur := flag.Duration("d", 10*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 2*time.Second, "warmup duration (excluded from stats)")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	target := flag.Int("n", 0, "stop after this many successful responses (0 = run for the full duration)")
	flag.Parse()

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *conc * 2,
			MaxIdleConnsPerHost: *conc * 2,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	endWarmup := time.Now().Add(*warmup)
	endMeasure := endWarmup.Add(*dur)

	var (
		counters     [8]atomic.Int64 // 0: ok, 1: non-200, 2: transport error, 3: other
		latencies    = make([][]float64, *conc)
		stop         atomic.Bool
		wg           sync.WaitGroup
		maxInFlight  atomic.Int64
		totalStarted atomic.Int64
	)
	_ = maxInFlight

	worker := func(wi int) {
		defer wg.Done()
		lat := make([]float64, 0, 4096)
		for !stop.Load() {
			if time.Now().After(endMeasure) {
				break
			}
			if *target > 0 && counters[0].Load() >= int64(*target) {
				break
			}
			start := time.Now()
			resp, err := client.Get(*url)
			el := time.Since(start).Seconds() * 1e6
			afterWarmup := !start.Before(endWarmup)
			if err != nil {
				if afterWarmup {
					counters[2].Add(1)
				}
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				if afterWarmup {
					counters[1].Add(1)
				}
				continue
			}
			if afterWarmup {
				counters[0].Add(1)
				lat = append(lat, el)
			}
			totalStarted.Add(1)
		}
		latencies[wi] = lat
	}

	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go worker(i)
	}

	// Progress ticker to stderr so it does not pollute results.
	done := make(chan struct{})
	var lastTotal int64
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				ok, fail := counters[0].Load(), counters[1].Load()+counters[2].Load()
				var window int64
				if ok+fail > lastTotal {
					window = ok + fail - lastTotal
				}
				lastTotal = ok + fail
				fmt.Fprintf(os.Stderr, "  ... ok=%d fail=%d (last 2s: %d)\n", ok, fail, window)
			}
		}
	}()

	wg.Wait()
	close(done)
	stop.Store(true)

	all := make([]float64, 0, 1<<20)
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Float64s(all)

	n := len(all)
	if n == 0 {
		fmt.Printf("RESULT ok=0 non200=%d err=%d rps=0\n", counters[1].Load(), counters[2].Load())
		return
	}
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return all[idx]
	}
	sum := 0.0
	for _, v := range all {
		sum += v
	}
	measElapsed := time.Since(endWarmup).Seconds()
	if measElapsed > dur.Seconds() {
		measElapsed = dur.Seconds()
	}
	rps := float64(n) / measElapsed
	fmt.Printf("RESULT ok=%d non200=%d err=%d rps=%.1f dur_s=%.3f mean_us=%.1f p50_us=%.1f p90_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f\n",
		counters[0].Load(), counters[1].Load(), counters[2].Load(),
		rps, measElapsed, sum/float64(n), pct(50), pct(90), pct(99), pct(99.9), all[n-1])
}
