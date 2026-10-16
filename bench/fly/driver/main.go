// The Canalis benchmark driver. `driver serve` is the mock target API
// (POST /work answers at once, or after ?ms=) and webhook sink (POST
// /hook). `driver run` and `driver ramp` send open-loop load at a fixed
// rate and print a JSON summary.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: driver serve | run [flags] | ramp [flags]")
	}
	switch os.Args[1] {
	case "serve":
		serve()
	case "run", "ramp":
		load(os.Args[1], os.Args[2:])
	case "resolve":
		// Every address a host resolves to, one per line (for reading each
		// Canalis node's own /metrics).
		addrs, err := net.LookupHost(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		sort.Strings(addrs)
		for _, a := range addrs {
			fmt.Println(a)
		}
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}

func serve() {
	var work, hooks atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if ms := r.URL.Query().Get("ms"); ms != "" {
			var d int
			fmt.Sscan(ms, &d)
			time.Sleep(time.Duration(d) * time.Millisecond)
		}
		work.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		hooks.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"work": work.Load(), "hooks": hooks.Load()})
	})
	log.Fatal(http.ListenAndServe(":9000", mux))
}

type summary struct {
	Label     string           `json:"label"`
	Rate      float64          `json:"offered_rps"`
	Seconds   float64          `json:"seconds"`
	Sent      int64            `json:"sent"`
	Achieved  float64          `json:"completed_rps"`
	Status    map[string]int64 `json:"status"`
	Queued    int64            `json:"queued_streams"`
	Errors    map[string]int64 `json:"errors,omitempty"`
	Skipped   int64            `json:"skipped_over_inflight_cap"`
	P50, P90  float64
	P99, P999 float64
	Max       float64
	Timeline  []second `json:"timeline,omitempty"`
}

type second struct {
	T      int   `json:"t"`
	OK     int64 `json:"ok"`
	Failed int64 `json:"failed"`
}

func load(mode string, args []string) {
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	target := fs.String("target", "", "base URL to send to, e.g. http://canalis-bench.internal:8080")
	spread := fs.Bool("spread", false, "round-robin across every address the target host resolves to")
	path := fs.String("path", "/proxy", "/proxy or /jobs")
	work := fs.String("work", "http://canalis-bench-driver.internal:9000/work", "the target API URL jobs point at")
	hook := fs.String("hook", "http://canalis-bench-driver.internal:9000/hook", "webhook_url")
	rate := fs.Float64("rate", 100, "requests per second (run)")
	rates := fs.String("rates", "100,200,400", "comma-separated steps (ramp)")
	duration := fs.Duration("duration", 30*time.Second, "how long to send (each step for ramp)")
	users := fs.Int("users", 100, "distinct user_ids, cycled")
	timeout := fs.Duration("timeout", 60*time.Second, "per-request timeout")
	maxInflight := fs.Int64("max-inflight", 20000, "skip sends past this many in flight")
	timeline := fs.Bool("timeline", false, "include per-second ok/failed counts")
	label := fs.String("label", "", "label for the summary")
	fs.Parse(args)
	if *target == "" {
		log.Fatal("-target is required")
	}
	bases := []string{strings.TrimRight(*target, "/")}
	if *spread {
		bases = resolveAll(*target)
	}
	client := &http.Client{Timeout: *timeout, Transport: &http.Transport{
		MaxIdleConns: 20000, MaxIdleConnsPerHost: 20000, IdleConnTimeout: 90 * time.Second,
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}}
	steps := []float64{*rate}
	if mode == "ramp" {
		steps = nil
		for _, s := range strings.Split(*rates, ",") {
			var r float64
			fmt.Sscan(strings.TrimSpace(s), &r)
			steps = append(steps, r)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	for _, r := range steps {
		s := step(client, bases, *path, *work, *hook, r, *duration, *users, *maxInflight, *timeline)
		s.Label = *label
		enc.Encode(s)
	}
}

func resolveAll(target string) []string {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatal(err)
	}
	addrs, err := net.LookupHost(u.Hostname())
	if err != nil || len(addrs) == 0 {
		log.Fatalf("resolve %s: %v", u.Hostname(), err)
	}
	sort.Strings(addrs)
	var out []string
	for _, a := range addrs {
		out = append(out, fmt.Sprintf("%s://%s", u.Scheme, net.JoinHostPort(a, u.Port())))
	}
	log.Printf("spreading across %v", out)
	return out
}

func randomKey() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func step(client *http.Client, bases []string, path, work, hook string, rate float64, duration time.Duration, users int, maxInflight int64, withTimeline bool) summary {
	var (
		mu        sync.Mutex
		latencies []float64
		status    = map[string]int64{}
		errs      = map[string]int64{}
		sent      int64
		queued    int64
		skipped   int64
		inflight  atomic.Int64
		wg        sync.WaitGroup
		buckets   = make([]second, int(duration.Seconds())+120)
	)
	start := time.Now()
	interval := time.Duration(float64(time.Second) / rate)
	next := start
	for i := 0; time.Since(start) < duration; i++ {
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}
		next = next.Add(interval)
		if inflight.Load() >= maxInflight {
			skipped++
			continue
		}
		sent++
		inflight.Add(1)
		wg.Add(1)
		body, _ := json.Marshal(map[string]any{
			"user_id": fmt.Sprintf("u-%d", i%users), "idempotent_key": randomKey(),
			"url": work, "method": "POST", "webhook_url": hook,
		})
		base := bases[i%len(bases)]
		go func() {
			defer wg.Done()
			defer inflight.Add(-1)
			t0 := time.Now()
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, base+path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			var code string
			sse := false
			if err == nil {
				sse = strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				code = fmt.Sprint(resp.StatusCode)
			}
			ms := float64(time.Since(t0).Microseconds()) / 1000
			sec := int(time.Since(start).Seconds())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				msg := err.Error()
				if len(msg) > 80 {
					msg = msg[len(msg)-80:]
				}
				errs[msg]++
			} else {
				status[code]++
				if sse {
					queued++
				}
				if code[0] == '2' {
					latencies = append(latencies, ms)
				}
			}
			if sec < len(buckets) {
				if err == nil && code[0] == '2' {
					buckets[sec].OK++
				} else {
					buckets[sec].Failed++
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	sort.Float64s(latencies)
	pct := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		i := int(p * float64(len(latencies)-1))
		return latencies[i]
	}
	s := summary{Rate: rate, Seconds: elapsed, Sent: sent, Status: status, Queued: queued, Errors: errs, Skipped: skipped,
		Achieved: float64(len(latencies)) / duration.Seconds(),
		P50:      pct(0.50), P90: pct(0.90), P99: pct(0.99), P999: pct(0.999), Max: pct(1)}
	if withTimeline {
		for i := range buckets {
			buckets[i].T = i
			if i < int(elapsed)+1 {
				s.Timeline = append(s.Timeline, buckets[i])
			}
		}
	}
	return s
}
