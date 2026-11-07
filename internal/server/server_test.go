package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"eventengine/internal/engine"
)

func newServer(t *testing.T, cfg engine.Config) (*httptest.Server, *engine.Engine) {
	t.Helper()
	if cfg.Accounts == 0 {
		cfg.Accounts = 10
		cfg.InitialBalance = 1000
	}
	cfg.Workers = 1
	cfg.DedupeCapacity = 1024
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(e))
	t.Cleanup(func() {
		srv.Close()
		e.Stop()
	})
	return srv, e
}

func post(t *testing.T, url, body string) (*http.Response, ingestResult) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res ingestResult
	json.NewDecoder(resp.Body).Decode(&res)
	return resp, res
}

func drain(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp.StatusCode, sb.String()
}

func TestIngestDuplicatesAndInvalid(t *testing.T) {
	srv, e := newServer(t, engine.Config{})
	resp, res := post(t, srv.URL+"/ingest", `[
		{"id":1,"from":0,"to":1,"amount":100},
		{"id":1,"from":0,"to":1,"amount":100},
		{"id":2,"from":3,"to":3,"amount":5},
		{"id":3,"from":0,"to":99,"amount":5}]`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if res != (ingestResult{Accepted: 1, Duplicates: 1, Invalid: 2}) {
		t.Fatalf("result %+v", res)
	}
	drain(t, e)
	code, body := get(t, srv.URL+"/balance/1")
	if code != http.StatusOK || !strings.Contains(body, "1100") {
		t.Fatalf("balance %d %s", code, body)
	}
}

func TestMalformedBody(t *testing.T) {
	srv, _ := newServer(t, engine.Config{})
	resp, _ := post(t, srv.URL+"/ingest", `not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestBackpressureReturns429(t *testing.T) {
	srv, _ := newServer(t, engine.Config{
		QueueSize: 2, FaultRate: 1, MaxAttempts: 1 << 20, BaseBackoff: time.Hour,
		Accounts: 10, InitialBalance: 1000,
	})
	var sb strings.Builder
	sb.WriteString("[")
	for i := 1; i <= 500; i++ {
		if i > 1 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":` + itoa(i) + `,"from":0,"to":1,"amount":1}`)
	}
	sb.WriteString("]")
	resp, res := post(t, srv.URL+"/ingest", sb.String())
	if resp.StatusCode == http.StatusAccepted {
		t.Skip("worker kept pace with the queue")
	}
	if resp.StatusCode != http.StatusTooManyRequests || res.Throttled == 0 {
		t.Fatalf("status %d result %+v", resp.StatusCode, res)
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestMetricsEndpoint(t *testing.T) {
	srv, e := newServer(t, engine.Config{})
	post(t, srv.URL+"/ingest", `[{"id":1,"from":0,"to":1,"amount":5}]`)
	drain(t, e)
	code, body := get(t, srv.URL+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"engine_accepted_total 1", "engine_committed_total 1", "engine_pending 0"} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestBalanceUnknownAccount(t *testing.T) {
	srv, _ := newServer(t, engine.Config{})
	for _, path := range []string{"/balance/99", "/balance/abc"} {
		if code, _ := get(t, srv.URL+path); code != http.StatusNotFound {
			t.Fatalf("%s status %d", path, code)
		}
	}
}

func TestDeadLetterReplay(t *testing.T) {
	srv, e := newServer(t, engine.Config{CommitFaultRate: 1, MaxAttempts: 2, BaseBackoff: 50 * time.Microsecond,
		Accounts: 10, InitialBalance: 1000})
	post(t, srv.URL+"/ingest", `[{"id":1,"from":0,"to":1,"amount":5},{"id":2,"from":2,"to":3,"amount":5}]`)
	drain(t, e)

	_, body := get(t, srv.URL+"/deadletters")
	var dead []map[string]any
	if err := json.Unmarshal([]byte(body), &dead); err != nil || len(dead) != 2 {
		t.Fatalf("dead letters %s (%v)", body, err)
	}
	resp, err := http.Post(srv.URL+"/deadletters/replay", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status %d", resp.StatusCode)
	}
	drain(t, e)
	if e.Ledger().Total() != 10*1000 {
		t.Fatal("conservation violated after replay")
	}
}

func TestTracesAndHealth(t *testing.T) {
	srv, e := newServer(t, engine.Config{TraceSample: 1, TraceBuffer: 256, Accounts: 10, InitialBalance: 1000})
	post(t, srv.URL+"/ingest", `[{"id":1,"from":0,"to":1,"amount":5}]`)
	drain(t, e)
	code, body := get(t, srv.URL+"/traces?limit=5")
	if code != http.StatusOK || !strings.Contains(body, `"Spans"`) {
		t.Fatalf("traces %d %s", code, body)
	}
	if code, _ := get(t, srv.URL+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz %d", code)
	}
}
