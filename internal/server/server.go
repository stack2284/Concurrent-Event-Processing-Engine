package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"eventengine/internal/engine"
	"eventengine/internal/event"
)

type ingestEvent struct {
	ID     uint64 `json:"id"`
	From   uint32 `json:"from"`
	To     uint32 `json:"to"`
	Amount int64  `json:"amount"`
}

type ingestResult struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
	Invalid    int `json:"invalid"`
	Throttled  int `json:"throttled"`
}

func Handler(e *engine.Engine) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /ingest", func(w http.ResponseWriter, r *http.Request) {
		var batch []ingestEvent
		r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var res ingestResult
		for _, b := range batch {
			switch err := e.TryIngest(event.Event{ID: b.ID, From: b.From, To: b.To, Amount: b.Amount}); {
			case err == nil:
				res.Accepted++
			case errors.Is(err, engine.ErrDuplicate):
				res.Duplicates++
			case errors.Is(err, engine.ErrBackpressure):
				res.Throttled++
			default:
				res.Invalid++
			}
		}
		status := http.StatusAccepted
		if res.Throttled > 0 {
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, res)
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		s := e.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "engine_accepted_total %d\n", s.Accepted)
		fmt.Fprintf(w, "engine_duplicates_total %d\n", s.Duplicates)
		fmt.Fprintf(w, "engine_invalid_total %d\n", s.Invalid)
		fmt.Fprintf(w, "engine_backpressure_total %d\n", s.Backpressure)
		fmt.Fprintf(w, "engine_committed_total %d\n", s.Committed)
		fmt.Fprintf(w, "engine_rejected_total %d\n", s.Rejected)
		fmt.Fprintf(w, "engine_retried_total %d\n", s.Retried)
		fmt.Fprintf(w, "engine_dead_lettered_total %d\n", s.DeadLettered)
		fmt.Fprintf(w, "engine_compensated_total %d\n", s.Compensated)
		fmt.Fprintf(w, "engine_pending %d\n", s.Pending)
		fmt.Fprintf(w, "engine_queue_depth %d\n", s.QueueDepth)
		fmt.Fprintf(w, "engine_latency_seconds{quantile=\"0.5\"} %g\n", s.Latency.P50.Seconds())
		fmt.Fprintf(w, "engine_latency_seconds{quantile=\"0.99\"} %g\n", s.Latency.P99.Seconds())
		fmt.Fprintf(w, "engine_latency_seconds{quantile=\"0.999\"} %g\n", s.Latency.P999.Seconds())
	})

	mux.HandleFunc("GET /traces", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 20
		}
		writeJSON(w, http.StatusOK, e.Traces(limit))
	})

	mux.HandleFunc("GET /deadletters", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, e.DeadLetters())
	})

	mux.HandleFunc("POST /deadletters/replay", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]int{"replayed": e.ReplayDeadLetters()})
	})

	mux.HandleFunc("GET /balance/{account}", func(w http.ResponseWriter, r *http.Request) {
		a, err := strconv.ParseUint(r.PathValue("account"), 10, 32)
		if err != nil || int(a) >= e.Ledger().Size() {
			http.Error(w, "unknown account", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"balance": e.Ledger().Balance(uint32(a))})
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
