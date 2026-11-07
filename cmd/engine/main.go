package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"text/tabwriter"
	"time"

	"eventengine/internal/bench"
	"eventengine/internal/engine"
	"eventengine/internal/server"
	"eventengine/internal/wal"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "bench":
		runBench(os.Args[2:])
	case "suite":
		runSuite(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: engine <bench|suite|serve> [flags]")
	os.Exit(2)
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	s := bench.Scenario{Name: "custom"}
	fs.IntVar(&s.Events, "events", 2_000_000, "unique events")
	fs.IntVar(&s.Accounts, "accounts", 100_000, "ledger accounts")
	fs.IntVar(&s.Workers, "workers", runtime.GOMAXPROCS(0), "worker count")
	fs.IntVar(&s.Producers, "producers", 4, "producer goroutines")
	fs.IntVar(&s.Queue, "queue", 8192, "per-worker queue size")
	fs.Float64Var(&s.DupRate, "dup", 0.1, "duplicate fraction")
	fs.Float64Var(&s.FaultRate, "fault", 0.02, "fault probability per stage")
	fs.Float64Var(&s.Skew, "skew", 0, "fraction of traffic on 10 hot accounts")
	fs.IntVar(&s.Rate, "rate", 0, "offered events/s, 0 for unbounded")
	fs.Uint64Var(&s.Seed, "seed", 1, "generator seed")
	fs.StringVar(&s.WAL, "wal", "", "write-ahead log mode: buffered, group or ack; empty disables")
	prof := fs.String("cpuprofile", "", "write cpu profile to file")
	fs.Parse(args)
	if *prof != "" {
		f, err := os.Create(*prof)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	report([]bench.Scenario{s})
}

func runSuite(args []string) {
	fs := flag.NewFlagSet("suite", flag.ExitOnError)
	events := fs.Int("events", 2_000_000, "unique events per scenario")
	fs.Parse(args)
	base := bench.Scenario{Events: *events, Accounts: 100_000, Workers: runtime.GOMAXPROCS(0), Queue: 8192}
	with := func(name string, f func(*bench.Scenario)) bench.Scenario {
		s := base
		s.Name = name
		f(&s)
		return s
	}
	list := []bench.Scenario{
		with("baseline", func(s *bench.Scenario) {}),
		with("dup10", func(s *bench.Scenario) { s.DupRate = 0.1 }),
		with("dup10+fault2", func(s *bench.Scenario) { s.DupRate = 0.1; s.FaultRate = 0.02 }),
		with("dup10+fault20", func(s *bench.Scenario) { s.DupRate = 0.1; s.FaultRate = 0.2 }),
		with("skew50", func(s *bench.Scenario) { s.DupRate = 0.1; s.FaultRate = 0.02; s.Skew = 0.5 }),
		with("paced300k", func(s *bench.Scenario) { s.DupRate = 0.1; s.FaultRate = 0.02; s.Rate = 300_000 }),
	}
	for _, m := range []string{"buffered", "group", "ack"} {
		m := m
		list = append(list, with("wal="+m, func(s *bench.Scenario) {
			s.WAL = m
			s.DupRate = 0.1
			s.FaultRate = 0.02
			if m == "ack" {
				s.Events = min(s.Events, 200_000)
			}
		}))
	}
	for _, w := range []int{1, 2, 4, 8, runtime.GOMAXPROCS(0)} {
		w := w
		list = append(list, with(fmt.Sprintf("workers=%d", w), func(s *bench.Scenario) {
			s.Workers = w
			s.DupRate = 0.1
			s.FaultRate = 0.02
		}))
	}
	report(list)
}

func report(list []bench.Scenario) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "scenario\tworkers\toffered\tevents/s\tp50\tp99\tp99.9\tdups\tretries\tcomp\tdlq\tallocs/ev\theap MB\trecovery\tconsistent")
	failed := false
	for _, s := range list {
		r, err := bench.Run(s)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.0f\t%v\t%v\t%v\t%d\t%d\t%d\t%d\t%.2f\t%.0f\t%s\t%v\n",
			r.Name, r.Workers, r.Offered, r.PerSec,
			r.P50.Round(time.Microsecond), r.P99.Round(time.Microsecond), r.P999.Round(time.Microsecond),
			r.Duplicates, r.Retried, r.Compensated, r.DeadLettered, r.AllocsPerEv, r.HeapMB, recoveryCell(r), r.Consistent)
		failed = failed || !r.Consistent
		tw.Flush()
	}
	if failed {
		os.Exit(1)
	}
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	var cfg engine.Config
	fs.IntVar(&cfg.Accounts, "accounts", 10_000, "ledger accounts")
	fs.Int64Var(&cfg.InitialBalance, "balance", 1_000_000, "initial balance per account")
	fs.IntVar(&cfg.Workers, "workers", runtime.GOMAXPROCS(0), "worker count")
	fs.IntVar(&cfg.QueueSize, "queue", 4096, "per-worker queue size")
	fs.IntVar(&cfg.DedupeCapacity, "dedupe", 1<<22, "dedupe capacity")
	fs.IntVar(&cfg.MaxAttempts, "attempts", 5, "max delivery attempts")
	fs.IntVar(&cfg.TraceSample, "trace", 256, "trace one in N events, 0 to disable")
	fs.IntVar(&cfg.TraceBuffer, "trace-buf", 1<<14, "trace span buffer")
	fs.Float64Var(&cfg.FaultRate, "fault", 0, "injected fault rate")
	fs.StringVar(&cfg.WALPath, "wal-path", "", "write-ahead log file, empty disables durability")
	mode := fs.String("wal-mode", "group", "write-ahead log mode: buffered, group or ack")
	fs.Parse(args)
	switch *mode {
	case "buffered":
		cfg.WALMode = wal.Buffered
	case "group":
		cfg.WALMode = wal.Group
	case "ack":
		cfg.WALMode = wal.Ack
	default:
		log.Fatalf("unknown wal mode %q", *mode)
	}
	cfg.CommitFaultRate = cfg.FaultRate

	e, err := engine.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if r := e.Recovery(); r.Records > 0 {
		log.Printf("recovered %d records in %v: %d committed, %d rejected, %d dead, %d requeued",
			r.Records, r.Elapsed.Round(time.Millisecond), r.Committed, r.Rejected, r.Dead, r.Requeued)
	}
	srv := &http.Server{Addr: *addr, Handler: server.Handler(e)}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		e.Drain(ctx)
		e.Stop()
	}()
	log.Printf("listening on %s", *addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func recoveryCell(r bench.Result) string {
	if r.Scenario.WAL == "" {
		return "-"
	}
	return fmt.Sprintf("%v/%dk rec", r.Recovery.Round(time.Millisecond), r.Recovered/1000)
}
