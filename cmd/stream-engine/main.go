// Command stream-engine runs the local event-time stream processing engine.
//
// Usage:
//
//	stream-engine demo [--dir DIR] [--window 10s] [--lateness 5s]
//	                  [--crash-after N] [--restart]
//	stream-engine run  --inputs id=path[,id=path...] --dir DIR [flags]
//
// "demo" uses only generated in-memory data and a local checkpoint dir
// (default ./.checkpoint-demo). Run once with --crash-after to kill the
// process without a trailing checkpoint, then run again with --restart to
// observe recovery from the latest valid checkpoint and identical results.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lisama569/event-time-stream-runtime-go/internal/checkpoint"
	"github.com/lisama569/event-time-stream-runtime-go/internal/engine"
	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
	"github.com/lisama569/event-time-stream-runtime-go/internal/source"
)

func main() {
	os.Exit(runWithWriter(context.Background(), os.Args[1:], os.Stdout))
}

func run(ctx context.Context, args []string) int {
	return runWithWriter(ctx, args, os.Stdout)
}

func runWithWriter(ctx context.Context, args []string, out io.Writer) int {
	if len(args) == 0 {
		args = []string{"demo"}
	}
	switch args[0] {
	case "demo":
		return cmdDemo(ctx, args[1:], out)
	case "run":
		return cmdRun(ctx, args[1:], out)
	case "-h", "--help", "help":
		fmt.Fprintln(out, "usage: stream-engine [demo|run] [flags]")
		return 0
	default:
		fmt.Fprintf(out, "unknown command %q (expected demo|run)\n", args[0])
		return 2
	}
}

type commonFlags struct {
	dir       string
	window    time.Duration
	lateness  time.Duration
	ooo       time.Duration
	cpEvery   time.Duration
	maxEvents int64
}

func bindCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.dir, "dir", "./.checkpoint-demo", "checkpoint directory")
	fs.DurationVar(&c.window, "window", 10*time.Second, "event-time window size")
	fs.DurationVar(&c.lateness, "lateness", 5*time.Second, "allowed event-time lateness after window end")
	fs.DurationVar(&c.ooo, "ooo", 2*time.Second, "per-source watermark out-of-order lag")
	fs.DurationVar(&c.cpEvery, "checkpoint-every", 500*time.Millisecond, "checkpoint interval")
	fs.Int64Var(&c.maxEvents, "crash-after", 0, "stop after N envelopes WITHOUT a final checkpoint (simulated crash)")
	return c
}

func (c *commonFlags) toConfig() engine.Config {
	cfg := engine.DefaultConfig()
	cfg.WindowSize = c.window
	cfg.AllowedLateness = c.lateness
	cfg.OutOfOrderBound = c.ooo
	cfg.CheckpointInterval = c.cpEvery
	cfg.MaxEvents = c.maxEvents
	return cfg
}

// base is a fixed wall-clock-independent anchor so the demo is reproducible:
// 2026-09-23T10:00:00Z in unix milliseconds.
var base = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC).UnixMilli()

func ts(sec int64) string {
	return strconv.FormatInt(base+sec*1000, 10)
}

// demoData exercises: in-order, out-of-order, within-lateness, after-close,
// duplicate redelivery, malformed, bad timestamp, out-of-range timestamps.
func demoData() (map[string][]string, map[string]string) {
	// One source makes the narrative deterministic: the watermark is a pure
	// function of observed events. After ts=31 the watermark is 29 (31 - 2s
	// lag), so window [0,10) is finalized and the trailing ts=9 event is
	// dropped as too late. Concurrent multi-source ingestion is covered by the
	// Go test suite (go test ./...).
	lines := []string{
		`{"id":"a1","key":"k1","ts":` + ts(1) + `,"value":1}`,
		`{"id":"a3","key":"k1","ts":` + ts(3) + `,"value":2}`,
		`{"id":"a2","key":"k1","ts":` + ts(2) + `,"value":4}`, // out of order, window [0,10)
		`{"id":"b1","key":"k2","ts":` + ts(5) + `,"value":7}`,
		// same event id as the previous line -> suppressed as duplicate redelivery
		`{"id":"b1","key":"k2","ts":` + ts(6) + `,"value":99}`,
		`{"id":"a4","key":"k1","ts":` + ts(12) + `,"value":8}`,
		`{"id":"a6","key":"k1","ts":` + ts(11) + `,"value":1}`, // out of order, window [10,20)
		`{"id":"b3","key":"k2","ts":` + ts(15) + `,"value":20}`,
		`{"id":"a7","key":"k1","ts":` + ts(14) + `,"value":1}`,
		`{"id":"c1","key":"k3","ts":` + ts(33) + `,"value":3}`,
		`{"id":"a5","key":"k1","ts":` + ts(31) + `,"value":1}`,  // jump: wm=29, [0,10) finalized
		`{"id":"a9","key":"k1","ts":` + ts(9) + `,"value":100}`, // LATE -> late_dropped
		`not-json-at-all`, // malformed
		`{"id":"x1","key":"k2","ts":"not-a-time","value":1}`,        // bad timestamp
		`{"id":"x2","key":"k2","ts":-5,"value":1}`,                  // bad timestamp
		`{"id":"x3","key":"k2","ts":` + ts(1000000) + `,"value":1}`, // out of span
	}
	descr := map[string]string{
		"a9": "arrives after watermark passed window end+lateness -> late_dropped",
	}
	_ = descr
	return map[string][]string{"events": lines}, descr
}

func cmdDemo(ctx context.Context, argv []string, out io.Writer) int {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	c := bindCommon(fs)
	restart := fs.Bool("restart", false, "recover from existing checkpoint dir instead of wiping it")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	cfg := c.toConfig()
	// bound accepted times so the 1000000s-ahead event is classified out_of_span
	cfg.MinEventTime = time.UnixMilli(base).Add(-time.Minute)
	cfg.MaxEventTime = time.UnixMilli(base).Add(time.Hour)

	streams, _ := demoData()

	if !*restart {
		if err := os.RemoveAll(c.dir); err != nil {
			fmt.Fprintf(os.Stderr, "clean dir: %v\n", err)
			return 1
		}
		fmt.Println("== demo: cold start ==")
	} else {
		fmt.Println("== demo: restart, recovering from latest valid checkpoint ==")
	}

	return execute(ctx, cfg, c.dir, streams, out)
}

func cmdRun(ctx context.Context, argv []string, out io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	c := bindCommon(fs)
	inputs := fs.String("inputs", "", "comma-separated id=path source list")
	minStr := fs.String("min-time", "", "optional minimum event time RFC3339")
	maxStr := fs.String("max-time", "", "optional maximum event time RFC3339")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if *inputs == "" {
		fmt.Fprintln(os.Stderr, "--inputs id=path[,id=path...] required")
		return 2
	}
	cfg := c.toConfig()
	if *minStr != "" {
		t, err := time.Parse(time.RFC3339, *minStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad --min-time: %v\n", err)
			return 2
		}
		cfg.MinEventTime = t
	}
	if *maxStr != "" {
		t, err := time.Parse(time.RFC3339, *maxStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad --max-time: %v\n", err)
			return 2
		}
		cfg.MaxEventTime = t
	}
	paths := map[string]string{}
	for _, item := range strings.Split(*inputs, ",") {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			fmt.Fprintf(os.Stderr, "bad --inputs item %q\n", item)
			return 2
		}
		paths[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	readers, err := cfg.FileSources(paths)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open sources: %v\n", err)
		return 1
	}
	return executeReaders(ctx, cfg, c.dir, readers, out)
}

func execute(ctx context.Context, cfg engine.Config, dir string, streams map[string][]string, out io.Writer) int {
	return executeReaders(ctx, cfg, dir, cfg.LineSources(streams), out)
}

func executeReaders(ctx context.Context, cfg engine.Config, dir string, readers []source.Reader, out io.Writer) int {
	cpStore, err := checkpoint.NewStore(dir, cfg.EngineID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "checkpoint store: %v\n", err)
		return 1
	}
	sink := engine.NewLogSink(out)
	rejects := int64(0)
	onReject := func(r event.Rejected) {
		rejects++
		fmt.Fprintf(out, "REJECT source=%s seq=%d reason=%s detail=%q\n", r.SourceID, r.Seq, r.Reason, r.Detail)
	}
	eng, err := engine.New(cfg, readers, cpStore, sink, onReject)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new engine: %v\n", err)
		return 1
	}
	stats, err := eng.Run(ctx)
	if err != nil {
		var ce *checkpoint.CorruptionError
		if errors.As(err, &ce) {
			fmt.Fprintf(os.Stderr, "CHECKPOINT REJECTED kind=%s version=%d detail=%s\n", ce.Kind, ce.Version, ce.Detail)
		} else {
			fmt.Fprintf(os.Stderr, "engine run: %v\n", err)
		}
		return 1
	}
	fmt.Printf("STATS processed=%d duplicates=%d late_dropped=%d rejected=%d checkpoints=%d published=%d eof=%d watermarkEnd=%d\n",
		stats.Processed, stats.Duplicates, stats.LateDropped, stats.Rejected,
		stats.Checkpoints, stats.Published, stats.EOFSeen, stats.WatermarkEnd)
	if cfg.MaxEvents > 0 {
		fmt.Fprintf(out, "SIMULATED CRASH after %d envelopes (no trailing checkpoint); rerun with --restart\n", cfg.MaxEvents)
	}
	return 0
}

var _ event.Event
