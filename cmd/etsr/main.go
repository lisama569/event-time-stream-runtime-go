// Command etsr 是本地事件时间流处理引擎的命令行入口。
// 它从一个或多个本地文件并发摄取事件，做键控事件时间窗口聚合，
// 支持水位、迟到语义、分类异常观测与一致性检查点恢复。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lisama569/event-time-stream-runtime-go/stream"
)

func main() {
	var (
		inputs    = flag.String("inputs", "", "逗号分隔的本地输入文件路径，可多个")
		out       = flag.String("out", "", "窗口结果输出文件（JSON Lines）；提供检查点时启动会按已提交状态重建。默认 stdout（崩溃恢复可能重复，仅演示用）")
		ckptPath  = flag.String("checkpoint", "", "检查点文件路径；提供则启用崩溃恢复（原子写、严格校验）")
		windowSec = flag.Int("window", 60, "窗口长度（秒）")
		lateSec   = flag.Int("lateness", 10, "允许迟到（秒）：水位越过 window_end+lateness 后窗口最终关闭")
		queueSize = flag.Int("queue", 1024, "每源有界摄取队列容量（背压粒度）")
		every     = flag.Int("every", 1000, "每处理多少条有效事件做一次检查点")
		maxAhead  = flag.Int("max-ahead", 0, "事件时间相对全局水位最大超前秒数，超出判 OUT_OF_RANGE；0=不限制")
		retention = flag.Int("retention", 0, "已发布结果在状态/检查点中的保留秒数（事件时间，关闭视界之后）；0=永久保留。长跑场景建议设为非 0 以约束状态体积")
		statusSec = flag.Int("status-interval", 0, "运行中每隔多少秒向 stderr 打印一次状态摘要；0=不打印")
		errLog    = flag.String("errors", "", "分类异常输出文件（JSON Lines 风格文本），默认 stderr")
	)
	flag.Parse()

	if *inputs == "" {
		fmt.Fprintln(os.Stderr, "missing -inputs")
		flag.Usage()
		os.Exit(2)
	}

	// 异常观测输出。
	var errW io.Writer = os.Stderr
	if *errLog != "" {
		f, err := os.OpenFile(*errLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open error log: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		errW = f
	}

	cfg := stream.DefaultConfig()
	cfg.WindowSize = time.Duration(*windowSec) * time.Second
	cfg.AllowedLateness = time.Duration(*lateSec) * time.Second
	cfg.SourceQueueSize = *queueSize
	cfg.CheckpointEvery = int64(*every)
	cfg.CheckpointPath = *ckptPath
	if *maxAhead > 0 {
		cfg.MaxOutofOrder = time.Duration(*maxAhead) * time.Second
	}
	if *retention > 0 {
		cfg.EmittedRetention = time.Duration(*retention) * time.Second
	}

	paths := strings.Split(*inputs, ",")
	var sources []stream.Source
	var files []*stream.FileSource
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		src, err := stream.NewFileSource(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open input %s: %v\n", p, err)
			os.Exit(1)
		}
		sources = append(sources, src)
		files = append(files, src)
	}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	// 汇：有检查点且指定输出文件时用提交式文件汇（崩溃后不重不漏）。
	var sink stream.Sink
	if *ckptPath != "" && *out != "" {
		fs, err := stream.NewCommittedFileSink(*out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open output: %v\n", err)
			os.Exit(1)
		}
		sink = fs
	} else if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open output: %v\n", err)
			os.Exit(1)
		}
		sink = stream.NewJSONSink(f)
	} else {
		sink = stream.NewJSONSink(os.Stdout)
	}
	defer sink.Close()

	onError := func(ce *stream.ClassifiedError) {
		fmt.Fprintln(errW, ce.Error())
	}

	eng := stream.NewEngine(cfg, onError)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 运行状态摘要：只读快照，不影响摄入。
	if *statusSec > 0 {
		go func() {
			tick := time.NewTicker(time.Duration(*statusSec) * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					s := eng.Status()
					fmt.Fprintf(os.Stderr,
						"status: active_windows=%d closed=%d retained=%d ingested=%d accepted=%d late=%d dup=%d malformed=%d sources=",
						s.ActiveWindows, s.WindowsClosed, s.EmittedRetained,
						s.Ingested, s.Accepted, s.LateDropped, s.Duplicates, s.Malformed)
					for _, ss := range s.Sources {
						fmt.Fprintf(os.Stderr, "[%s off=%d backlog=%d/%d done=%v] ",
							ss.Name, ss.Offset, ss.Backlog, ss.QueueCap, ss.Finished)
					}
					fmt.Fprintln(os.Stderr)
				}
			}
		}()
	}

	if err := eng.Run(ctx, sources, sink); err != nil {
		var ce *stream.CheckpointError
		if errors.As(err, &ce) {
			fmt.Fprintf(os.Stderr, "checkpoint rejected: kind=%s path=%s reason=%s\n",
				ce.Kind, ce.Path, ce.Reason)
			os.Exit(3)
		}
		fmt.Fprintf(os.Stderr, "engine run failed: %v\n", err)
		os.Exit(1)
	}

	st := eng.Report()
	fmt.Fprintf(os.Stderr,
		"done: ingested=%d accepted=%d malformed=%d invalid_ts=%d out_of_range=%d late_dropped=%d duplicates=%d windows=%d checkpoints=%d\n",
		st.Ingested, st.Accepted, st.Malformed, st.InvalidTS, st.OutOfRange,
		st.LateDropped, st.Duplicates, st.WindowsEmitted, st.Checkpoints)
}
