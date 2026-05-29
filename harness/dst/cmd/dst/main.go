package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
	"go.etcd.io/raft/v3"

	"github.com/caisc2026/harness/dst"
)

// silentLogger discards every log call raft makes. The default raft logger
// writes INFO-level messages to stderr on every state change, which drowns
// out the harness's own output during sweeps.
type silentLogger struct{}

func (silentLogger) Debug(...any)             {}
func (silentLogger) Debugf(string, ...any)    {}
func (silentLogger) Info(...any)              {}
func (silentLogger) Infof(string, ...any)     {}
func (silentLogger) Warning(...any)           {}
func (silentLogger) Warningf(string, ...any)  {}
func (silentLogger) Error(...any)             {}
func (silentLogger) Errorf(string, ...any)    {}
func (silentLogger) Fatal(v ...any)           { log.New(io.Discard, "", 0).Fatal(v...) }
func (silentLogger) Fatalf(f string, v ...any) {
	log.New(io.Discard, "", 0).Fatalf(f, v...)
}
func (silentLogger) Panic(v ...any)            { log.New(io.Discard, "", 0).Panic(v...) }
func (silentLogger) Panicf(f string, v ...any) { log.New(io.Discard, "", 0).Panicf(f, v...) }

func main() {
	seed := flag.Int64("seed", 1, "RNG seed")
	ticks := flag.Int("ticks", 5000, "logical ticks to simulate")
	nodes := flag.Int("nodes", 3, "cluster size")
	verbose := flag.Bool("v", false, "print per-100-tick progress")
	quiet := flag.Bool("quiet", true, "silence raft's internal INFO logger")
	linCheck := flag.Bool("lin-check", false, "after the run, check client history for linearizability")
	linTimeout := flag.Duration("lin-timeout", 30*time.Second, "wall-clock limit for the linearizability check")
	flag.Parse()

	if *quiet {
		raft.SetLogger(silentLogger{})
	}
	if os.Getenv("DST_DEBUG_CLIENT") != "" {
		dst.DebugClient = true
	}

	c, err := dst.NewCluster(*seed, *nodes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL setup seed=%d err=%v\n", *seed, err)
		os.Exit(2)
	}
	s := dst.DefaultScenario(c)

	for i := 0; i < *ticks; i++ {
		s.Drive(c)
		if err := c.Step(); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL step seed=%d tick=%d err=%v\n", *seed, c.Tick(), err)
			os.Exit(2)
		}
		if v := c.CheckInvariants(); v != nil {
			fmt.Printf("FAIL seed=%d %s\n", *seed, v)
			os.Exit(1)
		}
		if *verbose && c.Tick()%100 == 0 {
			fmt.Fprintf(os.Stderr, "  seed=%d %s\n", *seed, c.Summary())
		}
	}

	fmt.Printf("OK   seed=%d %s\n", *seed, c.Summary())

	if *linCheck {
		h := &dst.History{}
		for _, cl := range c.Clients() {
			h.Add(cl.History)
		}
		linearizable, done, nOps := h.Check(*linTimeout)
		switch {
		case !done:
			fmt.Printf("LIN  seed=%d UNKNOWN (timeout after %s, ops=%d)\n", *seed, *linTimeout, nOps)
			os.Exit(3)
		case !linearizable:
			fmt.Printf("LIN  seed=%d FAIL (ops=%d)\n", *seed, nOps)
			if os.Getenv("DST_VIZ_PATH") != "" {
				_, info, ops := h.CheckVerbose(*linTimeout)
				_ = ops
				if err := porcupine.VisualizePath(dst.RegisterModel(), info, os.Getenv("DST_VIZ_PATH")); err == nil {
					fmt.Fprintf(os.Stderr, "  visualization written to %s\n", os.Getenv("DST_VIZ_PATH"))
				} else {
					fmt.Fprintf(os.Stderr, "  visualization failed: %v\n", err)
				}
			}
			if os.Getenv("DST_DUMP_HISTORY") != "" {
				for i, op := range h.Ops {
					kind := "W"
					if op.Kind == dst.OpRead {
						kind = "R"
					}
					ok := "ok"
					if !op.Ok {
						ok = "TIMEOUT"
					}
					fmt.Fprintf(os.Stderr, "  [%3d] cl=%d node=%d %s val=%d out=%d invoke=%d return=%d %s\n",
						i, op.ClientID, op.NodeID, kind, op.Value, op.Output,
						op.InvokeTick, op.ReturnTick, ok)
				}
			}
			os.Exit(1)
		default:
			fmt.Printf("LIN  seed=%d OK (ops=%d)\n", *seed, nOps)
		}
	}
}
