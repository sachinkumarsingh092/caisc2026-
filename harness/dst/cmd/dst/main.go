package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

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
	flag.Parse()

	if *quiet {
		raft.SetLogger(silentLogger{})
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
}
