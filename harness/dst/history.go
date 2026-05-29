package dst

import (
	"time"

	"github.com/anishathalye/porcupine"
)

// In the porcupine model the input/output types are:
//   Input:  registerIn{ Op: "W"|"R", Value: uint64 }
//   Output: registerOut{ Value: uint64, Ok: bool }
//
// Writes ignore their output; the input value determines the new state.
// Reads must return the current state when Ok=true. When Ok=false (the op
// never returned in our run), the model allows any outcome consistent
// with the op's input: writes either take effect or do not (modelled as
// "did take effect" since the entry might still be in the log past our
// horizon — see notes below), and reads place no constraint on state.

type registerIn struct {
	Op    string
	Value uint64
}

type registerOut struct {
	Value uint64
	Ok    bool
}

// registerModel is a sequential specification for a single-cell integer
// register initialized to 0.
var registerModel = porcupine.Model{
	Init: func() interface{} { return uint64(0) },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		i := in.(registerIn)
		o := out.(registerOut)
		switch i.Op {
		case "W":
			if !o.Ok {
				// Unreturned write: the write may or may not have happened.
				// Porcupine will, via backtracking, consider both linearization
				// points (before and after) when the op overlaps other ops.
				// We model this conservatively by accepting *either* outcome
				// — handled by returning two candidate states is not allowed
				// in this model API, so we accept the "took effect" branch
				// (the alternative no-op branch is implicitly explored by
				// porcupine reorganising adjacent ops). In practice for our
				// histories this only matters at the very tail of the run.
				return true, i.Value
			}
			return true, i.Value
		case "R":
			if !o.Ok {
				return true, state
			}
			return o.Value == state.(uint64), state
		}
		return false, state
	},
	Equal: func(a, b interface{}) bool {
		return a.(uint64) == b.(uint64)
	},
	DescribeOperation: func(in, out interface{}) string {
		i := in.(registerIn)
		o := out.(registerOut)
		switch i.Op {
		case "W":
			return "W(" + u64s(i.Value) + ")"
		case "R":
			if o.Ok {
				return "R() -> " + u64s(o.Value)
			}
			return "R() -> ?"
		}
		return "?"
	},
	DescribeState: func(s interface{}) string { return u64s(s.(uint64)) },
}

// RegisterModel exposes the unexported model for external visualisation.
func RegisterModel() porcupine.Model { return registerModel }

// History collects ops from all clients into a flat, ordered list.
type History struct {
	Ops []Op
}

func (h *History) Add(ops []Op) {
	h.Ops = append(h.Ops, ops...)
}

// ToOperations converts the recorded history to porcupine.Operation values.
//
// Timed-out operations (Ok=false) model "unknown outcome": the cluster may
// or may not have committed the write, and the read either resolved with an
// unobserved value or never resolved. Including them as Return=MaxInt64
// would force porcupine to linearize a write of an arbitrary value at some
// point, which over-constrains a history that may have nothing to do with
// the timed-out op. We therefore exclude them entirely. Note: this is
// conservative — a timed-out write whose effect a subsequent read actually
// observes still appears as a constraint via the read's input/output, so
// real linearizability violations downstream of a timeout are still caught.
//
// Reads that resolved (Ok=true) and writes that returned via raft commit
// (Ok=true) are always included.
//
// Pending ops at end of run (ReturnTick == 0, Ok==false) are excluded by the
// same rule.
func (h *History) ToOperations() []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(h.Ops))
	for _, op := range h.Ops {
		if !op.Ok {
			continue
		}
		in := registerIn{Value: op.Value}
		o := registerOut{Value: op.Output, Ok: op.Ok}
		if op.Kind == OpWrite {
			in.Op = "W"
		} else {
			in.Op = "R"
		}
		out = append(out, porcupine.Operation{
			ClientId: op.ClientID,
			Input:    in,
			Call:     int64(op.InvokeTick),
			Output:   o,
			Return:   int64(op.ReturnTick),
		})
	}
	return out
}

// Check runs porcupine on the recorded history with a wall-clock timeout.
// Returns linearizable, ranOps (true iff the check completed before
// timeout), and the number of ops.
func (h *History) Check(timeout time.Duration) (linearizable bool, completed bool, nOps int) {
	ops := h.ToOperations()
	res := porcupine.CheckOperationsTimeout(registerModel, ops, timeout)
	switch res {
	case porcupine.Ok:
		return true, true, len(ops)
	case porcupine.Illegal:
		return false, true, len(ops)
	case porcupine.Unknown:
		return false, false, len(ops)
	}
	return false, false, len(ops)
}

// CheckVerbose runs the same check but also returns porcupine's
// linearization-info, useful for offline visualisation when something
// fails.
func (h *History) CheckVerbose(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo, []porcupine.Operation) {
	ops := h.ToOperations()
	res, info := porcupine.CheckOperationsVerbose(registerModel, ops, timeout)
	return res, info, ops
}

func u64s(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
