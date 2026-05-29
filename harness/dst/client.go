package dst

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
)

// DebugClient prints client lifecycle events when set to true. Set via
// the DST_DEBUG_CLIENT env var on the main binary.
var DebugClient = false

func clog(format string, args ...any) {
	if DebugClient {
		fmt.Fprintf(os.Stderr, "[client] "+format+"\n", args...)
	}
}

// OpKind enumerates the client operations we model: a write and a
// linearizable read (via ReadIndex).
type OpKind int

const (
	OpWrite OpKind = iota
	OpRead
)

// Op is one client operation, with timestamps in logical ticks. ReturnTick
// remains 0 while the op is still in flight; the history-export code uses
// that sentinel to assign Return = MaxInt to pending ops for Porcupine.
type Op struct {
	OpID       uint64
	ClientID   int
	NodeID     uint64 // node the op was issued through
	Kind       OpKind
	Value      uint64 // write value (input)
	Output     uint64 // read value (output)
	Ok         bool   // true iff the op completed (committed for write,
	//                read-index resolved and applied for read)
	InvokeTick uint64
	ReturnTick uint64
}

// Client models a single application client. At most one op is in flight
// per client at any time, which keeps history bookkeeping straightforward.
// Each client is biased toward a preferred node so the protocol exercises
// both leader and follower forwarding paths.
type Client struct {
	id      int
	prefer  uint64 // preferred node id
	rng     *rand.Rand
	cluster *Cluster

	nextOpID uint64
	nextVal  uint64

	pending *Op

	// For reads: the requestCtx passed to raft.ReadIndex, the safe-index
	// returned via Ready.ReadStates, and the node the read targets (set
	// at MaybeIssue time so pollRead reads the same node's register).
	readCtx       []byte
	readSafeIndex uint64
	readNode      uint64

	History []Op
}

func NewClient(id int, prefer uint64, c *Cluster) *Client {
	// Derive a per-client RNG from the cluster's RNG so different clients
	// make different choices but the run as a whole remains deterministic.
	return &Client{
		id:      id,
		prefer:  prefer,
		cluster: c,
		rng:     rand.New(rand.NewSource(c.rng.Int63())),
	}
}

// Tick expires the current pending op if it has been outstanding for
// longer than maxOpTicks; the op is recorded as Ok=false and the client
// becomes free to issue again. This models real-world client timeouts
// and prevents the client from getting permanently stuck after a lost
// proposal or a ReadIndex that the cluster never resolved (e.g., issued
// during a leadership gap).
func (cl *Client) Tick(maxOpTicks uint64) {
	if cl.pending == nil {
		return
	}
	if cl.cluster.Tick()-cl.pending.InvokeTick < maxOpTicks {
		return
	}
	cl.pending.ReturnTick = cl.cluster.Tick()
	cl.pending.Ok = false
	cl.History = append(cl.History, *cl.pending)
	cl.pending = nil
	cl.readCtx = nil
	cl.readSafeIndex = 0
}

// MaybeIssue issues a fresh op if no op is currently pending. pWrite is the
// probability of choosing a write versus a read on each issuance.
func (cl *Client) MaybeIssue(pWrite float64) {
	if cl.pending != nil {
		clog("skip cl=%d (pending opID=%d kind=%d)", cl.id, cl.pending.OpID, cl.pending.Kind)
		return
	}
	n := cl.cluster.Node(cl.prefer)
	if n == nil || cl.cluster.Net().IsPartitioned(cl.prefer) {
		clog("skip cl=%d (node %d down/partitioned)", cl.id, cl.prefer)
		return
	}

	cl.nextOpID++
	op := &Op{
		OpID:       cl.nextOpID,
		ClientID:   cl.id,
		NodeID:     cl.prefer,
		InvokeTick: cl.cluster.Tick(),
	}

	if cl.rng.Float64() < pWrite {
		cl.nextVal++
		op.Kind = OpWrite
		op.Value = cl.nextVal
		if err := n.raw.Propose(EncodeWrite(op.OpID, op.Value)); err != nil {
			clog("propose failed cl=%d node=%d opID=%d err=%v", cl.id, cl.prefer, op.OpID, err)
			return
		}
		clog("propose ok cl=%d node=%d opID=%d val=%d tick=%d", cl.id, cl.prefer, op.OpID, op.Value, cl.cluster.Tick())
		cl.pending = op
		return
	}

	op.Kind = OpRead
	ctx := make([]byte, 16)
	binary.BigEndian.PutUint64(ctx[0:8], uint64(cl.id))
	binary.BigEndian.PutUint64(ctx[8:16], op.OpID)
	cl.readCtx = ctx
	cl.readSafeIndex = 0
	cl.readNode = cl.prefer
	n.raw.ReadIndex(ctx)
	clog("readindex cl=%d node=%d opID=%d tick=%d", cl.id, cl.prefer, op.OpID, cl.cluster.Tick())
	cl.pending = op
}

// onWriteCommitted is called by the cluster when any node observes a
// committed write whose opID matches cl.pending. The first node to do so
// wins; the op is finalized at that tick.
func (cl *Client) onWriteCommitted(opID uint64, tick uint64) {
	if cl.pending == nil || cl.pending.Kind != OpWrite || cl.pending.OpID != opID {
		return
	}
	clog("write committed cl=%d opID=%d tick=%d (invoke=%d)", cl.id, opID, tick, cl.pending.InvokeTick)
	cl.pending.ReturnTick = tick
	cl.pending.Ok = true
	cl.History = append(cl.History, *cl.pending)
	cl.pending = nil
}

// onReadIndexReady is called when cl.prefer's Ready surfaces a ReadState
// whose RequestCtx matches the in-flight read context.
func (cl *Client) onReadIndexReady(reqCtx []byte, safeIndex uint64) {
	if cl.pending == nil || cl.pending.Kind != OpRead {
		return
	}
	if !bytesEqualBare(reqCtx, cl.readCtx) {
		return
	}
	cl.readSafeIndex = safeIndex
}

// pollRead finalizes a pending read once the preferred node's applied
// index has caught up to the safe index reported by raft.
func (cl *Client) pollRead(tick uint64) {
	if cl.pending == nil || cl.pending.Kind != OpRead || cl.readSafeIndex == 0 {
		return
	}
	n := cl.cluster.Node(cl.readNode)
	if n == nil {
		return
	}
	if n.appliedIndex >= cl.readSafeIndex {
		// Return the value at safeIndex, not the current register.
		// Walk down from safeIndex to find the snapshotted value. Since
		// every applied index is recorded, the very first iteration
		// usually hits.
		val := uint64(0)
		for idx := cl.readSafeIndex; idx > 0; idx-- {
			if v, ok := n.registerAt[idx]; ok {
				val = v
				break
			}
		}
		cl.pending.Output = val
		cl.pending.ReturnTick = tick
		cl.pending.Ok = true
		cl.History = append(cl.History, *cl.pending)
		cl.pending = nil
		cl.readCtx = nil
		cl.readSafeIndex = 0
		cl.readNode = 0
	}
}

func bytesEqualBare(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
