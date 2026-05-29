package dst

import (
	"math/rand"
	"sort"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Network is a deterministic, single-goroutine in-memory message bus.
// All randomness is sourced from a seeded *rand.Rand owned by the cluster,
// not from package-level rand or time.Now(), so a given seed produces an
// identical run.
//
// Faults supported:
//   - Partition: a node is disconnected from the cluster; messages to or
//     from it are dropped (and any already-pending messages addressed to
//     it are discarded on partition).
//   - Per-link drop: a configurable per-direction probability of dropping
//     a message when it is Sent.
//   - Delay / reorder: each sent message is scheduled for delivery at
//     curTick + Intn(maxDelay+1). The Drain operation returns only
//     messages whose readyTick has arrived, sorted by (readyTick, seq).
//     With maxDelay > 0, messages from a given sender may be reordered
//     relative to send order; with maxDelay == 0 the bus behaves as a
//     strict FIFO per receiver.
//   - Duplication: each Send may, with probability pDup, enqueue an
//     additional copy of the message (with an extra +1-tick delay) so
//     receivers see the message twice.
type Network struct {
	rng *rand.Rand

	// partitioned[id] == true means node id is disconnected from all peers.
	partitioned map[uint64]bool

	// dropRate[link] in [0,1] is the per-message drop probability for
	// directed link from->to. Defaults to 0.
	dropRate map[link]float64

	// inbox[id] is a FIFO of pending messages for node id, each tagged
	// with its scheduled delivery tick.
	inbox map[uint64][]pendingMsg

	// maxDelay is the upper bound on per-message delivery delay (ticks).
	// 0 means FIFO with same-tick delivery.
	maxDelay int

	// pDup is the per-send duplication probability in [0, 1].
	pDup float64

	// curTick is the network's view of logical time, advanced once per
	// Cluster.Step before any Send/Drain calls of that tick.
	curTick uint64

	// nextSeq is a monotonic tie-breaker so messages with identical
	// readyTick still get a stable order.
	nextSeq uint64
}

type link struct {
	from, to uint64
}

type pendingMsg struct {
	msg       pb.Message
	readyTick uint64
	seq       uint64
}

func NewNetwork(rng *rand.Rand, ids []uint64) *Network {
	n := &Network{
		rng:         rng,
		partitioned: make(map[uint64]bool),
		dropRate:    make(map[link]float64),
		inbox:       make(map[uint64][]pendingMsg),
	}
	for _, id := range ids {
		n.inbox[id] = nil
	}
	return n
}

// AdvanceTick is called once at the start of each cluster tick.
func (n *Network) AdvanceTick(t uint64) { n.curTick = t }

// SetMaxDelay enables message reordering with up to `d` ticks of delay
// per message. d=0 restores strict FIFO behaviour.
func (n *Network) SetMaxDelay(d int) {
	if d < 0 {
		d = 0
	}
	n.maxDelay = d
}

// SetDuplicateRate sets the per-send duplication probability.
func (n *Network) SetDuplicateRate(p float64) {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	n.pDup = p
}

// Send hands a message to the network. The network may drop, delay,
// duplicate, or deliver it depending on current fault state.
func (n *Network) Send(m pb.Message) {
	from, to := m.GetFrom(), m.GetTo()
	if n.partitioned[from] || n.partitioned[to] {
		return
	}
	if r := n.dropRate[link{from, to}]; r > 0 && n.rng.Float64() < r {
		return
	}
	delay := uint64(0)
	if n.maxDelay > 0 {
		delay = uint64(n.rng.Intn(n.maxDelay + 1))
	}
	n.nextSeq++
	n.inbox[to] = append(n.inbox[to], pendingMsg{
		msg:       m,
		readyTick: n.curTick + delay,
		seq:       n.nextSeq,
	})
	if n.pDup > 0 && n.rng.Float64() < n.pDup {
		n.nextSeq++
		n.inbox[to] = append(n.inbox[to], pendingMsg{
			msg:       m,
			readyTick: n.curTick + delay + 1,
			seq:       n.nextSeq,
		})
	}
}

// Drain returns and removes all pending messages for node id whose
// scheduled delivery tick has arrived, sorted canonically.
func (n *Network) Drain(id uint64) []pb.Message {
	pending := n.inbox[id]
	if len(pending) == 0 {
		return nil
	}
	out := pending[:0:0]
	remain := pending[:0:0]
	for _, pm := range pending {
		if pm.readyTick <= n.curTick {
			out = append(out, pm)
		} else {
			remain = append(remain, pm)
		}
	}
	n.inbox[id] = remain
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].readyTick != out[j].readyTick {
			return out[i].readyTick < out[j].readyTick
		}
		return out[i].seq < out[j].seq
	})
	res := make([]pb.Message, len(out))
	for i, pm := range out {
		res[i] = pm.msg
	}
	return res
}

// Partition disconnects node id from the cluster. Idempotent.
// Any messages addressed to id are discarded immediately.
func (n *Network) Partition(id uint64) {
	n.partitioned[id] = true
	n.inbox[id] = nil
}

// Heal reconnects node id. Idempotent.
func (n *Network) Heal(id uint64) { n.partitioned[id] = false }

func (n *Network) IsPartitioned(id uint64) bool { return n.partitioned[id] }

func (n *Network) SetDrop(from, to uint64, rate float64) {
	n.dropRate[link{from, to}] = rate
}
