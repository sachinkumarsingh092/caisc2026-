package dst

import (
	"math/rand"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Network is a deterministic, single-goroutine in-memory message bus.
// All randomness is sourced from a seeded *rand.Rand owned by the cluster,
// not from package-level rand or time.Now(), so a given seed produces an
// identical run.
type Network struct {
	rng *rand.Rand

	// partitioned[id] == true means node id is disconnected from all peers.
	// Messages addressed to or from a partitioned node are dropped.
	partitioned map[uint64]bool

	// dropRate[link] in [0,1] is the per-message drop probability for
	// directed link from->to. Defaults to 0.
	dropRate map[link]float64

	// inbox[id] is a FIFO of pending messages for node id.
	inbox map[uint64][]pb.Message
}

type link struct {
	from, to uint64
}

func NewNetwork(rng *rand.Rand, ids []uint64) *Network {
	n := &Network{
		rng:         rng,
		partitioned: make(map[uint64]bool),
		dropRate:    make(map[link]float64),
		inbox:       make(map[uint64][]pb.Message),
	}
	for _, id := range ids {
		n.inbox[id] = nil
	}
	return n
}

// Send hands a message to the network. The network may drop, delay, or
// deliver it depending on current fault state.
func (n *Network) Send(m pb.Message) {
	from, to := m.GetFrom(), m.GetTo()
	if n.partitioned[from] || n.partitioned[to] {
		return
	}
	if r := n.dropRate[link{from, to}]; r > 0 && n.rng.Float64() < r {
		return
	}
	n.inbox[to] = append(n.inbox[to], m)
}

// Drain returns and clears all pending messages for node id.
func (n *Network) Drain(id uint64) []pb.Message {
	ms := n.inbox[id]
	n.inbox[id] = nil
	return ms
}

// Partition disconnects node id from the cluster. Idempotent.
func (n *Network) Partition(id uint64) {
	n.partitioned[id] = true
	// Drop any pending messages addressed to or from id.
	n.inbox[id] = nil
}

// Heal reconnects node id. Idempotent.
func (n *Network) Heal(id uint64) {
	n.partitioned[id] = false
}

func (n *Network) IsPartitioned(id uint64) bool {
	return n.partitioned[id]
}

func (n *Network) SetDrop(from, to uint64, rate float64) {
	n.dropRate[link{from, to}] = rate
}
