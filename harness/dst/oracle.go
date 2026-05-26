package dst

import (
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Violation is the result of a failed invariant check.
type Violation struct {
	Kind   string
	Detail string
	Tick   uint64
}

func (v *Violation) String() string {
	if v == nil {
		return "<none>"
	}
	return fmt.Sprintf("tick=%d kind=%s detail=%s", v.Tick, v.Kind, v.Detail)
}

// CheckInvariants returns the first invariant violation observed, or nil.
//
// Properties checked (Raft safety properties, minus those that only hold in
// expectation across nodes after agreement, which need Layer 3):
//
//  1. Log-prefix agreement: for any two nodes i,j and any index k that is
//     present in both n_i.committed and n_j.committed, the entries must be
//     identical (same term, same data).
//
//  2. Per-term leader uniqueness: at any tick, at most one node may report
//     RaftState == StateLeader at a given term. We approximate this by
//     reading the cached soft state from the last Ready; it is a snapshot
//     of the most recent transition, not the live state, so this is a
//     necessary-but-not-sufficient check.
func (c *Cluster) CheckInvariants() *Violation {
	if v := c.checkLogPrefix(); v != nil {
		return v
	}
	if v := c.checkLeaderUniqueness(); v != nil {
		return v
	}
	return nil
}

func (c *Cluster) checkLogPrefix() *Violation {
	type idxKey struct {
		idx uint64
	}
	// For each committed index seen at any node, remember the first
	// (term, data) we saw and check all subsequent nodes agree.
	type seen struct {
		nodeID uint64
		term   uint64
		data   []byte
	}
	first := map[idxKey]seen{}

	for _, n := range c.Nodes() {
		for _, e := range n.committed {
			// Skip empty entries (Raft uses them for no-op leader heartbeats
			// after election; they carry an index but no payload, and both
			// nodes will see the same empty payload, so no special handling
			// is needed — but we still want to compare terms).
			key := idxKey{idx: e.GetIndex()}
			if prev, ok := first[key]; ok {
				if prev.term != e.GetTerm() || !bytesEqual(prev.data, e.GetData()) {
					return &Violation{
						Kind: "LogPrefixDivergence",
						Detail: fmt.Sprintf(
							"index=%d node=%d term=%d data=%q vs node=%d term=%d data=%q",
							e.GetIndex(),
							prev.nodeID, prev.term, prev.data,
							n.id, e.GetTerm(), e.GetData(),
						),
						Tick: c.tick,
					}
				}
			} else {
				first[key] = seen{nodeID: n.id, term: e.GetTerm(), data: e.GetData()}
			}
		}
	}
	return nil
}

func (c *Cluster) checkLeaderUniqueness() *Violation {
	type k struct{ term uint64 }
	byTerm := map[k]uint64{}
	for _, n := range c.Nodes() {
		if n.stateStr != "StateLeader" || n.term == 0 {
			continue
		}
		if prev, ok := byTerm[k{n.term}]; ok && prev != n.id {
			return &Violation{
				Kind:   "MultipleLeadersInTerm",
				Detail: fmt.Sprintf("term=%d leaders=%d,%d", n.term, prev, n.id),
				Tick:   c.tick,
			}
		}
		byTerm[k{n.term}] = n.id
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
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

// Summary returns a one-line cluster snapshot for the run log.
func (c *Cluster) Summary() string {
	leaders := 0
	committedCounts := make([]int, 0, len(c.ids))
	for _, n := range c.Nodes() {
		if n.stateStr == "StateLeader" {
			leaders++
		}
		committedCounts = append(committedCounts, len(n.committed))
	}
	_ = pb.EntryNormal // silence unused import if pb stripped later
	return fmt.Sprintf("tick=%d leaders=%d committed_per_node=%v", c.tick, leaders, committedCounts)
}
