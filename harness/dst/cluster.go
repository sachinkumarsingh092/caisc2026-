package dst

import (
	"fmt"
	"math/rand"
	"sort"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Node wraps a RawNode with its storage and an externally-observed
// committed-entry log used by the invariant checker.
type Node struct {
	id        uint64
	raw       *raft.RawNode
	storage   *raft.MemoryStorage
	committed []*pb.Entry // entries the cluster has reported as applied

	// State-machine state: a single integer register, updated as Write
	// entries are applied. appliedIndex tracks the highest applied index
	// at this node and is used by linearizable-read polling.
	//
	// registerAt records the register value immediately AFTER each
	// committed entry (writes, no-ops, conf-changes) is applied. A
	// linearizable read with safe index S must return the value at S,
	// not the latest value — because between the moment raft confirmed
	// the safe index and the moment our pollRead fires, more entries
	// may have applied, advancing the register past S. Looking up
	// registerAt[S] gives the value at the linearization point.
	register     uint64
	appliedIndex uint64
	registerAt   map[uint64]uint64

	// Per-tick observed soft state, cached so the oracle can read it cheaply.
	leaderID uint64
	term     uint64
	stateStr string
}

// Cluster is a deterministic in-memory Raft cluster. All scheduling lives in
// a single goroutine; the user calls Tick() once per logical tick.
type Cluster struct {
	rng   *rand.Rand
	net   *Network
	nodes map[uint64]*Node
	ids   []uint64 // stable iteration order
	tick  uint64

	// Clients registered via RegisterClient; the cluster routes commit and
	// read-index notifications to them inside Step.
	clients []*Client
}

func NewCluster(seed int64, size int) (*Cluster, error) {
	if size < 1 {
		return nil, fmt.Errorf("cluster size must be >= 1, got %d", size)
	}
	rng := rand.New(rand.NewSource(seed))

	ids := make([]uint64, size)
	peers := make([]raft.Peer, size)
	for i := 0; i < size; i++ {
		ids[i] = uint64(i + 1)
		peers[i] = raft.Peer{ID: ids[i]}
	}

	c := &Cluster{
		rng:   rng,
		net:   NewNetwork(rng, ids),
		nodes: make(map[uint64]*Node, size),
		ids:   ids,
	}

	for _, id := range ids {
		st := raft.NewMemoryStorage()
		cfg := &raft.Config{
			ID:                        id,
			ElectionTick:              10,
			HeartbeatTick:             1,
			Storage:                   st,
			MaxSizePerMsg:             1024 * 1024,
			MaxInflightMsgs:           256,
			MaxUncommittedEntriesSize: 1 << 30,
			CheckQuorum:               true,
			PreVote:                   true,
			// Seeded source for randomized election timeouts; required for
			// deterministic DST. Without this raft falls back to crypto/rand.
			Rand: rng.Intn,
		}
		rn, err := raft.NewRawNode(cfg)
		if err != nil {
			return nil, fmt.Errorf("new RawNode %d: %w", id, err)
		}
		if err := rn.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("bootstrap node %d: %w", id, err)
		}
		c.nodes[id] = &Node{id: id, raw: rn, storage: st, registerAt: make(map[uint64]uint64)}
	}
	return c, nil
}

func (c *Cluster) Tick() uint64       { return c.tick }
func (c *Cluster) Net() *Network      { return c.net }
func (c *Cluster) RNG() *rand.Rand    { return c.rng }
func (c *Cluster) Nodes() []*Node {
	out := make([]*Node, 0, len(c.ids))
	for _, id := range c.ids {
		out = append(out, c.nodes[id])
	}
	return out
}
func (c *Cluster) Node(id uint64) *Node { return c.nodes[id] }
func (c *Cluster) IDs() []uint64        { return c.ids }

// Step advances logical time by one tick:
//  1. Tick all non-partitioned nodes.
//  2. For each node, if Ready: persist entries, append committed, send msgs, Advance.
//  3. Deliver inbox messages by Step()ing them into their target nodes.
//  4. Refresh per-node soft state cache.
func (c *Cluster) Step() error {
	c.tick++
	c.net.AdvanceTick(c.tick)

	// 1. Tick.
	for _, id := range c.ids {
		if c.net.IsPartitioned(id) {
			continue
		}
		c.nodes[id].raw.Tick()
	}

	// 2. Drain Ready on each node.
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.raw.HasReady() {
			continue
		}
		rd := n.raw.Ready()
		// Persist HardState + Entries to the node's storage.
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := n.storage.SetHardState(rd.HardState); err != nil {
				return fmt.Errorf("node %d SetHardState: %w", id, err)
			}
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.Append(rd.Entries); err != nil {
				return fmt.Errorf("node %d Append: %w", id, err)
			}
		}
		if !raft.IsEmptySnap(rd.Snapshot) {
			if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
				return fmt.Errorf("node %d ApplySnapshot: %w", id, err)
			}
		}
		// Record committed entries for the oracle and apply them to the
		// node's state machine.
		for _, e := range rd.CommittedEntries {
			n.committed = append(n.committed, e)
			if e.GetIndex() > n.appliedIndex {
				n.appliedIndex = e.GetIndex()
			}
			switch e.GetType() {
			case pb.EntryNormal:
				if opID, value, ok := DecodeWrite(e.GetData()); ok {
					n.register = value
					for _, cl := range c.clients {
						cl.onWriteCommitted(opID, c.tick)
					}
				}
			case pb.EntryConfChange:
				var cc pb.ConfChange
				if err := proto.Unmarshal(e.GetData(), &cc); err == nil {
					n.raw.ApplyConfChange(&cc)
				}
			case pb.EntryConfChangeV2:
				var cc pb.ConfChangeV2
				if err := proto.Unmarshal(e.GetData(), &cc); err == nil {
					n.raw.ApplyConfChange(&cc)
				}
			}
			// Snapshot the register value at this applied index so a
			// linearizable read for safeIndex == e.GetIndex() (or any
			// index in [previous_applied+1, e.GetIndex()]) returns the
			// correct value rather than whatever the register holds
			// after additional entries applied in this same Ready.
			n.registerAt[e.GetIndex()] = n.register
		}
		// Surface ReadIndex resolutions to interested clients.
		for _, rs := range rd.ReadStates {
			for _, cl := range c.clients {
				cl.onReadIndexReady(rs.RequestCtx, rs.Index)
			}
		}
		// Send outbound messages through the network in a canonical order.
		// Upstream raft builds its message slice by iterating maps internally,
		// so the order of Ready.Messages is non-deterministic across runs;
		// sorting before delivery restores determinism without changing
		// observable behaviour, since all messages will reach their
		// destinations either way.
		sort.SliceStable(rd.Messages, func(i, j int) bool {
			return msgKey(rd.Messages[i]) < msgKey(rd.Messages[j])
		})
		for _, m := range rd.Messages {
			c.net.Send(*m)
		}
		n.raw.Advance(rd)

		// Refresh cached soft state for the oracle.
		if rd.SoftState != nil {
			n.leaderID = rd.SoftState.Lead
			n.stateStr = rd.SoftState.RaftState.String()
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			n.term = rd.HardState.GetTerm()
		}
	}

	// 2b. Poll pending reads — once a preferred node's appliedIndex has
	// caught up to a client's safe index, the read can return the
	// current register value at that node.
	for _, cl := range c.clients {
		cl.pollRead(c.tick)
	}

	// 3. Deliver inbox messages.
	for _, id := range c.ids {
		if c.net.IsPartitioned(id) {
			c.net.Drain(id) // discard while partitioned
			continue
		}
		ms := c.net.Drain(id)
		for i := range ms {
			m := ms[i]
			if err := c.nodes[id].raw.Step(&m); err != nil {
				// Step can return non-fatal errors (e.g., ErrStepPeerNotFound
				// during early-cluster races). They are not invariant
				// violations on their own — record but do not abort.
				_ = err
			}
		}
	}

	return nil
}

// RegisterClient attaches a Client to the cluster; Step will route relevant
// state-machine events (committed writes, read-index resolutions) to it.
func (c *Cluster) RegisterClient(cl *Client) {
	c.clients = append(c.clients, cl)
}

// Clients returns the registered clients in insertion order.
func (c *Cluster) Clients() []*Client { return c.clients }

// Propose attempts to propose data to the current leader. Returns the leader's
// id and whether the proposal was accepted. If no leader is known, returns
// (0, false).
func (c *Cluster) Propose(data []byte) (leader uint64, ok bool) {
	leader = c.findLeader()
	if leader == 0 {
		return 0, false
	}
	if c.net.IsPartitioned(leader) {
		return leader, false
	}
	if err := c.nodes[leader].raw.Propose(data); err != nil {
		return leader, false
	}
	return leader, true
}

// findLeader returns the id of any node that currently believes itself to be
// the leader, or 0 if none does.
func (c *Cluster) findLeader() uint64 {
	for _, id := range c.ids {
		if c.nodes[id].stateStr == "StateLeader" {
			return id
		}
	}
	return 0
}

// msgKey returns a canonical sort key for a raft message. Ordering by
// (To, From, Type, Term, Index) is deterministic regardless of how raft
// produced the messages internally.
func msgKey(m *pb.Message) string {
	return fmt.Sprintf("%020d|%020d|%05d|%020d|%020d",
		m.GetTo(), m.GetFrom(), int(m.GetType()), m.GetTerm(), m.GetIndex())
}

// CampaignAny triggers an election from a connected node. Useful at t=0 to
// seed the cluster's first election deterministically rather than waiting
// for randomized election timeouts.
func (c *Cluster) CampaignAny() error {
	for _, id := range c.ids {
		if !c.net.IsPartitioned(id) {
			return c.nodes[id].raw.Campaign()
		}
	}
	return fmt.Errorf("no connected node to campaign")
}
