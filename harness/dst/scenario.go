package dst

import "math/rand"

// Scenario drives randomized workload + faults against a Cluster. All
// decisions are made by the cluster's RNG so a given seed produces an
// identical run.
type Scenario struct {
	rng *rand.Rand

	// Probabilities applied each tick.
	pPropose   float64
	pPartition float64
	pHeal      float64

	// Per-client probability of issuing a write (vs. a read) on each
	// issuance opportunity.
	pWrite float64

	// Clients drive the workload through Propose / ReadIndex.
	clients []*Client

	// Network fault configuration, applied once at scenario init.
	maxDelay      int
	pDup          float64
	pLinkDrop     float64 // baseline per-link drop probability
	linkDropOnce  bool

	// Op timeout: any client op outstanding longer than this many ticks
	// is finalized as Ok=false to free the client.
	opTimeoutTicks uint64

	// Clients don't start issuing ops until the cluster has had a few
	// ticks past the warmup-campaign to settle into a steady leader.
	issueStartTick uint64

	// Internal counters used to generate distinct proposal payloads.
	seq uint64

	// Optional ticks-until-first-election. If > 0, the scenario will
	// CampaignAny() once when c.Tick() reaches this value. Used to bypass
	// the randomized election-timeout warm-up so we get to a working
	// cluster quickly.
	warmupCampaignAt uint64
	warmupDone       bool
}

// DefaultScenario returns a scenario with sensible probabilities and a
// moderately adversarial network for a short DST run. It also creates one
// Client per cluster node so reads and writes exercise both leader and
// follower forwarding paths.
func DefaultScenario(c *Cluster) *Scenario {
	s := &Scenario{
		rng:              c.rng,
		pPropose:         0.30,
		pPartition:       0.01,
		pHeal:            0.05,
		maxDelay:         3,
		pDup:             0.02,
		pLinkDrop:        0.02,
		pWrite:           0.7, // 70% writes, 30% reads
		warmupCampaignAt: 15,
		issueStartTick:   30,  // election usually settled by ~tick 25
		opTimeoutTicks:   200, // ~10x the typical commit latency
	}
	c.Net().SetMaxDelay(s.maxDelay)
	c.Net().SetDuplicateRate(s.pDup)
	for _, from := range c.IDs() {
		for _, to := range c.IDs() {
			if from == to {
				continue
			}
			c.Net().SetDrop(from, to, s.pLinkDrop)
		}
	}
	for i, id := range c.IDs() {
		cl := NewClient(i, id, c)
		s.clients = append(s.clients, cl)
		c.RegisterClient(cl)
	}
	return s
}

// Drive runs one tick of the scenario against c. It should be called once
// per simulated tick, immediately before c.Step().
func (s *Scenario) Drive(c *Cluster) {
	if !s.warmupDone && c.Tick() == s.warmupCampaignAt {
		_ = c.CampaignAny()
		s.warmupDone = true
	}

	// Time out long-running ops so stuck clients can recover.
	for _, cl := range s.clients {
		cl.Tick(s.opTimeoutTicks)
	}

	// Each tick (after issueStartTick), with probability pPropose, pick
	// a random client and let it attempt an op.
	if c.Tick() >= s.issueStartTick && len(s.clients) > 0 && s.rng.Float64() < s.pPropose {
		cl := s.clients[s.rng.Intn(len(s.clients))]
		cl.MaybeIssue(s.pWrite)
	}

	if s.rng.Float64() < s.pPartition {
		ids := c.IDs()
		victim := ids[s.rng.Intn(len(ids))]
		c.Net().Partition(victim)
	}

	if s.rng.Float64() < s.pHeal {
		ids := c.IDs()
		for _, id := range ids {
			if c.Net().IsPartitioned(id) {
				c.Net().Heal(id)
				break
			}
		}
	}
}
