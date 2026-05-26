package dst

import (
	"encoding/binary"
	"math/rand"
)

// Scenario drives randomized workload + faults against a Cluster. All
// decisions are made by the cluster's RNG so a given seed produces an
// identical run.
type Scenario struct {
	rng *rand.Rand

	// Probabilities applied each tick.
	pPropose   float64
	pPartition float64
	pHeal      float64

	// Internal counters used to generate distinct proposal payloads.
	seq uint64

	// Optional ticks-until-first-election. If > 0, the scenario will
	// CampaignAny() once when c.Tick() reaches this value. Used to bypass
	// the randomized election-timeout warm-up so we get to a working
	// cluster quickly.
	warmupCampaignAt uint64
	warmupDone       bool
}

// DefaultScenario returns a scenario with sensible probabilities for a
// short DST run.
func DefaultScenario(c *Cluster) *Scenario {
	return &Scenario{
		rng:              c.rng,
		pPropose:         0.20, // ~one proposal every five ticks
		pPartition:       0.01, // 1% chance per tick to partition someone
		pHeal:            0.05, // 5% chance per tick to heal a partition
		warmupCampaignAt: 15,   // give nodes a moment to settle, then kick off
	}
}

// Drive runs one tick of the scenario against c. It should be called once
// per simulated tick, immediately before c.Step().
func (s *Scenario) Drive(c *Cluster) {
	if !s.warmupDone && c.Tick() == s.warmupCampaignAt {
		_ = c.CampaignAny()
		s.warmupDone = true
	}

	if s.rng.Float64() < s.pPropose {
		s.seq++
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], s.seq)
		_, _ = c.Propose(buf[:])
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
