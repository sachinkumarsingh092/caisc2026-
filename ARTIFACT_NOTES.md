# Artifact Notes

Two known gaps between the paper and the raw artifacts in this archive, documented
here so reviewers can reconcile them directly.

## 1. Headline result (25.4% paired-median speedup)

The accepted candidate behind the headline number is included as source. Its change
to `maybeSendAppend` (three coordinated edits over the pinned baseline) is reproduced
verbatim below.

What is **not** persisted as a separate raw file in this archive is the paired 10-run
benchmark output from which the 25.4% paired-median figure was computed: the figure
was read from the live run and the per-run table was not saved to disk. The candidate
is deterministic and re-benchmarkable against the pinned baseline
(`raft-upstream/raft.go.baseline`) with the same bench used in the paper:

    go test -bench=BenchmarkProposal3Nodes -benchtime=1s -count=10

Candidate diff (pinned baseline -> accepted candidate, `maybeSendAppend`):

```diff
@@ func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
 	pr := r.trk.Progress[to]
 	if pr.IsPaused() {
 		return false
 	}
-	prevIndex := pr.Next - 1
+	// Determine if we are throttled (StateReplicate with full Inflights).
+	// In that case we can only send an empty MsgApp; if the caller doesn't
+	// want empties, bail out before any log access.
+	next := pr.Next
+	throttled := pr.State == tracker.StateReplicate && pr.Inflights.Full()
+	if throttled && !sendIfEmpty {
+		return false
+	}
+
+	prevIndex := next - 1
 	prevTerm, err := r.raftLog.term(prevIndex)
 	if err != nil {
 		// The log probably got truncated at >= pr.Next, so we can't catch up the
 		// follower log anymore. Send a snapshot instead.
 		return r.maybeSendSnapshot(to, pr)
 	}
@@
-	if pr.State != tracker.StateReplicate || !pr.Inflights.Full() {
-		ents, err = r.raftLog.entries(pr.Next, r.maxMsgSize)
-		if err != nil {
-			return r.maybeSendSnapshot(to, pr)
-		}
-	}
-	if len(ents) == 0 && !sendIfEmpty {
-		return false
-	}
-	// TODO(pav-kv): move this check up to where err is returned.
-	if err != nil { // send a snapshot if we failed to get the entries
-		return r.maybeSendSnapshot(to, pr)
-	}
+	var ents []*pb.Entry
+	if !throttled {
+		ents, err = r.raftLog.entries(next, r.maxMsgSize)
+		if err != nil {
+			return r.maybeSendSnapshot(to, pr)
+		}
+	}
+	nEnts := len(ents)
+	if nEnts == 0 && !sendIfEmpty {
+		return false
+	}
+
+	// Send the actual MsgApp and update the progress accordingly.
+	commit := r.raftLog.committed
 	r.send(&pb.Message{
 		To:      new(to),
 		Type:    pb.MsgApp.Enum(),
 		Index:   new(prevIndex),
 		LogTerm: new(prevTerm),
 		Entries: ents,
-		Commit:  new(r.raftLog.committed),
+		Commit:  new(commit),
 	})
-	pr.SentEntries(len(ents), uint64(payloadsSize(ents)))
-	pr.SentCommit(r.raftLog.committed)
+	// Compute payload size only when there are entries; skip the
+	// payloadsSize() scan entirely for the common empty-MsgApp path
+	// (used to convey commit-index updates) and fold both cases into
+	// a single SentEntries call.
+	var bytes uint64
+	if nEnts != 0 {
+		bytes = uint64(payloadsSize(ents))
+	}
+	pr.SentEntries(nEnts, bytes)
+	pr.SentCommit(commit)
 	return true
 }
```

## 2. Ablation attribution for the `skip_pause` variant (L2 vs. L3)

`harness/optimize/ablation_results.json` records the `skip_pause` variant with
`"caught_by": "L2_linearizability"` and `"2/12 seeds non-linearizable"`. The paper
(Sec. 6.4 and the "Planted-Bug Ablation" appendix) attributes the *reliable* catch to
the upstream test suite (L3). These are consistent:

- The `caught_by` field names the **earliest** layer that flagged the variant.
- For `skip_pause` that is L2, but only at 2/12 seeds, which sits at the control noise
  floor: the unmodified `baseline` control is itself flagged at 1/12 seeds in the same
  table. So the L2 signal is not a reliable detector for this bug.
- The same JSON record shows `"upstream_tests": "FAIL:TestMsgAppFlowControlFull"`, i.e.
  L3 fails deterministically on this variant. That is the catch the paper credits, and
  the reason accepted candidates are re-run against the upstream suite.

In short: the JSON reports the first layer to fire; the paper reports the layer that
fires reliably. No layer attribution in the paper contradicts the JSON.
