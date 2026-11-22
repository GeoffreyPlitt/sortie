package orchestrator

import (
	"context"
	"testing"
)

// exitAndSeed ends the running rebase's worker through HandleWorkerExit with
// the workspace metadata a rebase leaves, with SCM and the merge-conflict
// reaction configured, and lets the exit itself put the watch back.
func (p *rebasePath) exitAndSeed(result WorkerResult) {
	p.t.Helper()
	wsPath := p.t.TempDir()
	writePRSCMMetadata(p.t, wsPath, 77, "owner", "repo", "feature/"+p.issueID, p.head)
	result.IssueID = p.issueID
	result.Identifier = p.issueID + "-ident"
	result.AgentAdapter = "mock"
	result.WorkspacePath = wsPath
	params := defaultExitParams(p.t, nil)
	params.Store = p.store
	params.TrackerAdapter = p.tracker
	params.HandoffState = rebasePathHandoff
	params.ActiveStates = []string{"queue"}
	params.TerminalStates = []string{"done"}
	params.SCMAdapter = &scmAdapterStubExit{}
	params.MergeConflictReactionConfigured = true
	HandleWorkerExit(p.state, result, params)
	if _, ok := p.state.PendingReactions[ReactionKey(p.issueID, ReactionKindMergeConflict)]; !ok {
		p.t.Fatal("rebase exit left no merge-conflict watch")
	}
}

// TestMergeConflictPath_RetryTimerMarksExitReseedsPassEscalates drives the
// combined path with nothing simulated between the lanes: the pass
// schedules a rebase, the retry timer starts it and marks the head, the
// exit lifts the mark and seeds the watch again, and the next pass counts
// the unchanged head and escalates at the cap.
func TestMergeConflictPath_RetryTimerMarksExitReseedsPassEscalates(t *testing.T) {
	t.Parallel()

	p := newRebasePath(t, 1)
	if !p.pass() {
		t.Fatal("first pass scheduled no rebase")
	}
	p.start()
	if episode, _ := p.store.GetReactionEpisode(context.Background(), p.issueID, ReactionKindMergeConflict); !episode.Dispatched {
		t.Fatalf("head not marked once the retry timer started the rebase: %+v", episode)
	}
	p.exitAndSeed(WorkerResult{ExitKind: WorkerExitNormal})
	if episode, _ := p.store.GetReactionEpisode(context.Background(), p.issueID, ReactionKindMergeConflict); episode.Dispatched {
		t.Fatalf("head still marked after the rebase exited: %+v", episode)
	}

	if p.pass() {
		t.Fatal("pass after an unresolved rebase scheduled another at max_retries 1, want an escalation")
	}
	if !p.escalated() {
		t.Error("escalation does not stand")
	}
}

// TestMergeConflictPath_SuccessfulRebaseStartsNewEpisode drives the same
// path for a rebase that pushed: the exit's watch sees the new head
// conflict again, and the pass dispatches it as a new episode instead of
// escalating.
func TestMergeConflictPath_SuccessfulRebaseStartsNewEpisode(t *testing.T) {
	t.Parallel()

	p := newRebasePath(t, 1)
	for run := 1; run <= 3; run++ {
		if !p.pass() {
			t.Fatalf("pass %d scheduled no rebase for a new conflict", run)
		}
		p.start()
		p.head = p.head + "-rebased"
		p.exitAndSeed(WorkerResult{ExitKind: WorkerExitNormal})
	}
	if p.escalated() {
		t.Error("escalation stands after rebases that each pushed a new head")
	}
}
