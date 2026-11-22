package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
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

// TestMergeConflictPath_FailingRebaseEscalatesAtCap drives a rebase whose
// worker fails every time: the exit puts no error retry on the backoff lane,
// whose retries no budget counts, but restores the watch, so each failure
// spends one attempt and the episode escalates at max_retries.
func TestMergeConflictPath_FailingRebaseEscalatesAtCap(t *testing.T) {
	t.Parallel()

	const maxRetries = 2
	p := newRebasePath(t, maxRetries)
	for run := 1; run <= maxRetries; run++ {
		if !p.pass() {
			t.Fatalf("pass %d scheduled no rebase, want one", run)
		}
		p.start()
		p.exitAndSeed(WorkerResult{ExitKind: WorkerExitError, Error: errors.New("agent crashed")})
		if retry := p.state.RetryAttempts[p.issueID]; retry != nil {
			retry.TimerHandle.Stop()
			t.Fatalf("run %d: failed rebase put on the error retry lane, want the watch to decide", run)
		}
	}

	if p.pass() {
		t.Fatal("pass after max_retries failed rebases scheduled another, want an escalation")
	}
	if !p.escalated() {
		t.Error("escalation does not stand")
	}
	if got := p.metrics.checks["dispatched"]; got != maxRetries {
		t.Errorf(`IncMergeConflictChecks("dispatched") = %d, want %d`, got, maxRetries)
	}
}

// TestMergeConflictPath_UsageLimitedRebaseRefunds covers a rebase a usage
// limit refuses: it keeps its retry, and its attempt is given back until
// that retry starts.
func TestMergeConflictPath_UsageLimitedRebaseRefunds(t *testing.T) {
	t.Parallel()

	p := newRebasePath(t, 1)
	if !p.pass() {
		t.Fatal("first pass scheduled no rebase")
	}
	p.start()
	result := WorkerResult{ExitKind: WorkerExitError, Error: &domain.AgentError{Kind: domain.ErrUsageLimit, Message: "usage limit reached"}}
	result.IssueID, result.Identifier, result.AgentAdapter = p.issueID, p.issueID+"-ident", "mock"
	params := defaultExitParams(t, nil)
	params.Store = p.store
	params.TrackerAdapter = p.tracker
	params.HandoffState = rebasePathHandoff
	params.ActiveStates = []string{"queue"}
	params.TerminalStates = []string{"done"}
	HandleWorkerExit(p.state, result, params)

	retry := p.state.RetryAttempts[p.issueID]
	if retry == nil {
		t.Fatal("usage-limited rebase has no retry, want one")
	}
	retry.TimerHandle.Stop()
	if !retry.attemptPending {
		t.Error("retry of a usage-limited rebase does not hold its attempt pending, want it counted again at start")
	}
	if episode, err := p.store.GetReactionEpisode(context.Background(), p.issueID, ReactionKindMergeConflict); err != nil || episode.Attempts != 0 || episode.Dispatched {
		t.Errorf("persisted episode = %+v, %v; want 0 attempts and no mark", episode, err)
	}
}
