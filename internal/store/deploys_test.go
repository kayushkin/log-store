package store

import (
	"testing"
	"time"
)

func TestInsertAndListDeploys_NewestFirst(t *testing.T) {
	s := newTestStore(t)
	for _, rev := range []string{"aaa", "bbb", "ccc"} {
		if _, err := s.InsertDeploy(Deploy{Repo: "dash", Branch: "main", HeadCommit: rev, Status: "success"}); err != nil {
			t.Fatalf("insert %s: %v", rev, err)
		}
	}
	got, err := s.ListDeploys("dash", "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	if got[0].HeadCommit != "ccc" {
		t.Fatalf("want newest (ccc) first, got %s", got[0].HeadCommit)
	}
}

func TestInsertDeploy_RequiresRepoAndStatus(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InsertDeploy(Deploy{Status: "success"}); err == nil {
		t.Fatal("want error for missing repo")
	}
	if _, err := s.InsertDeploy(Deploy{Repo: "dash"}); err == nil {
		t.Fatal("want error for missing status")
	}
}

// A failed, off-default-branch deploy is exactly the regression the log exists
// to catch, so every field of it must survive the round trip.
func TestInsertDeploy_FailedOffDefaultBranchRoundTrips(t *testing.T) {
	s := newTestStore(t)
	start := time.Now().UTC().Truncate(time.Second)
	fin := start.Add(30 * time.Second)
	in := Deploy{
		Repo: "llm-bridge-claudecode", Branch: "fix/old-side-branch",
		HeadCommit: "deadbeef", HeadDirty: true,
		DefaultBranch: "main", OnDefaultBranch: false,
		InstalledRevision: "deadbeef", InstalledDirty: true,
		BinaryPath: "/home/x/bin/llm-bridge-claudecode", DeployScript: "/home/x/repos/llm-bridge-claudecode/deploy.sh",
		Host: "box", SessionID: "br_123", Actor: "worker",
		Status: "failed", ExitCode: 1, StartedAt: &start, FinishedAt: &fin,
	}
	if _, err := s.InsertDeploy(in); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := s.ListDeploys("", "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	d := got[0]
	if d.OnDefaultBranch || !d.HeadDirty || d.Status != "failed" || d.ExitCode != 1 {
		t.Fatalf("regression fields lost: %+v", d)
	}
	if d.Branch != "fix/old-side-branch" || d.SessionID != "br_123" || d.Actor != "worker" {
		t.Fatalf("context fields lost: %+v", d)
	}
	if d.StartedAt == nil || !d.StartedAt.Equal(start) || d.FinishedAt == nil || !d.FinishedAt.Equal(fin) {
		t.Fatalf("timestamps lost: started=%v finished=%v", d.StartedAt, d.FinishedAt)
	}
}

func TestListDeploys_RepoAndBranchFilter(t *testing.T) {
	s := newTestStore(t)
	s.InsertDeploy(Deploy{Repo: "dash", Branch: "main", Status: "success"})
	s.InsertDeploy(Deploy{Repo: "dash", Branch: "fix/x", Status: "success"})
	s.InsertDeploy(Deploy{Repo: "noteboard", Branch: "main", Status: "success"})

	if got, _ := s.ListDeploys("dash", "", 0); len(got) != 2 {
		t.Fatalf("repo filter: want 2, got %d", len(got))
	}
	if got, _ := s.ListDeploys("dash", "fix/x", 0); len(got) != 1 {
		t.Fatalf("repo+branch filter: want 1, got %d", len(got))
	}
	if got, _ := s.ListDeploys("", "", 0); len(got) != 3 {
		t.Fatalf("no filter: want 3, got %d", len(got))
	}
}
