package store

import (
	"database/sql"
	"fmt"
	"time"
)

// Deploy is one recorded run of a repo's deploy.sh — success or failure.
//
// Why this lives in log-store: log-store is the record of the llm-bridge
// architecture, and a deploy run by a model session is part of that record.
// A stale or wrong-branch binary looks identical to a healthy one (it is up,
// it answers, its health check is green — it just is not running the code we
// committed), and the deploy-drift guard can only see Go binaries through
// their vcs stamp. Anything a deploy.sh builds that is NOT a stamped Go binary
// — a frontend dist, a config, a shell tool — is invisible to that guard. A
// deploy that never ran through deploy.sh leaves no row here, so "running code
// with no matching deploy record" becomes a detectable class rather than a
// mystery reported by the user for the fifth time.
//
// log-store computes NOTHING in these fields. The git facts (branch, commit,
// dirty, default branch, ancestry) are measured by deploy-log.sh in the repo
// being deployed and sent verbatim; the installed revision is read back from
// the binary by that same script. This store records what it was told, exactly
// as the events table stores a msg.Event verbatim.
type Deploy struct {
	ID     int64  `json:"id"`
	Repo   string `json:"repo"`
	Branch string `json:"branch"`
	// HeadCommit is the repo's HEAD at deploy time.
	HeadCommit string `json:"head_commit"`
	HeadDirty  bool   `json:"head_dirty"`
	// DefaultBranch is the repo's default branch name (origin/HEAD's target).
	DefaultBranch string `json:"default_branch"`
	// OnDefaultBranch is whether HEAD contains the default branch tip — i.e.
	// the deploy is NOT built from a stale side branch that has fallen behind
	// merged work. This is the regression class the drift judge escalates on
	// the first morning: a binary from a parked branch silently undoes merged
	// fixes for every session. deploy-log.sh measures it with an ancestry
	// check; a false here on a real deploy is the loud signal.
	OnDefaultBranch bool `json:"on_default_branch"`
	// InstalledRevision is the vcs.revision stamped into the binary that was
	// actually installed, read back with `go version -m`. Empty for a repo
	// that installs no stamped Go binary (a frontend, a config) — and that
	// emptiness is itself information the guard could not otherwise get.
	InstalledRevision string `json:"installed_revision"`
	InstalledDirty    bool   `json:"installed_dirty"`
	BinaryPath        string `json:"binary_path"`
	DeployScript      string `json:"deploy_script"`
	Host              string `json:"host"`
	// SessionID is the llm-bridge session that ran the deploy, if any, so a
	// model-run deploy is attributed. Empty for a hand-run deploy from a plain
	// shell.
	SessionID string `json:"session_id"`
	// Actor is how the deploy was run: interactive, worker, human, unknown.
	Actor  string `json:"actor"`
	Status string `json:"status"` // success | failed | started
	// ExitCode is deploy.sh's exit status. 0 with status=success; non-zero
	// with status=failed.
	ExitCode   int        `json:"exit_code"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

func (s *Store) migrateDeploys() error {
	_, err := s.writer.Exec(`
		CREATE TABLE IF NOT EXISTS deploys (
			id                 INTEGER PRIMARY KEY AUTOINCREMENT,
			repo               TEXT NOT NULL,
			branch             TEXT NOT NULL DEFAULT '',
			head_commit        TEXT NOT NULL DEFAULT '',
			head_dirty         INTEGER NOT NULL DEFAULT 0,
			default_branch     TEXT NOT NULL DEFAULT '',
			on_default_branch  INTEGER NOT NULL DEFAULT 0,
			installed_revision TEXT NOT NULL DEFAULT '',
			installed_dirty    INTEGER NOT NULL DEFAULT 0,
			binary_path        TEXT NOT NULL DEFAULT '',
			deploy_script      TEXT NOT NULL DEFAULT '',
			host               TEXT NOT NULL DEFAULT '',
			session_id         TEXT NOT NULL DEFAULT '',
			actor              TEXT NOT NULL DEFAULT '',
			status             TEXT NOT NULL,
			exit_code          INTEGER NOT NULL DEFAULT 0,
			started_at         DATETIME,
			finished_at        DATETIME,
			created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_deploys_repo ON deploys(repo, id);
	`)
	return err
}

// InsertDeploy records one deploy run and returns its row id. The repo and a
// status are the only required fields; everything else is best-effort context
// the client measured. A row is written even for a failed or partial deploy —
// a half-finished wrong-branch deploy is exactly the case worth catching.
func (s *Store) InsertDeploy(d Deploy) (int64, error) {
	if d.Repo == "" {
		return 0, fmt.Errorf("deploy: repo is required")
	}
	if d.Status == "" {
		return 0, fmt.Errorf("deploy: status is required")
	}
	res, err := s.writer.Exec(`
		INSERT INTO deploys (
			repo, branch, head_commit, head_dirty, default_branch, on_default_branch,
			installed_revision, installed_dirty, binary_path, deploy_script, host,
			session_id, actor, status, exit_code, started_at, finished_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.Repo, d.Branch, d.HeadCommit, d.HeadDirty, d.DefaultBranch, d.OnDefaultBranch,
		d.InstalledRevision, d.InstalledDirty, d.BinaryPath, d.DeployScript, d.Host,
		d.SessionID, d.Actor, d.Status, d.ExitCode, d.StartedAt, d.FinishedAt,
	)
	if err != nil {
		return 0, fmt.Errorf("insert deploy: %w", err)
	}
	return res.LastInsertId()
}

// ListDeploys returns deploy runs newest-first. An empty repo lists every
// repo; branch filters to deploys built from that branch; limit bounds the
// result (default 50, hard cap 500 so a caller cannot drain the table).
func (s *Store) ListDeploys(repo, branch string, limit int) ([]Deploy, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	query := `SELECT id, repo, branch, head_commit, head_dirty, default_branch,
		on_default_branch, installed_revision, installed_dirty, binary_path,
		deploy_script, host, session_id, actor, status, exit_code,
		started_at, finished_at, created_at FROM deploys WHERE 1=1`
	args := []any{}
	if repo != "" {
		query += " AND repo = ?"
		args = append(args, repo)
	}
	if branch != "" {
		query += " AND branch = ?"
		args = append(args, branch)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.reader.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deploys: %w", err)
	}
	defer rows.Close()

	out := []Deploy{}
	for rows.Next() {
		var d Deploy
		var startedAt, finishedAt sql.NullTime
		if err := rows.Scan(
			&d.ID, &d.Repo, &d.Branch, &d.HeadCommit, &d.HeadDirty, &d.DefaultBranch,
			&d.OnDefaultBranch, &d.InstalledRevision, &d.InstalledDirty, &d.BinaryPath,
			&d.DeployScript, &d.Host, &d.SessionID, &d.Actor, &d.Status, &d.ExitCode,
			&startedAt, &finishedAt, &d.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan deploy: %w", err)
		}
		if startedAt.Valid {
			d.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			d.FinishedAt = &finishedAt.Time
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
