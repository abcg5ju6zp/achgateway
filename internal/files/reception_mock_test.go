package files

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/moov-io/base/database"
	"github.com/stretchr/testify/require"
)

func testReception(commitID string) Reception {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	return Reception{
		CommitID:   commitID,
		ShardName:  "testing",
		ShardKey:   "testing",
		FileID:     "file-" + commitID,
		Source:     SourceHTTP,
		Hostname:   "achgateway-test",
		Stage:      StageReceived,
		AcceptedAt: now,
		UpdatedAt:  now,
	}
}

func driveToPublished(t *testing.T, repo Repository, r Reception) Reception {
	t.Helper()
	ctx := context.Background()
	at := r.AcceptedAt.Add(time.Second)

	require.NoError(t, repo.Record(ctx, AcceptedFile{
		FileID: r.FileID, ShardKey: r.ShardKey, AcceptedAt: r.AcceptedAt,
	}))
	require.NoError(t, repo.MarkStaged(ctx, r.CommitID, "hash-1", at))
	require.NoError(t, repo.MarkIndexed(ctx, r.CommitID, at.Add(time.Second)))
	require.NoError(t, repo.MarkPublished(ctx, r.CommitID, r.FileID, []byte(`{"id":"done"}`), at.Add(2*time.Second)))

	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, StagePublished, got.Stage)
	require.NotNil(t, got.PublishedAt)
	require.Equal(t, `{"id":"done"}`, string(got.ResultJSON))
	return *got
}

func TestMockRepository_ReceptionLifecycle(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-lifecycle")

	// Record is idempotent and keeps the first accepted_at.
	require.NoError(t, repo.RecordReception(ctx, r))
	earlier := r.AcceptedAt.Add(-time.Hour)
	r2 := r
	r2.AcceptedAt = earlier
	require.NoError(t, repo.RecordReception(ctx, r2))
	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, r.AcceptedAt, got.AcceptedAt)
	require.Equal(t, StageReceived, got.Stage)

	driveToPublished(t, repo, r)

	// Notified closes the lifecycle.
	require.NoError(t, repo.MarkNotified(ctx, r.CommitID, r.AcceptedAt.Add(3*time.Second)))
	pending, err := repo.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)

	got, err = repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.NotNil(t, got.NotifiedAt)
}

func TestMockRepository_RejectsStageRegression(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-regression")

	require.NoError(t, repo.RecordReception(ctx, r))

	// indexed before staging is a regression.
	err := repo.MarkIndexed(ctx, r.CommitID, time.Now())
	require.ErrorIs(t, err, ErrInvalidStage)

	// Even with an index row present, published before staging is a regression.
	require.NoError(t, repo.Record(ctx, AcceptedFile{FileID: r.FileID, ShardKey: r.ShardKey}))
	err = repo.MarkPublished(ctx, r.CommitID, r.FileID, nil, time.Now())
	require.ErrorIs(t, err, ErrInvalidStage)

	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, StageReceived, got.Stage, "state must not move on rejected transition")
}

func TestMockRepository_MarkStagedIsIdempotent(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-idempotent")
	require.NoError(t, repo.RecordReception(ctx, r))

	at := time.Now()
	require.NoError(t, repo.MarkStaged(ctx, r.CommitID, "hash", at))
	require.NoError(t, repo.MarkStaged(ctx, r.CommitID, "hash", at))

	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, StageStaged, got.Stage)
}

func TestMockRepository_RecordReceptionCollision(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-key")
	require.NoError(t, repo.RecordReception(ctx, r))
	require.NoError(t, repo.RecordReception(ctx, r))

	// Distinct commitID but same (shardKey, fileID) is prevented in the real
	// schema by uniq_reception_shard_file; the mock keeps file index Record
	// duplicate detection to preserve UniqueViolation behavior.
	require.NoError(t, repo.Record(ctx, AcceptedFile{FileID: r.FileID, ShardKey: r.ShardKey}))
	err := repo.Record(ctx, AcceptedFile{FileID: r.FileID, ShardKey: r.ShardKey})
	require.Error(t, err)
	require.True(t, database.UniqueViolation(err), "duplicate must be recognizable: %v", err)
}

func TestMockRepository_ListsAndPublishedSet(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()

	unfinished, err := repo.ListUnfinishedReceptions(ctx)
	require.NoError(t, err)
	require.Empty(t, unfinished)
	published, err := repo.ListPublishedFileIDs(ctx)
	require.NoError(t, err)
	require.Empty(t, published)

	a := testReception("commit-a")
	b := testReception("commit-b")
	require.NoError(t, repo.RecordReception(ctx, a))
	require.NoError(t, repo.RecordReception(ctx, b))

	// a reaches published but is not notified; b stays staged.
	driveToPublished(t, repo, a)
	require.NoError(t, repo.MarkStaged(ctx, b.CommitID, "hash-b", b.AcceptedAt))

	pending, err := repo.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "commit-a", pending[0].CommitID)

	unfinished, err = repo.ListUnfinishedReceptions(ctx)
	require.NoError(t, err)
	ids := map[string]struct{}{}
	for _, reception := range unfinished {
		ids[reception.CommitID] = struct{}{}
	}
	require.Contains(t, ids, "commit-a")
	require.Contains(t, ids, "commit-b")

	published, err = repo.ListPublishedFileIDs(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"file-commit-a": {}}, published)
}

func TestMockRepository_CancelOnlyPublished(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-cancel")
	require.NoError(t, repo.RecordReception(ctx, r))
	require.NoError(t, repo.Record(ctx, AcceptedFile{FileID: r.FileID, ShardKey: r.ShardKey}))

	// Cancel before publication leaves the index untouched.
	require.NoError(t, repo.Cancel(ctx, r.FileID))
	file, err := repo.GetFile(ctx, r.FileID)
	require.NoError(t, err)
	require.True(t, file.CanceledAt.IsZero())

	// Cancel intent is recorded on the ledger.
	at := time.Now()
	require.NoError(t, repo.RequestCancellation(ctx, r.CommitID, at))
	require.NoError(t, repo.RequestCancellation(ctx, r.CommitID, at.Add(time.Hour)), "idempotent")
	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.NotNil(t, got.CancelRequestedAt)
	require.Equal(t, at.UTC().Format(time.RFC3339), got.CancelRequestedAt.UTC().Format(time.RFC3339))

	// After publication cancel lands on the index row.
	require.NoError(t, repo.MarkStaged(ctx, r.CommitID, "h", at))
	require.NoError(t, repo.MarkIndexed(ctx, r.CommitID, at))
	require.NoError(t, repo.MarkPublished(ctx, r.CommitID, r.FileID, nil, at))
	require.NoError(t, repo.Cancel(ctx, r.FileID))
	file, err = repo.GetFile(ctx, r.FileID)
	require.NoError(t, err)
	require.False(t, file.CanceledAt.IsZero())
}

func TestMockRepository_MarkFailed(t *testing.T) {
	repo := &MockRepository{}
	ctx := context.Background()
	r := testReception("commit-fail")
	require.NoError(t, repo.RecordReception(ctx, r))
	require.NoError(t, repo.MarkStaged(ctx, r.CommitID, "h", time.Now()))

	require.NoError(t, repo.MarkFailed(ctx, r.CommitID, "disk gone", time.Now()))
	got, err := repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, StageFailed, got.Stage)
	require.Equal(t, "disk gone", got.LastError)

	require.NoError(t, repo.MarkFailed(ctx, r.CommitID, "again", time.Now()))
	got, err = repo.GetReception(ctx, r.CommitID)
	require.NoError(t, err)
	require.Equal(t, "disk gone", got.LastError, "first failure reason is retained")

	// Not-found surfaces the typed sentinel.
	_, err = repo.GetReception(ctx, "missing")
	require.ErrorIs(t, err, ErrReceptionNotFound)
	require.True(t, errors.Is(err, ErrReceptionNotFound))
}
