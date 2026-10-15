package files

import (
	"context"
	"testing"
	"time"

	"github.com/moov-io/achgateway/internal/dbtest"
	"github.com/moov-io/base"
	"github.com/moov-io/base/database"

	"github.com/stretchr/testify/require"
)

func TestRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag was specified")
	}

	conf := dbtest.CreateTestDatabase(t, dbtest.LocalDatabaseConfig())
	db := dbtest.LoadDatabase(t, conf)
	require.NoError(t, db.Ping())

	repo := NewRepository(db)
	if _, ok := repo.(*sqlRepository); !ok {
		t.Errorf("unexpected repository type: %T", repo)
	}

	ctx := context.Background()
	fileID1 := base.ID()
	accepted := AcceptedFile{
		FileID:     fileID1,
		ShardKey:   base.ID(),
		Hostname:   "achgateway-0",
		AcceptedAt: time.Now(),
	}

	// Record
	err := repo.Record(ctx, accepted)
	require.NoError(t, err)

	err = repo.Record(ctx, accepted)
	require.ErrorContains(t, err, "Duplicate entry")
	require.True(t, database.UniqueViolation(err))

	// Second File
	fileID2 := base.ID()
	accepted.FileID = fileID2
	err = repo.Record(ctx, accepted)
	require.NoError(t, err)

	// Cancel
	err = repo.Cancel(ctx, fileID1)
	require.NoError(t, err)

	err = repo.Cancel(ctx, base.ID())
	require.NoError(t, err)
}

func TestRepository_Receptions(t *testing.T) {
	if testing.Short() {
		t.Skip("-short flag was specified")
	}

	conf := dbtest.CreateTestDatabase(t, dbtest.LocalDatabaseConfig())
	db := dbtest.LoadDatabase(t, conf)

	repo := NewRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	commitID := base.ID()
	reception := Reception{
		CommitID:   commitID,
		ShardName:  "testing",
		ShardKey:   base.ID(),
		FileID:     base.ID(),
		Source:     SourceStream,
		Hostname:   "achgateway-mysql",
		Stage:      StageReceived,
		AcceptedAt: now,
		UpdatedAt:  now,
	}

	// Record is idempotent and keeps the first writer's row.
	require.NoError(t, repo.RecordReception(ctx, reception))
	require.NoError(t, repo.RecordReception(ctx, reception))
	got, err := repo.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, StageReceived, got.Stage)
	require.Equal(t, reception.FileID, got.FileID)

	// Drive the ledger and index forward together.
	require.NoError(t, repo.MarkStaged(ctx, commitID, "deadbeef", now))
	require.NoError(t, repo.Record(ctx, AcceptedFile{
		FileID:     reception.FileID,
		ShardKey:   reception.ShardKey,
		Hostname:   "achgateway-mysql",
		AcceptedAt: now,
	}))
	require.NoError(t, repo.MarkIndexed(ctx, commitID, now))
	require.NoError(t, repo.MarkPublished(ctx, commitID, reception.FileID, []byte(`{"error":""}`), now))
	require.NoError(t, repo.MarkNotified(ctx, commitID, now))

	got, err = repo.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, StagePublished, got.Stage)
	require.NotNil(t, got.StagedAt)
	require.NotNil(t, got.PublishedAt)
	require.NotNil(t, got.NotifiedAt)
	require.Equal(t, "deadbeef", got.ContentSHA256)
	require.JSONEq(t, `{"error":""}`, string(got.ResultJSON))

	pending, err := repo.ListPendingNotifications(ctx)
	require.NoError(t, err)
	for _, r := range pending {
		require.NotEqual(t, commitID, r.CommitID)
	}

	published, err := repo.ListPublishedFileIDs(ctx)
	require.NoError(t, err)
	require.Contains(t, published, reception.FileID)

	// Cancel intent before/after publication is idempotent.
	require.NoError(t, repo.RequestCancellation(ctx, commitID, now))
	require.NoError(t, repo.RequestCancellation(ctx, commitID, now))
}
