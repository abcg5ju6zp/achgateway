// Licensed to The Moov Authors under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The Moov Authors licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pipeline

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/base"

	"github.com/stretchr/testify/require"
)

func recoverFile(t *testing.T) *ach.File {
	t.Helper()
	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	return file
}

func plantStagedReception(t *testing.T, fr *TestFileReceiver, commitID, fileID string, file *ach.File) string {
	t.Helper()
	ctx := context.Background()
	m := testingMerger(fr)
	hashSum, err := m.StageXfer(ctx, incoming.ACHFile{FileID: fileID, ShardKey: "testing", File: file}, commitID)
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, fr.fileRepository.RecordReception(ctx, files.Reception{
		CommitID:   commitID,
		ShardName:  "testing",
		ShardKey:   "testing",
		FileID:     fileID,
		Source:     files.SourceStream,
		Hostname:   "recovery-test",
		AcceptedAt: now,
		UpdatedAt:  now,
	}))
	require.NoError(t, fr.fileRepository.MarkStaged(ctx, commitID, hashSum, now))
	return hashSum
}

func TestRecovery_StagedUnindexed_ResumesToPublished(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := recoverFile(t)
	commitID := "recover-staged"
	fileID := base.ID()
	hashSum := plantStagedReception(t, fr, commitID, fileID, file)

	require.NoError(t, fr.reconcileShard(ctx, agg))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.NotNil(t, reception.PublishedAt)
	require.NotEmpty(t, reception.ResultJSON)

	indexed, err := fr.fileRepository.GetFile(ctx, fileID)
	require.NoError(t, err)
	require.NotNil(t, indexed.PublishedAt)

	require.Equal(t, hashSum, readStorageHash(t, m, filepath.Join("mergable", "testing", fileID+".ach")))
	staged, err := m.ListStaged(ctx)
	require.NoError(t, err)
	require.Empty(t, staged)

	// The recovered result is deliverable through the outbox.
	require.Equal(t, 1, fr.pumpOutbox(ctx))
	reception, err = fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.NotNil(t, reception.NotifiedAt)
}

func TestRecovery_RenameHappenedBeforeCrash_FinishesMarkers(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := recoverFile(t)
	commitID := "recover-renamed"
	fileID := base.ID()
	xfer := incoming.ACHFile{FileID: fileID, ShardKey: "testing", File: file}
	hashSum := plantStagedReception(t, fr, commitID, fileID, file)

	// Simulate a crash after the atomic move but before the published markers.
	stagedACH, _ := m.stagingPaths(xfer, commitID)
	finalACH, _ := m.mergablePaths(xfer)
	require.NoError(t, m.storage.MoveFile(stagedACH, finalACH))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	// No second move: the mergable content is the staged content.
	require.Equal(t, hashSum, readStorageHash(t, m, finalACH))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
}

func TestRecovery_MissingEverywhere_MarkedFailedWithResult(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	fileID := base.ID()
	commitID := "recover-ghost"
	now := time.Now().UTC()
	require.NoError(t, fr.fileRepository.RecordReception(ctx, files.Reception{
		CommitID:   commitID,
		ShardName:  "testing",
		ShardKey:   "testing",
		FileID:     fileID,
		Source:     files.SourceStream,
		AcceptedAt: now,
		UpdatedAt:  now,
	}))
	require.NoError(t, fr.fileRepository.MarkStaged(ctx, commitID, "deadbeef", now))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StageFailed, reception.Stage)
	require.Contains(t, reception.LastError, "staged content missing")
	require.NotEmpty(t, reception.ResultJSON, "the failure result must be frozen for the outbox")

	// No phantom file must appear in mergable; the failure is notified once.
	_, err = m.storage.Open(filepath.Join("mergable", "testing", fileID+".ach"))
	require.Error(t, err)
	pending, err := fr.fileRepository.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, files.StageFailed, pending[0].Stage)
	require.Equal(t, 1, fr.pumpOutbox(ctx))
}

func TestRecovery_LegacyMergable_Backfilled(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := recoverFile(t)

	// Old deployment state: mergable file present and files row present but
	// unpublished; no reception ledger row at all.
	var buf bytes.Buffer
	require.NoError(t, ach.NewWriter(&buf).Write(file))
	legacyID := "legacy-" + base.ID()
	legacyPath := filepath.Join("mergable", "testing", legacyID+".ach")
	require.NoError(t, m.storage.WriteFile(legacyPath, buf.Bytes()))
	acceptedAt := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, fr.fileRepository.Record(ctx, files.AcceptedFile{
		FileID:     legacyID,
		ShardKey:   "testing",
		Hostname:   "old-host",
		AcceptedAt: acceptedAt,
	}))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	indexed, err := fr.fileRepository.GetFile(ctx, legacyID)
	require.NoError(t, err)
	require.NotNil(t, indexed.PublishedAt)

	reception, err := fr.fileRepository.GetReception(ctx, incoming.DeriveCommitID("testing", legacyID))
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.Equal(t, acceptedAt.Unix(), reception.AcceptedAt.Unix(), "original acceptance time must be retained")
	require.NotEmpty(t, reception.ResultJSON)

	published, err := fr.fileRepository.ListPublishedFileIDs(ctx)
	require.NoError(t, err)
	require.Contains(t, published, legacyID)
}

func TestRecovery_PublishedCanceledPendingNotify_NotFailed(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := recoverFile(t)
	commitID := "recover-canceled-published"
	fileID := base.ID()

	// Cancel lands first, then the file publishes as .ach.canceled.
	require.NoError(t, fr.processMessage(ctx,
		sendCancelMessage(t, commitID, "testing", fileID), files.SourceHTTP))
	require.NoError(t, fr.processMessage(ctx,
		sendQueueMessage(t, commitID, "testing", fileID, file), files.SourceHTTP))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.Nil(t, reception.NotifiedAt, "precondition: notification still pending")

	// Recovery must recognize the canceled published file and leave it alone.
	require.NoError(t, fr.reconcileShard(ctx, agg))
	require.NoError(t, fr.reconcileShard(ctx, agg))

	reception, err = fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage, "healthy canceled file must not be marked failed")

	canceled := readMergable(t, m, fileID, ".ach.canceled")
	require.NotEmpty(t, canceled)

	// The pending canceled result is still deliverable.
	pending, err := fr.fileRepository.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestRecovery_PublishedPendingNotify_IsIdempotent(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	// A full publish leaves the reception published with a pending notification.
	file := recoverFile(t)
	commitID := "recover-published"
	fileID := base.ID()
	msg := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(ctx, msg, files.SourceStream))

	receptionBefore, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, receptionBefore.Stage)

	// Recovery must not rewrite or duplicate anything.
	require.NoError(t, fr.reconcileShard(ctx, agg))

	receptionAfter, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, receptionBefore.Stage, receptionAfter.Stage)
	require.Equal(t, receptionBefore.ResultJSON, receptionAfter.ResultJSON)

	matches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestRecovery_CompletesInterruptedSidecarMove(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := optsFile(t) // carries CustomReturnCodes -> .json sidecar staged
	commitID := "recover-half-sidecar"
	fileID := "sidecar-file"
	xfer := incoming.ACHFile{FileID: fileID, ShardKey: "testing", File: file}

	_, err := m.StageXfer(ctx, xfer, commitID)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, fr.fileRepository.RecordReception(ctx, files.Reception{
		CommitID: commitID, ShardName: "testing", ShardKey: "testing", FileID: fileID,
		Source: files.SourceStream, AcceptedAt: now, UpdatedAt: now,
	}))
	require.NoError(t, fr.fileRepository.MarkStaged(ctx, commitID, hashOfFile(t, file), now))
	require.NoError(t, fr.fileRepository.Record(ctx, files.AcceptedFile{
		FileID: fileID, ShardKey: "testing", AcceptedAt: now,
	}))

	// Crash between the ACH move and the sidecar move.
	stagedACH, stagedOpts := m.stagingPaths(xfer, commitID)
	finalACH, _ := m.mergablePaths(xfer)
	require.NoError(t, m.storage.MoveFile(stagedACH, finalACH))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	fd, err := m.storage.Open(stagedOpts)
	if err == nil {
		fd.Close()
	}
	_, optsErr := m.storage.Open(filepath.Join("mergable", "testing", fileID+".json"))
	require.NoError(t, optsErr, "recovery must complete the interrupted sidecar move")
	require.Error(t, err, "staged sidecar must be gone")
}

func TestRecovery_CanceledFileConsumedByCutoff_StaysPublished(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	m := testingMerger(fr)
	agg := fr.shardAggregators["testing"]

	file := recoverFile(t)
	commitID := "recover-canceled-consumed"
	fileID := base.ID()

	require.NoError(t, fr.processMessage(ctx, sendCancelMessage(t, commitID, "testing", fileID), files.SourceHTTP))
	require.NoError(t, fr.processMessage(ctx, sendQueueMessage(t, commitID, "testing", fileID, file), files.SourceHTTP))

	// Simulate a cutoff that isolated and removed the canceled marker.
	require.NoError(t, m.storage.RmdirAll(filepath.Join("mergable", "testing", fileID+".ach.canceled")))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage, "consumed canceled file is not a failure")
	pending, err := fr.fileRepository.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the success result still needs delivery")
}

func TestRecovery_ReceivedOnly_LeftForRedelivery(t *testing.T) {
	fr := directReceiver(t)
	ctx := context.Background()
	agg := fr.shardAggregators["testing"]

	commitID := "recover-waiting"
	now := time.Now().UTC()
	require.NoError(t, fr.fileRepository.RecordReception(ctx, files.Reception{
		CommitID:   commitID,
		ShardName:  "testing",
		ShardKey:   "testing",
		FileID:     "waiting-file",
		Source:     files.SourceHTTP,
		AcceptedAt: now,
		UpdatedAt:  now,
	}))

	require.NoError(t, fr.reconcileShard(ctx, agg))

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StageReceived, reception.Stage)
	pending, err := fr.fileRepository.ListPendingNotifications(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "received-only commits are not terminal results")
}
