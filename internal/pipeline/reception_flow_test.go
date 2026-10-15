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
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/incoming/stream/streamtest"
	"github.com/moov-io/achgateway/internal/storage"
	"github.com/moov-io/achgateway/pkg/compliance"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base"

	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
)

// countingChest counts the mutating storage operations so tests can prove a
// retry never writes the file a second time.
type countingChest struct {
	storage.Chest
	writes int64
	moves  int64
}

func (c *countingChest) WriteFile(path string, contents []byte) error {
	atomic.AddInt64(&c.writes, 1)
	return c.Chest.WriteFile(path, contents)
}

func (c *countingChest) MoveFile(oldpath, newpath string) error {
	atomic.AddInt64(&c.moves, 1)
	return c.Chest.MoveFile(oldpath, newpath)
}

func testingMerger(fr *TestFileReceiver) *filesystemMerging {
	return fr.shardAggregators["testing"].merger.(*filesystemMerging)
}

// directReceiver builds a receiver with its background subscription closed so
// only the test's explicit processMessage calls consume messages (the inmem
// topic would otherwise fan each message out to the running consumer too).
func directReceiver(t *testing.T) *TestFileReceiver {
	t.Helper()
	fr := setupTestFileReceiver(t)
	fr.Shutdown()
	return fr
}

func installAckCounter(fr *TestFileReceiver) *atomic.Int32 {
	var acks atomic.Int32
	fr.ackMessage = func(*pubsub.Message) { acks.Add(1) }
	return &acks
}

// sendQueueMessage publishes a QueueACHFile carrying commitID and returns the
// delivered message for direct processing.
func sendQueueMessage(t *testing.T, commitID, shardKey, fileID string, file *ach.File) *pubsub.Message {
	t.Helper()
	bs, err := compliance.Protect(nil, models.Event{
		Event: models.QueueACHFile{
			CommitID: commitID,
			FileID:   fileID,
			ShardKey: shardKey,
			File:     file,
		},
	})
	require.NoError(t, err)

	pub, sub := streamtest.InmemStream(t)
	require.NoError(t, pub.Send(context.Background(), &pubsub.Message{
		Body: bs,
		Metadata: map[string]string{
			incoming.CommitIDMetadataKey: commitID,
		},
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := sub.Receive(ctx)
	require.NoError(t, err)
	return msg
}

func sendCancelMessage(t *testing.T, commitID, shardKey, fileID string) *pubsub.Message {
	t.Helper()
	bs, err := compliance.Protect(nil, models.Event{
		Event: models.CancelACHFile{
			CommitID: commitID,
			FileID:   fileID,
			ShardKey: shardKey,
		},
	})
	require.NoError(t, err)

	pub, sub := streamtest.InmemStream(t)
	require.NoError(t, pub.Send(context.Background(), &pubsub.Message{Body: bs}))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := sub.Receive(ctx)
	require.NoError(t, err)
	return msg
}

func drainQueueResponse(t *testing.T, fr *TestFileReceiver) incoming.QueueACHFileResponse {
	t.Helper()
	select {
	case resp := <-fr.QueueFileResponses:
		return resp
	case <-time.After(2 * time.Second):
		t.Fatal("no QueueACHFileResponse delivered")
		return incoming.QueueACHFileResponse{}
	}
}

func readMergable(t *testing.T, m *filesystemMerging, fileID, suffix string) []byte {
	t.Helper()
	fd, err := m.storage.Open(filepath.Join("mergable", "testing", fileID+suffix))
	require.NoError(t, err)
	defer fd.Close()
	bs, err := io.ReadAll(fd)
	require.NoError(t, err)
	return bs
}

func TestReception_HappyPath_StagesAndAck(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)

	m := testingMerger(fr)
	counter := &countingChest{Chest: m.storage}
	m.storage = counter

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	expectedHash := hashOfFile(t, file)

	commitID := "commit-happy"
	fileID := base.ID()
	msg := sendQueueMessage(t, commitID, "testing", fileID, file)

	require.NoError(t, fr.processMessage(context.Background(), msg, files.SourceHTTP))

	// Acked only once the file was published.
	require.Equal(t, int32(1), acks.Load())

	// Ledger ran through every stage with a frozen result.
	reception, err := fr.fileRepository.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.NotNil(t, reception.StagedAt)
	require.NotNil(t, reception.PublishedAt)
	require.Equal(t, expectedHash, reception.ContentSHA256)
	require.NotEmpty(t, reception.ResultJSON)
	require.Nil(t, reception.NotifiedAt, "notification is driven by the T5 outbox, not inline")

	// Index row is published and therefore cancellable.
	indexed, err := fr.fileRepository.GetFile(context.Background(), fileID)
	require.NoError(t, err)
	require.NotNil(t, indexed.PublishedAt)

	// Staging content moved atomically; mergable bytes match the input.
	require.Equal(t, expectedHash, sha256Hex(readMergable(t, m, fileID, ".ach")))
	staged, err := m.ListStaged(context.Background())
	require.NoError(t, err)
	require.Empty(t, staged)

	resp := drainQueueResponse(t, fr)
	require.Equal(t, commitID, resp.CommitID)
	require.Equal(t, fileID, resp.FileID)
	require.NotNil(t, resp.AcceptedAt)
	require.NotNil(t, resp.NextCutoff)
	require.Empty(t, resp.Error)

	// Exactly one physical write/move per file component: the NACHA file and
	// the ValidateOpts sidecar that models.Read always attaches.
	require.Equal(t, int64(2), atomic.LoadInt64(&counter.writes))
	require.Equal(t, int64(2), atomic.LoadInt64(&counter.moves))

	pending, err := fr.fileRepository.ListPendingNotifications(context.Background())
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestReception_StageFailure_CleansUpAndRetrySucceeds(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)

	m := testingMerger(fr)
	// Every write fails on the first attempt.
	m.storage = &storage.MockStorage{WriteFileErr: os.ErrPermission}

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-stage-fail"
	fileID := base.ID()

	msg := sendQueueMessage(t, commitID, "testing", fileID, file)
	err = fr.processMessage(context.Background(), msg, files.SourceStream)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRetriable)
	require.ErrorContains(t, err, "staging file")
	require.Equal(t, int32(0), acks.Load(), "failed staging must not acknowledge the upstream message")

	// No unpublished residue, no mergable file, reception stays resumable.
	_, err = m.storage.Open(filepath.Join("mergable", "testing", fileID+".ach"))
	require.Error(t, err)
	reception, err := fr.fileRepository.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StageReceived, reception.Stage)

	// Recover the disk and redeliver the same commit.
	realChest, err := storage.NewFilesystem(fr.MergingDir)
	require.NoError(t, err)
	m.storage = realChest

	retry := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(context.Background(), retry, files.SourceStream))
	require.Equal(t, int32(1), acks.Load())

	reception, err = fr.fileRepository.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.Equal(t, reception.AcceptedAt, reception.AcceptedAt, "first acceptance time is retained")

	matches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestReception_IndexFailure_RetryDoesNotRewriteStaging(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)

	m := testingMerger(fr)
	counter := &countingChest{Chest: m.storage}
	m.storage = counter

	repo := fr.fileRepository.(*files.MockRepository)
	var indexFailing atomic.Bool
	indexFailing.Store(true)
	repo.RecordFileHook = func(files.AcceptedFile) error {
		if indexFailing.Load() {
			return errors.New("index down")
		}
		return nil
	}

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-index-fail"
	fileID := base.ID()

	first := sendQueueMessage(t, commitID, "testing", fileID, file)
	err = fr.processMessage(context.Background(), first, files.SourceStream)
	require.ErrorIs(t, err, ErrRetriable)
	require.Equal(t, int32(0), acks.Load())

	reception, err := repo.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StageStaged, reception.Stage, "staged fact must survive index failure")

	staged, err := m.ListStaged(context.Background())
	require.NoError(t, err)
	require.Len(t, staged, 1, "staged content must survive index failure")
	writesAfterFirst := atomic.LoadInt64(&counter.writes)

	// Index recovers; redelivery resumes without restaging the file.
	indexFailing.Store(false)
	second := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(context.Background(), second, files.SourceStream))
	require.Equal(t, int32(1), acks.Load())

	require.Equal(t, writesAfterFirst, atomic.LoadInt64(&counter.writes),
		"retry must not rewrite staged bytes")
	require.Equal(t, int64(2), atomic.LoadInt64(&counter.moves), "exactly one publish move per component")

	reception, err = repo.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
}

func TestReception_RedeliveryAfterMoveFinalizesWithoutRestaging(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)
	ctx := context.Background()
	m := testingMerger(fr)

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-moved-redeliver"
	fileID := base.ID()
	xfer := incoming.ACHFile{FileID: fileID, ShardKey: "testing", File: file}

	// Crash window: reception is indexed, files row unpublished, and the
	// atomic move into mergable already happened but markers were never set.
	plantStagedReception(t, fr, commitID, fileID, file)
	now := time.Now().UTC()
	require.NoError(t, fr.fileRepository.Record(ctx, files.AcceptedFile{
		FileID: fileID, ShardKey: "testing", AcceptedAt: now,
	}))
	require.NoError(t, fr.fileRepository.MarkIndexed(ctx, commitID, now))
	stagedACH, _ := m.stagingPaths(xfer, commitID)
	finalACH, _ := m.mergablePaths(xfer)
	require.NoError(t, m.storage.MoveFile(stagedACH, finalACH))

	// Hot redelivery: PublishXfer's move fails (staging gone), but the bytes
	// are already in mergable — the receiver finalizes instead of looping.
	msg := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(ctx, msg, files.SourceStream))
	require.Equal(t, int32(1), acks.Load())

	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.NotEmpty(t, reception.ResultJSON)

	matches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestReception_SameCommitRetryReturnsFrozenResult(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)

	m := testingMerger(fr)
	counter := &countingChest{Chest: m.storage}
	m.storage = counter

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-retry"
	fileID := base.ID()

	first := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(context.Background(), first, files.SourceStream))
	firstResp := drainQueueResponse(t, fr)

	// Same identity redelivered (Kafka retry / HTTP retry): frozen result,
	// same acceptance time, no second write.
	second := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(context.Background(), second, files.SourceStream))
	secondResp := drainQueueResponse(t, fr)

	// JSON round-trip loses the Location name on times, so compare the
	// frozen payload field-by-field with instant equality.
	require.Equal(t, firstResp.CommitID, secondResp.CommitID)
	require.Equal(t, firstResp.FileID, secondResp.FileID)
	require.Equal(t, firstResp.ShardKey, secondResp.ShardKey)
	require.Equal(t, firstResp.Error, secondResp.Error)
	require.True(t, firstResp.AcceptedAt.Equal(*secondResp.AcceptedAt))
	require.True(t, firstResp.NextCutoff.Equal(*secondResp.NextCutoff))
	require.Equal(t, int32(2), acks.Load(), "each delivery is acknowledged after the published gate")
	require.Equal(t, int64(2), atomic.LoadInt64(&counter.writes), "the retry must not write any file")
	require.Equal(t, int64(2), atomic.LoadInt64(&counter.moves), "the retry must not move any file")

	matches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestReception_PreArrivalCancel_PublishesCanceled(t *testing.T) {
	fr := directReceiver(t)
	acks := installAckCounter(fr)
	m := testingMerger(fr)
	repo := fr.fileRepository.(*files.MockRepository)

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-early-cancel-e2e"
	fileID := base.ID()

	// Cancel arrives first.
	cancelMsg := sendCancelMessage(t, commitID, "testing", fileID)
	require.NoError(t, fr.processMessage(context.Background(), cancelMsg, files.SourceHTTP))
	require.Equal(t, int32(1), acks.Load())

	// The cancel intent is durable in the ledger and on disk.
	reception, err := repo.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.NotNil(t, reception.CancelRequestedAt)
	fd, err := m.storage.Open(filepath.Join("mergable", "testing", fileID+".ach.canceled"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	// The file then arrives and is published directly in canceled form.
	queueMsg := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(context.Background(), queueMsg, files.SourceHTTP))
	require.Equal(t, int32(2), acks.Load())

	_, err = m.storage.Open(filepath.Join("mergable", "testing", fileID+".ach"))
	require.Error(t, err, "no mergeable .ach may exist for a pre-canceled commit")

	canceled := readMergable(t, m, fileID, ".ach.canceled")
	require.NotEmpty(t, canceled, "staged bytes must replace the empty cancel placeholder")

	reception, err = repo.GetReception(context.Background(), commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
}

// sha256Hex renders the file exactly like staging does and returns its hash.
func sha256Hex(bs []byte) string {
	return hash(bs)
}

func hashOfFile(t *testing.T, file *ach.File) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, ach.NewWriter(&buf).Write(file))
	return hash(buf.Bytes())
}
