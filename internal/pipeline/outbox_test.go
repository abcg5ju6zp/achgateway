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
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base"

	"github.com/stretchr/testify/require"
)

func TestOutbox_FailingEmitterRetriesWithoutRewriting(t *testing.T) {
	fr := directReceiver(t) // background outbox is stopped; drive pumps manually
	ctx := context.Background()

	m := testingMerger(fr)
	counter := &countingChest{Chest: m.storage}
	m.storage = counter

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	commitID := "commit-outbox-fail"
	fileID := base.ID()

	emitter := &failingEmitter{err: errors.New("events stream down")}
	fr.eventEmitter = emitter

	msg := sendQueueMessage(t, commitID, "testing", fileID, file)
	require.NoError(t, fr.processMessage(ctx, msg, files.SourceStream))

	// Published but not notified while the emitter is down. Publishing a
	// queue result never touches the emitter inline — the outbox owns that.
	reception, err := fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Equal(t, files.StagePublished, reception.Stage)
	require.Nil(t, reception.NotifiedAt)

	// Failed notification attempts touch no files at all.
	beforeWrites := atomic.LoadInt64(&counter.writes)
	beforeMoves := atomic.LoadInt64(&counter.moves)
	require.Equal(t, 0, fr.pumpOutbox(ctx))
	require.Equal(t, beforeWrites, atomic.LoadInt64(&counter.writes))
	require.Equal(t, beforeMoves, atomic.LoadInt64(&counter.moves))

	reception, err = fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.Nil(t, reception.NotifiedAt, "notification failure keeps the result pending")

	// Emitter recovers: the same frozen result goes out exactly once and the
	// row is marked notified.
	fr.eventEmitter = fr.Events
	require.Equal(t, 1, fr.pumpOutbox(ctx))
	reception, err = fr.fileRepository.GetReception(ctx, commitID)
	require.NoError(t, err)
	require.NotNil(t, reception.NotifiedAt)

	require.Len(t, fr.Events.Sent(), 1)
	sent := fr.Events.Sent()[0]
	resp, ok := sent.Event.(*models.QueueACHFileResponse)
	require.True(t, ok, "expected QueueACHFileResponse event, got %T", sent.Event)
	require.Equal(t, commitID, resp.CommitID)
	require.Equal(t, fileID, resp.FileID)

	// A subsequent sweep has nothing left to send.
	require.Equal(t, 0, fr.pumpOutbox(ctx))
}

func TestOutbox_FrozenResultRoundTrip(t *testing.T) {
	fr := directReceiver(t)
	agg := fr.shardAggregators["testing"]

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)
	msg := incoming.ACHFile{CommitID: "commit-roundtrip", FileID: "f-rt", ShardKey: "testing", File: file}

	acceptedAt := receptionAcceptedAt()
	original := agg.buildQueueResponse(msg, msg.CommitID, acceptedAt, "")
	raw, err := freezeResult(original)
	require.NoError(t, err)

	var decoded incoming.QueueACHFileResponse
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, original.CommitID, decoded.CommitID)
	require.Equal(t, original.FileID, decoded.FileID)
	require.Equal(t, original.ShardKey, decoded.ShardKey)
	require.True(t, original.AcceptedAt.Equal(*decoded.AcceptedAt))
	require.True(t, original.NextCutoff.Equal(*decoded.NextCutoff))
}

// receptionAcceptedAt returns a fixed time so nextCutoff computation stays
// deterministic for the round-trip test.
func receptionAcceptedAt() time.Time {
	return time.Date(2026, time.October, 14, 12, 0, 0, 0, time.UTC)
}
