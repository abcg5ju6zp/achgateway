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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moov-io/achgateway/internal/storage"
	"github.com/stretchr/testify/require"
)

func newTestChest(t *testing.T) storage.Chest {
	t.Helper()
	chest, err := storage.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	return chest
}

func TestBatch_Identities(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	window := time.Date(2026, 10, 14, 15, 0, 0, 0, ny)

	require.Equal(t, "live-20261014-1500", automatedBatchID("live", window))
	// Deterministic across calls.
	require.Equal(t, automatedBatchID("live", window), automatedBatchID("live", window))

	// Same identity regardless of seconds within the window.
	require.Equal(t, automatedBatchID("live", window), automatedBatchID("live", window.Add(59*time.Second)))

	manual := time.Date(2026, 10, 14, 14, 30, 5, 0, ny)
	require.Equal(t, "live-manual-20261014-143005", manualBatchID("live", manual))

	require.Equal(t, "live-20261014-150000", isolatedDirName("live", time.Date(2026, 10, 14, 15, 0, 0, 0, ny)))
}

func TestBatch_PersistAndLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	chest := newTestChest(t)

	dir := "live-20261014-150000"
	require.NoError(t, chest.MkdirAll(dir))

	state := BatchState{
		ID:     "live-20261014-1500",
		Shard:  "live",
		Window: "15:00",
		Kind:   BatchKindAutomated,
		Dir:    dir,
		Inputs: []BatchInput{
			{Filename: "a.ach", Size: 10, SHA256: "aaa"},
			{Filename: "b.ach", Size: 20, SHA256: "bbb"},
		},
		Outputs: []BatchOutput{
			{
				Index:        0,
				Filename:     "20261014-150000-987654320-0.ach",
				RemotePath:   filepath.Join("outbound", "20261014-150000-987654320-0.ach"),
				Uploaded:     true,
				Verified:     true,
				UploadedAt:   time.Date(2026, 10, 14, 15, 0, 1, 0, time.UTC),
				Audited:      true,
				ArtifactPath: filepath.Join(dir, batchArtifactDir, "0-x.ach"),
				Inputs:       []int{0, 1},
			},
		},
	}
	b := newBatch(chest, state)
	require.NoError(t, b.Persist(ctx))

	// Atomic commit leaves no temp file behind.
	entries, err := chest.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for i := range entries {
		names = append(names, entries[i].Name())
	}
	for _, name := range names {
		require.NotEqual(t, batchManifestFile+".tmp", name)
		require.False(t, strings.HasPrefix(name, ".batch."), "no staging temp must remain: %s", name)
	}
	require.Contains(t, names, batchManifestFile)

	loaded, err := loadBatch(ctx, chest, dir)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	got := loaded.State()
	require.Equal(t, "live-20261014-1500", got.ID)
	require.Equal(t, batchStatusActive, got.Status)
	require.Len(t, got.Inputs, 2)
	require.Len(t, got.Outputs, 1)
	require.Equal(t, []int{0, 1}, got.Outputs[0].Inputs)
	require.True(t, got.Outputs[0].Verified)
	require.False(t, got.CreatedAt.IsZero())
}

func TestLoadBatch_MissingManifest(t *testing.T) {
	ctx := context.Background()
	chest := newTestChest(t)
	require.NoError(t, chest.MkdirAll("legacy-20261014-150000"))

	b, err := loadBatch(ctx, chest, "legacy-20261014-150000")
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestBatch_InitializeOutputsMapsBothDirections(t *testing.T) {
	b := newBatch(newTestChest(t), BatchState{
		ID: "x", Dir: "x",
		Inputs: []BatchInput{{Filename: "a.ach"}, {Filename: "b.ach"}, {Filename: "c.ach"}},
	})
	b.InitializeOutputs([]BatchOutput{
		{Index: 0, Inputs: []int{0, 1}},
		{Index: 1, Inputs: []int{2}},
	})

	st := b.State()
	require.Equal(t, 0, st.Inputs[0].OutputIndex)
	require.Equal(t, 0, st.Inputs[1].OutputIndex)
	require.Equal(t, 1, st.Inputs[2].OutputIndex)

	// Resuming with outputs already recorded must not overwrite them.
	b.InitializeOutputs([]BatchOutput{{Index: 0, Filename: "must-not-win.ach"}})
	require.Empty(t, b.State().Outputs[0].Filename)
}

func TestBatch_CompletionTruthTable(t *testing.T) {
	base := func() *Batch {
		return newBatch(newTestChest(t), BatchState{
			ID: "x", Dir: "x",
			Inputs:  []BatchInput{{Filename: "a.ach", OutputIndex: 0}},
			Outputs: []BatchOutput{{Index: 0}},
		})
	}

	t.Run("nothing done", func(t *testing.T) {
		require.False(t, base().IsComplete())
	})
	t.Run("uploaded not audited", func(t *testing.T) {
		b := base()
		b.MarkUploaded(0, false, time.Now())
		require.False(t, b.IsComplete())
	})
	t.Run("audited but event missing", func(t *testing.T) {
		b := base()
		b.MarkUploaded(0, false, time.Now())
		b.MarkAudited(0)
		require.False(t, b.IsComplete())
	})
	t.Run("all stages done", func(t *testing.T) {
		b := base()
		b.MarkUploaded(0, false, time.Now())
		b.MarkAudited(0)
		b.MarkEventsSent([]int{0})
		require.True(t, b.IsComplete())
		require.NoError(t, b.MarkCompleted())
		require.Equal(t, batchStatusCompleted, b.State().Status)
	})
	t.Run("conflict never completes", func(t *testing.T) {
		b := base()
		b.MarkPrepared(0, BatchOutput{Index: 0})
		b.MarkUploaded(0, false, time.Now())
		b.MarkAudited(0)
		b.MarkEventsSent([]int{0})
		b.MarkConflict(0)
		require.False(t, b.IsComplete())
		require.Error(t, b.MarkCompleted())
		require.True(t, b.HasConflict())
	})
	t.Run("unmapped input does not block completion", func(t *testing.T) {
		b := newBatch(newTestChest(t), BatchState{
			ID: "x", Dir: "x",
			Inputs: []BatchInput{
				{Filename: "a.ach", OutputIndex: 0},
				{Filename: "orphan.ach", OutputIndex: unmappedOutput},
			},
			Outputs: []BatchOutput{{Index: 0}},
		})
		b.MarkUploaded(0, false, time.Now())
		b.MarkAudited(0)
		b.MarkEventsSent([]int{0})
		require.True(t, b.IsComplete())
	})
}

func TestBatch_MarkPreparedNeverRenames(t *testing.T) {
	b := newBatch(newTestChest(t), BatchState{ID: "x", Dir: "x"})

	first := BatchOutput{
		Index:        0,
		Filename:     "20261014-150000-987654320-0.ach",
		RemotePath:   "outbound/20261014-150000-987654320-0.ach",
		AuditPath:    "outbound/host/2026-10-14/20261014-150000-987654320-0.ach",
		ArtifactPath: "x/outbox/0-a.ach",
		SHA256:       "first",
	}
	b.MarkPrepared(0, first)
	b.Persist(context.Background())

	// A retry with freshly rendered names must not change the fixed facts.
	b.MarkPrepared(0, BatchOutput{
		Index:        0,
		Filename:     "20261014-160000-987654320-0.ach",
		RemotePath:   "outbound/20261014-160000-987654320-0.ach",
		AuditPath:    "outbound/host/2026-10-15/20261014-160000-987654320-0.ach",
		ArtifactPath: "x/outbox/0-b.ach",
		SHA256:       "second",
	})
	out := b.State().Outputs[0]
	require.Equal(t, first.Filename, out.Filename)
	require.Equal(t, first.RemotePath, out.RemotePath)
	require.Equal(t, first.AuditPath, out.AuditPath)
	require.Equal(t, first.ArtifactPath, out.ArtifactPath)
	require.Equal(t, "first", out.SHA256)
}

func TestBatch_Artifacts(t *testing.T) {
	ctx := context.Background()
	chest := newTestChest(t)
	b := newBatch(chest, BatchState{ID: "x-20261014-1500", Shard: "x", Dir: "x-20261014-150000"})
	require.NoError(t, b.Persist(ctx))

	rel, err := b.WriteArtifact("0-20261014-150000-987654320-0.ach", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join("x-20261014-150000", batchArtifactDir, "0-20261014-150000-987654320-0.ach"), rel)

	got, err := b.ReadArtifact(rel)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestActiveBatches_ScanAndFind(t *testing.T) {
	ctx := context.Background()
	chest := newTestChest(t)

	// Legacy isolated dir without a manifest.
	require.NoError(t, chest.MkdirAll("live-20261013-150000"))

	// Completed batch must be ignored.
	completed := newBatch(chest, BatchState{ID: "live-20261012-1500", Shard: "live", Dir: "live-20261012-150000", Status: batchStatusCompleted})
	require.NoError(t, completed.Persist(ctx))

	oldest := newBatch(chest, BatchState{ID: "live-20261014-1400", Shard: "live", Dir: "live-20261014-140000", CreatedAt: time.Date(2026, 10, 14, 14, 0, 0, 0, time.UTC)})
	require.NoError(t, oldest.Persist(ctx))
	newest := newBatch(chest, BatchState{ID: "live-20261014-1500", Shard: "live", Dir: "live-20261014-150000", CreatedAt: time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC)})
	require.NoError(t, newest.Persist(ctx))

	// Another shard's active batch must not surface.
	other := newBatch(chest, BatchState{ID: "other-20261014-1500", Shard: "other", Dir: "other-20261014-150000"})
	require.NoError(t, other.Persist(ctx))

	got, err := activeBatches(ctx, chest, "live")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "live-20261014-1400", got[0].ID(), "oldest first")
	require.Equal(t, "live-20261014-1500", got[1].ID())

	found, err := findActiveBatchID(ctx, chest, "live", "live-20261014-1500")
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, "live-20261014-150000", found.Dir())

	found, err = findActiveBatchID(ctx, chest, "live", "live-20990101-0000")
	require.NoError(t, err)
	require.Nil(t, found)

	latest, err := newestActiveBatch(ctx, chest, "live")
	require.NoError(t, err)
	require.Equal(t, "live-20261014-1500", latest.ID())
}

func TestUniqueIsolatedDir_AvoidsSameSecondCollision(t *testing.T) {
	chest := newTestChest(t)

	// First cutoff occupies the name (with a manifest).
	first := "live-20261014-150000"
	b := newBatch(chest, BatchState{ID: "live-20261014-1500", Shard: "live", Dir: first, Outputs: []BatchOutput{{Index: 0}}})
	require.NoError(t, b.Persist(context.Background()))

	second := uniqueIsolatedDir(chest, first)
	require.Equal(t, first+"-1", second)
	require.NoError(t, chest.ReplaceDir(filepath.Join("mergable", "live"), second))

	// Original manifest must survive the second isolation.
	loaded, err := loadBatch(context.Background(), chest, first)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, "live-20261014-1500", loaded.ID())
}

func TestBatch_MarkEventsSentConcurrent(t *testing.T) {
	const n = 200
	inputs := make([]BatchInput, n)
	for i := range inputs {
		inputs[i] = BatchInput{Filename: "x", OutputIndex: 0}
	}
	b := newBatch(newTestChest(t), BatchState{ID: "x", Dir: "x", Inputs: inputs})

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.MarkEventsSent([]int{idx})
		}()
	}
	wg.Wait()
	st := b.State()
	require.Len(t, st.Inputs, n)
	for i := range st.Inputs {
		require.True(t, st.Inputs[i].EventSent, "input %d", i)
	}
}
