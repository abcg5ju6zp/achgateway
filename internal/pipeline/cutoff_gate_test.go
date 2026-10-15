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
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/upload"

	"github.com/stretchr/testify/require"
)

func gatedMerger(t *testing.T, repo files.Repository) (*filesystemMerging, string) {
	t.Helper()
	merging, dir := testMergingSetup(t, service.Shard{Name: "testing", UploadAgent: "mock"})
	m := merging.(*filesystemMerging)
	m.publishedFileIDs = repo.ListPublishedFileIDs
	return m, dir
}

func noUploadTransform() func(context.Context, int, upload.Agent, *ach.File) (string, error) {
	return func(ctx context.Context, index int, agent upload.Agent, file *ach.File) (string, error) {
		// Produce no uploaded filename: the gate is what we test here.
		return "", nil
	}
}

// optsFile builds a valid file that carries explicit ValidateOpts, so staging
// writes a .json sidecar alongside it.
func optsFile(t *testing.T) *ach.File {
	t.Helper()
	file, err := ach.ReadFile("../../testdata/ppd-debit.ach")
	require.NoError(t, err)
	file.SetValidation(&ach.ValidateOpts{CustomReturnCodes: true})
	return file
}

func TestCutoffGate_MergesOnlyPublished(t *testing.T) {
	repo := &files.MockRepository{}
	m, _ := gatedMerger(t, repo)
	ctx := context.Background()

	publishedID := "published-file"
	unpublishedID := "unpublished-file"
	canceledID := "canceled-file"

	// All three exist in mergable, but only the first is marked published.
	enqueuePPD(t, m, "testing", publishedID, 100, false, 1)
	enqueuePPD(t, m, "testing", unpublishedID, 100, false, 2)
	enqueuePPD(t, m, "testing", canceledID, 100, false, 3)

	now := time.Now().UTC()
	require.NoError(t, repo.Record(ctx, files.AcceptedFile{FileID: publishedID, ShardKey: "testing", AcceptedAt: now}))
	require.NoError(t, repo.MarkFilePublished(ctx, publishedID, now))
	require.NoError(t, repo.Record(ctx, files.AcceptedFile{FileID: unpublishedID, ShardKey: "testing", AcceptedAt: now}))
	// Canceled rows may be published; the disk marker excludes them either way.
	require.NoError(t, repo.Record(ctx, files.AcceptedFile{FileID: canceledID, ShardKey: "testing", AcceptedAt: now}))
	require.NoError(t, repo.MarkFilePublished(ctx, canceledID, now))
	_, err := m.HandleCancel(ctx, incoming.CancelACHFile{FileID: canceledID, ShardKey: "testing"})
	require.NoError(t, err)

	var transforms int32
	_, err = m.WithEachMerged(ctx, func(ctx context.Context, index int, agent upload.Agent, file *ach.File) (string, error) {
		atomic.AddInt32(&transforms, 1)
		return "", nil
	})
	require.NoError(t, err)

	// The unpublished file is returned to the live mergable directory and is
	// available to the next cutoff.
	matches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.Equal(t, unpublishedID+".ach", filepath.Base(matches[0].RelativePath))

	// The single published file was the only transform input.
	require.Greater(t, atomic.LoadInt32(&transforms), int32(0))

	published, err := repo.ListPublishedFileIDs(ctx)
	require.NoError(t, err)
	require.NotContains(t, published, unpublishedID, "the gate never promotes an unpublished index row")
}

func TestCutoffGate_UnpublishedReturnsValidateOpts(t *testing.T) {
	repo := &files.MockRepository{}
	m, _ := gatedMerger(t, repo)
	ctx := context.Background()

	// A file carrying explicit ValidateOpts writes both .ach and .json.
	file := optsFile(t)
	unpublishedID := "unpublished-opts"
	require.NoError(t, m.HandleXfer(ctx, incoming.ACHFile{
		FileID: unpublishedID, ShardKey: "testing", File: file,
	}))

	var transforms int32
	_, err := m.WithEachMerged(ctx, func(ctx context.Context, index int, agent upload.Agent, file *ach.File) (string, error) {
		atomic.AddInt32(&transforms, 1)
		return "", nil
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), atomic.LoadInt32(&transforms))

	// Both components are returned to the live mergable directory.
	achMatches, err := m.storage.Glob(filepath.Join("mergable", "testing", "*.ach"))
	require.NoError(t, err)
	require.Len(t, achMatches, 1)
	require.Equal(t, unpublishedID+".ach", filepath.Base(achMatches[0].RelativePath))

	fd, err := m.storage.Open(filepath.Join("mergable", "testing", unpublishedID+".json"))
	require.NoError(t, err, "ValidateOpts sidecar must be returned alongside the file")
	require.NoError(t, fd.Close())
}

func TestCutoffGate_AcceptsSuffixedIndexKey(t *testing.T) {
	repo := &files.MockRepository{}
	m, _ := gatedMerger(t, repo)
	ctx := context.Background()

	// The submission kept the .ach suffix in its index key (POST /files/{id}.ach).
	enqueuePPD(t, m, "testing", "suffixed.ach", 100, false, 1)
	require.NoError(t, repo.Record(ctx, files.AcceptedFile{
		FileID: "suffixed.ach", ShardKey: "testing", AcceptedAt: time.Now().UTC(),
	}))
	require.NoError(t, repo.MarkFilePublished(ctx, "suffixed.ach", time.Now().UTC()))

	var transforms int32
	_, err := m.WithEachMerged(ctx, func(ctx context.Context, index int, agent upload.Agent, file *ach.File) (string, error) {
		atomic.AddInt32(&transforms, 1)
		return "", nil
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&transforms), "suffixed index key must pass the gate")

	// The file was isolated for the cutoff instead of returned to mergable.
	roots, err := m.storage.ReadDir(".")
	require.NoError(t, err)
	var isolatedName string
	for _, entry := range roots {
		if strings.HasPrefix(entry.Name(), "testing-") {
			isolatedName = entry.Name()
		}
	}
	require.NotEmpty(t, isolatedName, "cutoff isolated directory missing")
	isolatedFiles, err := m.storage.ReadDir(isolatedName)
	require.NoError(t, err)
	var isolatedNames []string
	for _, entry := range isolatedFiles {
		isolatedNames = append(isolatedNames, entry.Name())
	}
	require.Contains(t, isolatedNames, "suffixed.ach")
}

func TestCutoffGate_FailsClosedWhenIndexUnavailable(t *testing.T) {
	repo := &files.MockRepository{}
	m, _ := gatedMerger(t, repo)
	ctx := context.Background()

	enqueuePPD(t, m, "testing", "whatever", 100, false, 1)
	repo.Err = errors.New("index down")

	var transforms int32
	_, err := m.WithEachMerged(ctx, func(ctx context.Context, index int, agent upload.Agent, file *ach.File) (string, error) {
		atomic.AddInt32(&transforms, 1)
		return "", nil
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "cutoff fails closed")
	require.Equal(t, int32(0), atomic.LoadInt32(&transforms), "no file may be merged/uploaded when the index is down")
}
