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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"testing"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/service"

	"github.com/stretchr/testify/require"
)

func newStagingMerger(t *testing.T, shardName string) (*filesystemMerging, string) {
	t.Helper()

	merging, dir := testMergingSetup(t, service.Shard{Name: shardName})
	return merging.(*filesystemMerging), dir
}

func readStorageHash(t *testing.T, m *filesystemMerging, path string) string {
	t.Helper()
	fd, err := m.storage.Open(path)
	require.NoError(t, err)
	defer fd.Close()
	bs, err := io.ReadAll(fd)
	require.NoError(t, err)
	sum := sha256.Sum256(bs)
	return hex.EncodeToString(sum[:])
}

func TestStageXfer_then_PublishXfer(t *testing.T) {
	m, _ := newStagingMerger(t, "SD-stage")

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)

	xfer := incoming.ACHFile{FileID: "stage-1.ach", ShardKey: "SD-stage", File: file}
	const commitID = "commit-stage-1"

	hashSum, err := m.StageXfer(context.Background(), xfer, commitID)
	require.NoError(t, err)
	require.NotEmpty(t, hashSum)

	stagedACH, _ := m.stagingPaths(xfer, commitID)
	require.Equal(t, filepath.Join("staging", "SD-stage", commitID, "stage-1.ach"), stagedACH)
	require.Equal(t, hashSum, readStorageHash(t, m, stagedACH), "staged content hash mismatch")

	// Nothing is visible in mergable before publication.
	_, err = m.storage.Open(filepath.Join("mergable", "SD-stage", "stage-1.ach"))
	require.Error(t, err)

	staged, err := m.ListStaged(context.Background())
	require.NoError(t, err)
	require.Len(t, staged, 1)
	require.Equal(t, commitID, staged[0].CommitID)
	require.Equal(t, stagedACH, staged[0].ACHPath)
	require.Empty(t, staged[0].OptsPath, "ppd-debit.ach carries no ValidateOpts")

	require.NoError(t, m.PublishXfer(context.Background(), xfer, commitID))

	// Staging is gone and the mergable file holds identical bytes.
	_, err = m.storage.Open(stagedACH)
	require.Error(t, err)
	require.Equal(t, hashSum, readStorageHash(t, m, filepath.Join("mergable", "SD-stage", "stage-1.ach")))

	staged, err = m.ListStaged(context.Background())
	require.NoError(t, err)
	require.Empty(t, staged)
}

func TestStageXfer_ValidateOptsMoved(t *testing.T) {
	m, _ := newStagingMerger(t, "SD-opts-stage")

	file := ach.NewFile()
	file.SetHeader(staticFileHeader())
	file.SetValidation(&ach.ValidateOpts{CustomReturnCodes: true})
	batch := ach.NewBatchPPD(mockBatchPPDHeader())
	entry := mockPPDEntryDetail()
	entry.SetTraceNumber(mockBatchPPDHeader().ODFIIdentification, 9)
	batch.AddEntry(entry)
	require.NoError(t, batch.Create())
	file.AddBatch(batch)
	require.NoError(t, file.Create())

	xfer := incoming.ACHFile{FileID: "opts.ach", ShardKey: "SD-opts-stage", File: file}
	const commitID = "commit-opts"

	_, err := m.StageXfer(context.Background(), xfer, commitID)
	require.NoError(t, err)

	_, stagedOpts := m.stagingPaths(xfer, commitID)
	fd, err := m.storage.Open(stagedOpts)
	require.NoError(t, err, "ValidateOpts sidecar should be staged")
	require.NoError(t, fd.Close())

	require.NoError(t, m.PublishXfer(context.Background(), xfer, commitID))

	_, err = m.storage.Open(stagedOpts)
	require.Error(t, err, "staged opts must move")
	fd, err = m.storage.Open(filepath.Join("mergable", "SD-opts-stage", "opts.json"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
}

func TestPublishXfer_PreArrivalCancel(t *testing.T) {
	m, _ := newStagingMerger(t, "SD-cancel-stage")

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)

	xfer := incoming.ACHFile{FileID: "early-cancel.ach", ShardKey: "SD-cancel-stage", File: file}
	const commitID = "commit-early-cancel"

	// Simulate HandleCancel's pre-arrival placeholder: an empty
	// mergable/<id>.ach.canceled marker.
	require.NoError(t, m.storage.WriteFile(filepath.Join("mergable", "SD-cancel-stage", "early-cancel.ach.canceled"), nil))

	_, err = m.StageXfer(context.Background(), xfer, commitID)
	require.NoError(t, err)
	require.NoError(t, m.PublishXfer(context.Background(), xfer, commitID))

	// No mergeable .ach is produced; the canceled target holds the bytes.
	_, err = m.storage.Open(filepath.Join("mergable", "SD-cancel-stage", "early-cancel.ach"))
	require.Error(t, err)
	fd, err := m.storage.Open(filepath.Join("mergable", "SD-cancel-stage", "early-cancel.ach.canceled"))
	require.NoError(t, err)
	bs, err := io.ReadAll(fd)
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	require.NotEmpty(t, bs, "canceled placeholder must be replaced by staged content")
}

func TestCleanupStaging_Isolated(t *testing.T) {
	m, _ := newStagingMerger(t, "SD-cleanup")

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)

	one := incoming.ACHFile{FileID: "one.ach", ShardKey: "SD-cleanup", File: file}
	two := incoming.ACHFile{FileID: "two.ach", ShardKey: "SD-cleanup", File: file}

	_, err = m.StageXfer(context.Background(), one, "commit-one")
	require.NoError(t, err)
	_, err = m.StageXfer(context.Background(), two, "commit-two")
	require.NoError(t, err)

	// A previously published file must be untouched by staging cleanup.
	require.NoError(t, m.storage.WriteFile(filepath.Join("mergable", "SD-cleanup", "old.ach"), []byte("published")))

	require.NoError(t, m.CleanupStaging(context.Background(), "commit-one"))

	_, err = m.storage.Open(filepath.Join("staging", "SD-cleanup", "commit-one", "one.ach"))
	require.Error(t, err)
	fd, err := m.storage.Open(filepath.Join("staging", "SD-cleanup", "commit-two", "two.ach"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	fd, err = m.storage.Open(filepath.Join("mergable", "SD-cleanup", "old.ach"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())
}

func TestPublishXfer_MissingStagingErrors(t *testing.T) {
	m, _ := newStagingMerger(t, "SD-missing")

	file, err := ach.ReadFile(filepath.Join("..", "..", "testdata", "ppd-debit.ach"))
	require.NoError(t, err)

	xfer := incoming.ACHFile{FileID: "ghost.ach", ShardKey: "SD-missing", File: file}
	err = m.PublishXfer(context.Background(), xfer, "never-staged")
	require.Error(t, err)
}
