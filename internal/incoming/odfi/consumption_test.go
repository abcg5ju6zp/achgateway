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

package odfi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/moov-io/base/log"

	"github.com/stretchr/testify/require"
)

func TestConsumptionID_StableAndUnique(t *testing.T) {
	id := consumptionID("host", "inbound/a.ach", "digest")

	// Same inputs produce the same identity.
	require.Equal(t, id, consumptionID("host", "/inbound/a.ach", "digest"))
	require.Equal(t, id, consumptionID("host", "inbound//a.ach", "digest"))

	// Host, path and content digest each participate in the identity.
	require.NotEqual(t, id, consumptionID("other", "inbound/a.ach", "digest"))
	require.NotEqual(t, id, consumptionID("host", "return/a.ach", "digest"))
	require.NotEqual(t, id, consumptionID("host", "inbound/a.ach", "other"))
}

func TestStore_CommitDownload(t *testing.T) {
	baseDir := t.TempDir()
	store, err := newConsumptionStore(baseDir)
	require.NoError(t, err)

	staged := filepath.Join(t.TempDir(), "a.ach")
	require.NoError(t, os.WriteFile(staged, []byte("ach contents"), 0600))

	u, err := store.CommitDownload("host:22", "inbound/a.ach", "inbound/", staged)
	require.NoError(t, err)
	require.Equal(t, "host:22", u.state.Host)
	require.Equal(t, "inbound/a.ach", u.state.RemotePath)
	require.Equal(t, "a.ach", u.state.Filename)
	require.Equal(t, int64(len("ach contents")), u.state.Size)
	require.Equal(t, stageConfirmed, u.state.Downloaded.Status)

	// Slot layout isolates host, path and content.
	require.FileExists(t, filepath.Join(u.dir, consumptionDataFile))
	require.FileExists(t, filepath.Join(u.dir, consumptionStateFile))
	require.Contains(t, u.dir, filepath.Join(consumptionDirName, "host%3A22", "inbound", "a.ach", u.state.SHA256))

	// The staged file is gone.
	_, err = os.Stat(staged)
	require.True(t, os.IsNotExist(err))

	// Downloaded bytes are preserved verbatim.
	data, err := os.ReadFile(filepath.Join(u.dir, consumptionDataFile))
	require.NoError(t, err)
	require.Equal(t, "ach contents", string(data))
}

func TestStore_CommitDownload_SameNameDifferentDirectoriesIsolated(t *testing.T) {
	store := newTestStore(t)

	stage := func(remotePath string) *consumptionUnit {
		staging := filepath.Join(t.TempDir(), filepath.Base(remotePath))
		require.NoError(t, os.WriteFile(staging, []byte("contents"), 0600))
		kind := filepath.Dir(remotePath) + "/"
		u, err := store.CommitDownload("host", remotePath, kind, staging)
		require.NoError(t, err)
		return u
	}

	inbound := stage("inbound/dup.ach")
	returned := stage("return/dup.ach")

	require.NotEqual(t, inbound.state.ID, returned.state.ID)
	require.NotEqual(t, inbound.dir, returned.dir)
	require.FileExists(t, filepath.Join(inbound.dir, consumptionDataFile))
	require.FileExists(t, filepath.Join(returned.dir, consumptionDataFile))
}

func TestStore_CommitDownload_DifferentContentVersions(t *testing.T) {
	store := newTestStore(t)

	commit := func(contents string) *consumptionUnit {
		staging := filepath.Join(t.TempDir(), "a.ach")
		require.NoError(t, os.WriteFile(staging, []byte(contents), 0600))
		u, err := store.CommitDownload("host", "inbound/a.ach", "inbound/", staging)
		require.NoError(t, err)
		return u
	}

	first := commit("v1")
	second := commit("v1")
	require.Equal(t, first.state.ID, second.state.ID, "identical content reuses the slot")

	changed := commit("v2")
	require.NotEqual(t, first.state.ID, changed.state.ID, "changed content is a new consumption unit")
	require.FileExists(t, filepath.Join(first.dir, consumptionDataFile), "the prior version is isolated")
}

func TestStore_ResumeIncompleteAcrossRestart(t *testing.T) {
	baseDir := t.TempDir()

	store, err := newConsumptionStore(baseDir)
	require.NoError(t, err)

	stage := func(name string) (string, string) {
		staging := filepath.Join(t.TempDir(), name)
		require.NoError(t, os.WriteFile(staging, []byte("contents"), 0600))
		return name, staging
	}

	name, staging := stage("pending.ach")
	pending, err := store.CommitDownload("host", "inbound/"+name, "inbound/", staging)
	require.NoError(t, err)
	require.NoError(t, pending.markValidated(nil))

	name, staging = stage("done.ach")
	done, err := store.CommitDownload("host", "inbound/"+name, "inbound/", staging)
	require.NoError(t, err)
	require.NoError(t, done.markValidated(nil))
	require.NoError(t, done.skipAudited("empty"))
	require.NoError(t, done.setExpectedProcessors([]string{"mock"}))
	require.NoError(t, done.markProcessorConfirmed("mock"))
	require.NoError(t, done.markRemoteDeleted(stageConfirmed, ""))

	// Simulate a process restart with a fresh store over the same directory.
	reopened, err := newConsumptionStore(baseDir)
	require.NoError(t, err)

	pendingUnits, err := reopened.ListIncomplete("host")
	require.NoError(t, err)
	var ids []string
	for _, u := range pendingUnits {
		ids = append(ids, u.state.ID)
	}
	require.ElementsMatch(t, []string{pending.state.ID}, ids)
	require.Equal(t, stageConfirmed, pendingUnits[0].state.Validated.Status)
}

func TestStore_RemoveComplete_RetainsFailedEvidence(t *testing.T) {
	store := newTestStore(t)

	failed := commitTestUnit(t, store, "host", "inbound/", "failed.ach", []byte("data"))
	require.NoError(t, failed.markProcessorFailed("mock", errSentinel))

	done := commitTestUnit(t, store, "host", "inbound/", "done.ach", []byte("data"))
	require.NoError(t, done.setExpectedProcessors(nil))
	require.NoError(t, done.markRemoteDeleted(stageSkipped, "kept"))

	require.NoError(t, store.RemoveComplete("host"))

	require.NoDirExists(t, done.dir)
	require.DirExists(t, failed.dir)
	require.FileExists(t, filepath.Join(failed.dir, consumptionDataFile))
	require.FileExists(t, filepath.Join(failed.dir, consumptionErrorFile))
}

func TestStore_Results(t *testing.T) {
	store := newTestStore(t)
	u := commitTestUnit(t, store, "host", "inbound/", "a.ach", []byte("data"))

	results, err := store.Results("")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, u.state.ID, results[0].ID)

	missing, err := store.Results("does-not-exist")
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestSweepStagingDirs(t *testing.T) {
	baseDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "download-stale"), 0750))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "download-stale", "x"), []byte("x"), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "consumption"), 0750))
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "keep"), 0750))

	sweepStagingDirs(log.NewTestLogger(), baseDir)

	require.NoDirExists(t, filepath.Join(baseDir, "download-stale"))
	require.DirExists(t, filepath.Join(baseDir, "consumption"))
	require.DirExists(t, filepath.Join(baseDir, "keep"))
}
