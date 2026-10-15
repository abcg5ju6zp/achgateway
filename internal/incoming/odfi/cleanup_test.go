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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base/log"

	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *consumptionStore {
	t.Helper()
	store, err := newConsumptionStore(t.TempDir())
	require.NoError(t, err)
	return store
}

// commitTestUnit stages and commits a downloaded file into the consumption
// store and fast-forwards it through validation and audit so it is eligible
// for remote deletion.
func commitTestUnit(t *testing.T, store *consumptionStore, host, kindDir, name string, content []byte) *consumptionUnit {
	t.Helper()

	staging := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(staging, content, 0600))

	remotePath := filepath.ToSlash(filepath.Join(kindDir, name))
	u, err := store.CommitDownload(host, remotePath, kindDir, staging)
	require.NoError(t, err)

	require.NoError(t, u.markValidated(nil))
	if len(content) == 0 {
		require.NoError(t, u.skipAudited("empty file"))
	} else {
		require.NoError(t, u.markAudited("odfi/"+host+"/"+name))
	}
	return u
}

func testDownloadedFiles(t *testing.T, store *consumptionStore, host string, units ...*consumptionUnit) *downloadedFiles {
	t.Helper()
	dl := &downloadedFiles{
		dir:   t.TempDir(),
		store: store,
		host:  host,
	}
	for _, u := range units {
		dl.addUnit(u)
	}
	return dl
}

func TestCleanup_DeleteUncertainWhenRemoteStillPresent(t *testing.T) {
	agent := &upload.MockAgent{
		Err: errors.New("i/o timeout"),
		InboundFilepaths: []string{
			filepath.Join("inbound", "file.ach"),
		},
	}

	store := newTestStore(t)
	u := commitTestUnit(t, store, agent.Hostname(), agent.InboundPath(), "file.ach", []byte("data"))
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	// A delete error while the file is still listed remotely is uncertain.
	err := Cleanup(context.Background(), log.NewTestLogger(), agent, dl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "uncertain")

	require.Equal(t, filepath.Join("inbound", "file.ach"), agent.DeletedFile)

	reloaded, err := store.loadLocked(u.dir)
	require.NoError(t, err)
	require.Equal(t, stageUncertain, reloaded.state.RemoteDeleted.Status)
	require.FileExists(t, filepath.Join(u.dir, consumptionErrorFile))
}

func TestCleanup_MissingUnits(t *testing.T) {
	agent := &upload.MockAgent{}
	store := newTestStore(t)
	dl := testDownloadedFiles(t, store, agent.Hostname())

	err := Cleanup(context.Background(), log.NewTestLogger(), agent, dl)
	require.NoError(t, err)
}

func TestCleanup_UnfinishedUnitKeepsRemote(t *testing.T) {
	agent := &upload.MockAgent{}
	store := newTestStore(t)

	// Unit is downloaded but never validated - the remote file must stay.
	staging := filepath.Join(t.TempDir(), "file.ach")
	require.NoError(t, os.WriteFile(staging, []byte("data"), 0600))
	u, err := store.CommitDownload(agent.Hostname(), "inbound/file.ach", agent.InboundPath(), staging)
	require.NoError(t, err)
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	require.NoError(t, Cleanup(context.Background(), log.NewTestLogger(), agent, dl))
	require.Empty(t, agent.DeletedFile)
}

func Test_CleanupEmptyFiles_InboundPath_Success(t *testing.T) {
	agent := &upload.MockAgent{
		InboundFilepaths: []string{filepath.Join("inbound", "empty_file.ach")},
	}
	store := newTestStore(t)
	u := commitTestUnit(t, store, agent.Hostname(), agent.InboundPath(), "empty_file.ach", nil)
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	err := CleanupEmptyFiles(context.Background(), log.NewTestLogger(), agent, dl)
	require.NoError(t, err)

	require.Equal(t, filepath.Join("inbound", "empty_file.ach"), agent.DeletedFile)
}

func Test_CleanupEmptyFiles_ReconciliationPath_Success(t *testing.T) {
	agent := &upload.MockAgent{
		ReconciliationFilepaths: []string{filepath.Join("reconciliation", "empty_file.ach")},
	}
	store := newTestStore(t)
	u := commitTestUnit(t, store, agent.Hostname(), agent.ReconciliationPath(), "empty_file.ach", nil)
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	err := CleanupEmptyFiles(context.Background(), log.NewTestLogger(), agent, dl)
	require.NoError(t, err)

	require.Equal(t, filepath.Join("reconciliation", "empty_file.ach"), agent.DeletedFile)
}

func Test_CleanupEmptyFiles_ReturnPath_Success(t *testing.T) {
	agent := &upload.MockAgent{
		ReturnFilepaths: []string{filepath.Join("return", "empty_file.ach")},
	}
	store := newTestStore(t)
	u := commitTestUnit(t, store, agent.Hostname(), agent.ReturnPath(), "empty_file.ach", nil)
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	err := CleanupEmptyFiles(context.Background(), log.NewTestLogger(), agent, dl)
	require.NoError(t, err)

	require.Equal(t, filepath.Join("return", "empty_file.ach"), agent.DeletedFile)
}

func Test_CleanupEmptyFiles_PopulatedFile(t *testing.T) {
	agent := &upload.MockAgent{
		InboundFilepaths: []string{filepath.Join("inbound", "file.ach")},
	}
	store := newTestStore(t)
	u := commitTestUnit(t, store, agent.Hostname(), agent.InboundPath(), "file.ach", []byte("sample data"))
	dl := testDownloadedFiles(t, store, agent.Hostname(), u)

	err := CleanupEmptyFiles(context.Background(), log.NewTestLogger(), agent, dl)
	require.NoError(t, err)

	require.Empty(t, agent.DeletedFile, "expected no deleted files")
}
