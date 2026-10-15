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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/alerting"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base/log"

	"github.com/stretchr/testify/require"
)

var errSentinel = errors.New("sentinel processor failure")

// testAgent is an in-memory upload Agent with call counters and configurable
// delete/listing failures.
type testAgent struct {
	*upload.MockAgent

	mu sync.Mutex

	remote map[string][]byte

	deleteErr error
	listErr   error

	deleteCalls []string
	listCalls   int
}

func newTestAgent(remote map[string][]byte) *testAgent {
	return &testAgent{
		MockAgent: &upload.MockAgent{},
		remote:    remote,
	}
}

func (a *testAgent) pathsFor(prefix string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.listCalls++
	if a.listErr != nil {
		return nil
	}
	var out []string
	for p := range a.remote {
		if filepath.Dir(p) == filepath.Clean(prefix) {
			out = append(out, p)
		}
	}
	return out
}

func (a *testAgent) GetInboundFiles(context.Context) ([]string, error) {
	if a.listErr != nil {
		a.mu.Lock()
		a.listCalls++
		a.mu.Unlock()
		return nil, a.listErr
	}
	return a.pathsFor(a.InboundPath()), nil
}

func (a *testAgent) GetReconciliationFiles(context.Context) ([]string, error) {
	if a.listErr != nil {
		a.mu.Lock()
		a.listCalls++
		a.mu.Unlock()
		return nil, a.listErr
	}
	return a.pathsFor(a.ReconciliationPath()), nil
}

func (a *testAgent) GetReturnFiles(context.Context) ([]string, error) {
	if a.listErr != nil {
		a.mu.Lock()
		a.listCalls++
		a.mu.Unlock()
		return nil, a.listErr
	}
	return a.pathsFor(a.ReturnPath()), nil
}

func (a *testAgent) ReadFile(_ context.Context, path string) (*upload.File, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	bs, ok := a.remote[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &upload.File{
		Filepath: filepath.Base(path),
		Contents: io.NopCloser(bytes.NewReader(bs)),
	}, nil
}

func (a *testAgent) Delete(_ context.Context, path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deleteCalls = append(a.deleteCalls, path)
	if a.deleteErr != nil {
		// Simulate an uncertain response: the call failed and the file remains.
		return a.deleteErr
	}
	delete(a.remote, path)
	return nil
}

func (a *testAgent) deleteCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.deleteCalls)
}

// countingProcessor records how many times Handle ran.
type countingProcessor struct {
	mu    sync.Mutex
	typ   string
	calls int
	err   error
}

func (p *countingProcessor) Type() string { return p.typ }

func (p *countingProcessor) Handle(_ context.Context, _ log.Logger, _ File) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.err
}

func (p *countingProcessor) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type countingAlerter struct {
	mu    sync.Mutex
	calls int
	errs  []error
}

func (a *countingAlerter) AlertError(err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.errs = append(a.errs, err)
	return nil
}

func (a *countingAlerter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func newTestDownloader(t *testing.T) Downloader {
	t.Helper()
	d, err := NewDownloader(log.NewTestLogger(), service.ODFIStorage{Directory: t.TempDir()})
	require.NoError(t, err)
	return d
}

func tickDownload(t *testing.T, ctx context.Context, d Downloader, agent upload.Agent) *downloadedFiles {
	t.Helper()
	dl, err := d.CopyFilesFromRemote(ctx, agent, &service.Shard{Name: "test"})
	require.NoError(t, err)
	t.Cleanup(func() { dl.deleteFiles() })
	return dl
}

func TestConsumption_NormalProcessing_DeletesOnce(t *testing.T) {
	ctx := context.Background()
	content, err := os.ReadFile(filepath.Join("testdata", "forward.ach"))
	require.NoError(t, err)

	agent := newTestAgent(map[string][]byte{
		"inbound/forward.ach": content,
	})
	d := newTestDownloader(t)
	proc := &countingProcessor{typ: "mock"}
	alerters, _ := alerting.NewAlerters(service.ErrorAlerting{})

	// First tick: download, process, delete.
	dl := tickDownload(t, ctx, d, agent)
	require.NoError(t, ProcessFiles(ctx, log.NewTestLogger(), dl, alerters, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent))
	require.NoError(t, Cleanup(ctx, log.NewTestLogger(), agent, dl))
	require.Equal(t, 1, proc.count())
	require.Equal(t, 1, agent.deleteCount())

	results, err := d.ConsumptionResults("")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, stageConfirmed, results[0].RemoteDeleted.Status)

	// A later tick sees a file at the same path again - it must not be
	// reprocessed or deleted while the consumption unit is already confirmed.
	agent.mu.Lock()
	agent.remote["inbound/forward.ach"] = content
	agent.mu.Unlock()
	dl2 := tickDownload(t, ctx, d, agent)
	require.NoError(t, ProcessFiles(ctx, log.NewTestLogger(), dl2, alerters, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent))
	require.NoError(t, Cleanup(ctx, log.NewTestLogger(), agent, dl2))
	require.Equal(t, 1, proc.count(), "confirmed processor must not run again")
	require.Equal(t, 1, agent.deleteCount(), "confirmed deletion must not run again")
}

func TestConsumption_ProcessorFailure_RetainsRemoteAndEvidenceThenRecovers(t *testing.T) {
	ctx := context.Background()
	content, err := os.ReadFile(filepath.Join("testdata", "forward.ach"))
	require.NoError(t, err)

	agent := newTestAgent(map[string][]byte{
		"inbound/forward.ach": content,
	})
	d := newTestDownloader(t)
	proc := &countingProcessor{typ: "mock", err: errSentinel}
	alerter := &countingAlerter{}

	// First attempt: processing fails.
	dl := tickDownload(t, ctx, d, agent)
	processErr := ProcessFiles(ctx, log.NewTestLogger(), dl, alerting.Alerters{alerter}, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent)
	require.Error(t, processErr)
	require.Contains(t, processErr.Error(), "mock")
	require.Equal(t, 1, alerter.count(), "processor errors keep their existing alert behavior")

	// The remote file is untouched and local evidence is retained.
	require.Equal(t, 0, agent.deleteCount())
	require.NoError(t, Cleanup(ctx, log.NewTestLogger(), agent, dl))
	require.Equal(t, 0, agent.deleteCount())
	units, err := dl.allUnits()
	require.NoError(t, err)
	require.Len(t, units, 1)
	require.FileExists(t, filepath.Join(units[0].dir, consumptionDataFile))
	require.FileExists(t, filepath.Join(units[0].dir, consumptionErrorFile))
	require.Equal(t, stageFailed, units[0].state.Processors["mock"].Status)

	// Recovery: the file is no longer listed (simulating a restart) but the
	// unfinished unit resumes. Only the failed processor runs again.
	agent.mu.Lock()
	agent.remote = map[string][]byte{}
	agent.mu.Unlock()
	proc.err = nil

	dl2 := tickDownload(t, ctx, d, agent)
	require.NoError(t, ProcessFiles(ctx, log.NewTestLogger(), dl2, alerting.Alerters{alerter}, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent))
	require.Equal(t, 2, proc.count())
	require.Equal(t, 1, alerter.count(), "no additional alerts on recovery")
	require.NoError(t, Cleanup(ctx, log.NewTestLogger(), agent, dl2))
	require.Equal(t, 1, agent.deleteCount())
	require.Equal(t, "inbound/forward.ach", agent.deleteCalls[0])
}

func TestConsumption_UncertainDelete_QueriesRemoteStateBeforeRetrying(t *testing.T) {
	ctx := context.Background()
	content, err := os.ReadFile(filepath.Join("testdata", "forward.ach"))
	require.NoError(t, err)

	t.Run("file still present keeps uncertain and retries delete", func(t *testing.T) {
		agent := newTestAgent(map[string][]byte{"inbound/forward.ach": content})
		d := newTestDownloader(t)
		dl := tickDownload(t, ctx, d, agent)
		u := dl.units[0]
		require.NoError(t, u.markValidated(nil))
		require.NoError(t, u.skipAudited("empty"))

		// Delete fails and the file is still listed remotely.
		agent.deleteErr = errors.New("i/o timeout")
		err := deleteRemoteUnit(ctx, log.NewTestLogger(), agent, u)
		require.Error(t, err)
		require.Equal(t, 1, agent.deleteCount())
		require.Equal(t, stageUncertain, u.state.RemoteDeleted.Status)

		// Retry: remote query finds the file present, then Delete is attempted.
		agent.deleteErr = nil
		err = deleteRemoteUnit(ctx, log.NewTestLogger(), agent, u)
		require.NoError(t, err)
		require.Equal(t, 2, agent.deleteCount())
		require.Equal(t, stageConfirmed, u.state.RemoteDeleted.Status)
	})

	t.Run("file absent confirms without another delete", func(t *testing.T) {
		agent := newTestAgent(map[string][]byte{"inbound/forward.ach": content})
		d := newTestDownloader(t)
		dl := tickDownload(t, ctx, d, agent)
		u := dl.units[0]
		require.NoError(t, u.markValidated(nil))
		require.NoError(t, u.skipAudited("empty"))

		agent.deleteErr = errors.New("i/o timeout")
		require.Error(t, deleteRemoteUnit(ctx, log.NewTestLogger(), agent, u))
		require.Equal(t, 1, agent.deleteCount())

		// The uncertain delete actually happened: listing no longer has the file.
		agent.mu.Lock()
		agent.remote = map[string][]byte{}
		agent.mu.Unlock()
		agent.deleteErr = nil

		require.NoError(t, deleteRemoteUnit(ctx, log.NewTestLogger(), agent, u))
		require.Equal(t, 1, agent.deleteCount(), "no second delete when the remote file is gone")
		require.Equal(t, stageConfirmed, u.state.RemoteDeleted.Status)
	})

	t.Run("listing unavailable stays uncertain", func(t *testing.T) {
		agent := newTestAgent(map[string][]byte{"inbound/forward.ach": content})
		d := newTestDownloader(t)
		dl := tickDownload(t, ctx, d, agent)
		u := dl.units[0]
		require.NoError(t, u.markValidated(nil))
		require.NoError(t, u.skipAudited("empty"))

		agent.deleteErr = errors.New("i/o timeout")
		agent.listErr = errors.New("connection refused")
		require.Error(t, deleteRemoteUnit(ctx, log.NewTestLogger(), agent, u))
		require.Equal(t, 1, agent.deleteCount())
		require.Equal(t, stageUncertain, u.state.RemoteDeleted.Status)
	})
}

func TestConsumption_InvalidACH_KeepsRemoteAndEvidence(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent(map[string][]byte{
		"inbound/bad.ach": []byte("### this is not an ach file ###\n"),
	})
	d := newTestDownloader(t)
	proc := &countingProcessor{typ: "mock"}
	alerter := &countingAlerter{}

	dl := tickDownload(t, ctx, d, agent)
	err := ProcessFiles(ctx, log.NewTestLogger(), dl, alerting.Alerters{alerter}, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "problem parsing")
	require.Equal(t, 0, proc.count(), "processors never run for an invalid file")
	require.Equal(t, 0, alerter.count(), "parse errors alert through the tick, not the processor path")

	require.NoError(t, Cleanup(ctx, log.NewTestLogger(), agent, dl))
	require.Equal(t, 0, agent.deleteCount(), "invalid files remain on the remote server")

	results, err := d.ConsumptionResults("")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, stageFailed, results[0].Validated.Status)
	require.NotEmpty(t, results[0].LastError)
}

func TestConsumption_KeepRemoteFiles_ZeroByteDeletedOthersSkipped(t *testing.T) {
	ctx := context.Background()
	content, err := os.ReadFile(filepath.Join("testdata", "forward.ach"))
	require.NoError(t, err)

	agent := newTestAgent(map[string][]byte{
		"inbound/normal.ach": content,
		"inbound/empty.ach":  {},
	})
	d := newTestDownloader(t)
	proc := &countingProcessor{typ: "mock"}
	alerters, _ := alerting.NewAlerters(service.ErrorAlerting{})

	dl := tickDownload(t, ctx, d, agent)
	require.NoError(t, ProcessFiles(ctx, log.NewTestLogger(), dl, alerters, nil, ach.ValidateOpts{}, SetupProcessors(proc), agent))

	// KeepRemoteFiles: populated files stay but zero byte files are removed.
	require.NoError(t, CleanupEmptyFiles(ctx, log.NewTestLogger(), agent, dl))
	require.NoError(t, markRemoteFilesKept(dl))

	require.Equal(t, 1, agent.deleteCount())
	require.Equal(t, "inbound/empty.ach", agent.deleteCalls[0])

	results, err := d.ConsumptionResults("")
	require.NoError(t, err)
	statuses := map[string]stageStatus{}
	for _, r := range results {
		statuses[r.Filename] = r.RemoteDeleted.Status
	}
	require.Equal(t, stageSkipped, statuses["normal.ach"])
	require.Equal(t, stageConfirmed, statuses["empty.ach"])
}

func TestConsumption_ResultsEndpoint(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent(map[string][]byte{})
	d := newTestDownloader(t)

	s := &PeriodicScheduler{downloader: d}
	handler := s.consumptionResults()

	// Empty results.
	req := httptest.NewRequest(http.MethodGet, "/inbound/consumption", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "[]\n", w.Body.String())

	// Commit a unit and query it.
	dl := tickDownload(t, ctx, d, agent)
	_ = dl
	staging := filepath.Join(t.TempDir(), "a.ach")
	require.NoError(t, os.WriteFile(staging, []byte("data"), 0600))
	u, err := dl.store.CommitDownload(agent.Hostname(), "inbound/a.ach", agent.InboundPath(), staging)
	require.NoError(t, err)

	req = httptest.NewRequest(http.MethodGet, "/inbound/consumption?id="+u.state.ID, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var results []consumptionState
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &results))
	require.Len(t, results, 1)
	require.Equal(t, u.state.ID, results[0].ID)

	// Unknown id.
	req = httptest.NewRequest(http.MethodGet, "/inbound/consumption?id=missing", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}
