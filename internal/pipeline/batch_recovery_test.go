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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/alerting"
	"github.com/moov-io/achgateway/internal/events"
	"github.com/moov-io/achgateway/internal/schedule"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/storage"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/log"

	"github.com/stretchr/testify/require"
)

// scriptedAgent is an in-memory remote with upload/read/delete counters.
type scriptedAgent struct {
	*upload.MockAgent

	mu sync.Mutex

	uploads int
	deletes int
	reads   []string

	written  map[string][]byte // full remote path -> bytes
	failNext map[string]int    // filename -> failing attempts (server keeps bytes)
	dropNext map[string]int    // filename -> failing attempts (server keeps nothing)
	mismatch map[string]bool   // full remote path -> ReadFile returns wrong bytes
}

func newScriptedAgent() *scriptedAgent {
	return &scriptedAgent{
		MockAgent: &upload.MockAgent{},
		written:   make(map[string][]byte),
		failNext:  make(map[string]int),
		dropNext:  make(map[string]int),
		mismatch:  make(map[string]bool),
	}
}

func (a *scriptedAgent) ID() string           { return "scripted" }
func (a *scriptedAgent) OutboundPath() string { return "outbound/" }
func (a *scriptedAgent) Hostname() string     { return "scripted-host" }

func (a *scriptedAgent) failAmbigous(filename string, times int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failNext[filename] = times
}

func (a *scriptedAgent) failDropped(filename string, times int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dropNext[filename] = times
}

// matchFailRule finds a rule keyed by filename suffix (e.g. "-1.ach").
func matchFailRule(rules map[string]int, filename string) (string, int, bool) {
	for suffix, remaining := range rules {
		if remaining > 0 && strings.HasSuffix(filename, suffix) {
			return suffix, remaining, true
		}
	}
	return "", 0, false
}

func (a *scriptedAgent) UploadFile(_ context.Context, f upload.File) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	bs, _ := io.ReadAll(f.Contents)
	full := fmt.Sprintf("outbound/%s", f.Filepath)

	if key, remaining, ok := matchFailRule(a.dropNext, f.Filepath); ok {
		a.dropNext[key] = remaining - 1
		return errors.New("i/o timeout after STOR (connection lost)")
	}
	if key, remaining, ok := matchFailRule(a.failNext, f.Filepath); ok {
		a.failNext[key] = remaining - 1
		// Ambiguous response: the server persisted the bytes anyway.
		a.written[full] = append([]byte(nil), bs...)
		return errors.New("i/o timeout after STOR")
	}

	a.uploads++
	a.written[full] = append([]byte(nil), bs...)
	return nil
}

func (a *scriptedAgent) ReadFile(_ context.Context, path string) (*upload.File, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.reads = append(a.reads, path)
	if a.mismatch[path] {
		return &upload.File{Filepath: path, Contents: io.NopCloser(bytes.NewReader([]byte("unexpected different content")))}, nil
	}
	bs, ok := a.written[path]
	if !ok {
		return nil, errors.New("ftp open: 550 no such file")
	}
	return &upload.File{Filepath: path, Contents: io.NopCloser(bytes.NewReader(bs))}, nil
}

func (a *scriptedAgent) Delete(_ context.Context, path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deletes++
	delete(a.written, path)
	return nil
}

func (a *scriptedAgent) stats() (uploads, deletes int, reads []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.uploads, a.deletes, append([]string(nil), a.reads...)
}

func (a *scriptedAgent) put(path string, bs []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.written[path] = append([]byte(nil), bs...)
}

// scriptedAudit counts SaveFile calls (including failed attempts).
type scriptedAudit struct {
	mu       sync.Mutex
	saved    map[string]int
	attempts int
	err      error
}

func newScriptedAudit() *scriptedAudit {
	return &scriptedAudit{saved: make(map[string]int)}
}

func (s *scriptedAudit) SaveFile(_ context.Context, path string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.err != nil {
		return s.err
	}
	s.saved[path]++
	return nil
}
func (s *scriptedAudit) GetFile(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (s *scriptedAudit) Close() error { return nil }
func (s *scriptedAudit) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.saved {
		n += c
	}
	return n
}
func (s *scriptedAudit) attemptsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// scriptedEmitter fails the first failN Send calls, then succeeds, recording
// every FileUploaded event accepted.
type scriptedEmitter struct {
	mu       sync.Mutex
	failN    int
	calls    int
	uploaded []models.FileUploaded
}

func (e *scriptedEmitter) Send(_ context.Context, evt models.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.calls++
	if e.failN > 0 {
		e.failN--
		return errors.New("kafka broker unavailable")
	}
	if fu, ok := evt.Event.(models.FileUploaded); ok {
		e.uploaded = append(e.uploaded, fu)
	}
	return nil
}

func (e *scriptedEmitter) uploadedEvents() []models.FileUploaded {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]models.FileUploaded(nil), e.uploaded...)
}

type recordingAlerter struct {
	mu     sync.Mutex
	errors []error
}

func (r *recordingAlerter) AlertError(err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err)
	return nil
}

// cutoffHarness wires a real filesystemMerging + aggregator handler against
// t.TempDir storage and the supplied remote/audit/emitter doubles.
type cutoffHarness struct {
	t       *testing.T
	root    string
	shard   service.Shard
	cfg     service.UploadAgents
	agent   *scriptedAgent
	audit   *scriptedAudit
	emitter *scriptedEmitter
	alerter *recordingAlerter
}

func newCutoffHarness(t *testing.T, name string, conditions *ach.Conditions) *cutoffHarness {
	t.Helper()

	root := t.TempDir()
	shard := service.Shard{
		Name: name,
		Cutoffs: service.Cutoffs{
			Timezone: "America/New_York",
			Windows:  []string{"15:00"},
		},
		UploadAgent: "scripted",
		Mergable: service.MergableConfig{
			Conditions: conditions,
		},
	}
	cfg := service.UploadAgents{
		Agents: []service.UploadAgent{
			{ID: "scripted", Mock: &service.MockAgent{}, Paths: service.UploadPaths{Outbound: "outbound/"}},
		},
		Merging: service.Merging{
			Storage: storage.Config{
				Filesystem: storage.FilesystemConfig{Directory: root},
			},
		},
	}

	return &cutoffHarness{
		t:       t,
		root:    root,
		shard:   shard,
		cfg:     cfg,
		agent:   newScriptedAgent(),
		audit:   newScriptedAudit(),
		emitter: &scriptedEmitter{},
		alerter: &recordingAlerter{},
	}
}

func (h *cutoffHarness) enqueue(fileID string, amount int, credit bool, seq int) {
	h.t.Helper()
	merging, err := NewMerging(log.NewTestLogger(), h.shard, h.cfg)
	require.NoError(h.t, err)
	enqueuePPD(h.t, merging, h.shard.Name, fileID, amount, credit, seq)
}

// cutoffAggregator builds a fresh merger+aggregator pair (simulates a process),
// sharing the on-disk chest and in-memory doubles.
func (h *cutoffHarness) aggregator() (*aggregator, *filesystemMerging) {
	h.t.Helper()

	merging, err := NewMerging(log.NewTestLogger(), h.shard, h.cfg)
	require.NoError(h.t, err)
	fm := merging.(*filesystemMerging)
	agent := h.agent
	fm.newAgent = func(_ log.Logger, _ service.UploadAgents, _ string) (upload.Agent, error) {
		return agent, nil
	}

	agg, err := newAggregator(log.NewTestLogger(), h.emitter, h.shard, h.cfg, service.ErrorAlerting{})
	require.NoError(h.t, err)
	agg.merger = fm
	agg.auditStorage = h.audit
	agg.alerters = alerting.Alerters{h.alerter}
	return agg, fm
}

func (h *cutoffHarness) activeBatches(fm *filesystemMerging) []*Batch {
	h.t.Helper()
	batches, err := activeBatches(context.Background(), fm.storage, h.shard.Name)
	require.NoError(h.t, err)
	return batches
}

func (h *cutoffHarness) singleBatchState(fm *filesystemMerging) BatchState {
	h.t.Helper()
	batches := h.activeBatches(fm)
	require.Len(h.t, batches, 1)
	return batches[0].State()
}

func (h *cutoffHarness) isolatedDirs() []string {
	h.t.Helper()
	return findIsolatedShardDirs(h.t, h.root, h.shard.Name)
}

func automatedWindow(minute ...int) cutoffTrigger {
	ny, _ := time.LoadLocation("America/New_York")
	m := 0
	if len(minute) > 0 {
		m = minute[0]
	}
	return cutoffTrigger{
		kind:   BatchKindAutomated,
		window: time.Date(2026, 10, 14, 15, m, 0, 0, ny),
	}
}

func sha256Hex(bs []byte) string {
	sum := sha256.Sum256(bs)
	return hex.EncodeToString(sum[:])
}

func TestRecovery_HappyPath_BatchIdentityAndCompletion(t *testing.T) {
	h := newCutoffHarness(t, "SD-happy", nil)
	h.enqueue("happy-1.ach", 1500, true, 1)
	h.enqueue("happy-2.ach", 2500, true, 2)

	agg, fm := h.aggregator()
	merged, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.NoError(t, err)
	require.Len(t, merged, 1)

	// One upload, one audit copy, one FileUploaded per input.
	uploads, deletes, _ := h.agent.stats()
	require.Equal(t, 1, uploads)
	require.Zero(t, deletes)
	require.Equal(t, 1, h.audit.calls())

	got := h.emitter.uploadedEvents()
	require.Len(t, got, 2)
	wantID := "SD-happy-20261014-1500"
	for _, e := range got {
		require.Equal(t, wantID, e.BatchID)
		require.Equal(t, "SD-happy", e.ShardKey)
		require.False(t, e.UploadedAt.IsZero())
	}

	// Manifest completed and no active batches remain.
	require.Empty(t, h.activeBatches(fm))

	dirs := h.isolatedDirs()
	require.Len(t, dirs, 1)
	loaded, err := loadBatch(context.Background(), fm.storage, dirs[0])
	require.NoError(t, err)
	st := loaded.State()
	require.Equal(t, batchStatusCompleted, st.Status)
	require.Equal(t, wantID, st.ID)
	require.Len(t, st.Inputs, 2)
	require.Len(t, st.Outputs, 1)
	require.True(t, st.Outputs[0].Uploaded && st.Outputs[0].Audited)
	require.Equal(t, st.Outputs[0].SHA256, remoteDigest(t, h.agent, st.Outputs[0].RemotePath))

	// Re-running the same window is idempotent: no new upload, no new directory.
	merged2, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.NoError(t, err)
	require.Empty(t, merged2)
	uploads2, _, _ := h.agent.stats()
	require.Equal(t, 1, uploads2)
	require.Len(t, h.isolatedDirs(), 1)
}

func TestRecovery_AmbiguousUpload_VerifiedByPathAndDigest(t *testing.T) {
	h := newCutoffHarness(t, "SD-amb", nil)
	h.enqueue("amb-1.ach", 1100, true, 1)

	// First upload attempt errors, but the server keeps the bytes (ambiguous).
	h.agent.failAmbigous("-0.ach", 1)

	agg, fm := h.aggregator()
	merged, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.NoError(t, err, "path+digest verification treats ambiguous upload as confirmed")
	require.Len(t, merged, 1)

	uploads, deletes, reads := h.agent.stats()
	require.Equal(t, 0, uploads, "no second UploadFile after verification confirmed the bytes")
	require.Zero(t, deletes)
	require.NotEmpty(t, reads, "remote ReadFile verification must happen")

	st := h.completedState(fm)
	require.True(t, st.Outputs[0].Uploaded)
	require.True(t, st.Outputs[0].Verified)
	// Verified within the same run, audit and events proceed.
	require.True(t, st.Outputs[0].Audited)
	require.Len(t, h.emitter.uploadedEvents(), 1)
}

func TestRecovery_RemoteMissingThenRestartUploads(t *testing.T) {
	h := newCutoffHarness(t, "SD-miss", nil)
	h.enqueue("miss-1.ach", 1200, true, 1)

	// Fail without persisting server-side: verification reports missing.
	h.agent.failDropped("-0.ach", 1)
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)

	st := h.singleBatchState(fm1)
	require.Equal(t, batchStatusActive, st.Status)
	require.False(t, st.Outputs[0].Uploaded)
	require.Zero(t, h.audit.calls(), "audit must not run before upload confirmation")
	require.Empty(t, h.emitter.uploadedEvents())
	require.Len(t, h.activeBatches(fm1), 1)

	// "Process restart": new instances over the same chest now succeed.
	agg2, fm2 := h.aggregator()
	require.NoError(t, agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2))

	uploads, _, _ := h.agent.stats()
	require.Equal(t, 1, uploads, "only one successful upload across both runs")
	require.Equal(t, 1, h.audit.calls())
	require.Len(t, h.emitter.uploadedEvents(), 1)
	require.Empty(t, h.activeBatches(fm2))

	// Fixed filename and digest across runs.
	final := h.completedState(fm2)
	require.Equal(t, st.Outputs[0].Filename, final.Outputs[0].Filename)
	require.Equal(t, st.Outputs[0].RemotePath, final.Outputs[0].RemotePath)
}

func TestRecovery_RemoteContentConflictFreezesOutput(t *testing.T) {
	h := newCutoffHarness(t, "SD-conflict", nil)
	h.enqueue("conflict-1.ach", 1300, true, 1)

	// First attempt drops the bytes: batch stays active with a fixed remote path.
	h.agent.failDropped("-0.ach", 1)
	agg, fm := h.aggregator()
	_, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.Error(t, err)
	st := h.singleBatchState(fm)
	require.False(t, st.Outputs[0].Uploaded)

	// Someone else's file now occupies the target path. Second attempt also
	// fails to write, verification reads the foreign bytes → mismatch.
	h.agent.put(st.Outputs[0].RemotePath, []byte("someone else's file"))
	h.agent.failDropped("-0.ach", 1)

	// Drive through the scheduled-cutoff entry point so the window-level
	// critical alert is emitted exactly as in production.
	day := &schedule.Day{Time: automatedWindow().window, IsBankingDay: true}
	agg.handleScheduledCutoff(day)
	require.NotEmpty(t, h.alerter.errors, "conflict triggers critical alerting")
	require.ErrorIs(t, h.alerter.errors[0], upload.ErrRemoteContentConflict)

	uploads, deletes, _ := h.agent.stats()
	require.Zero(t, uploads, "conflicting object must never be overwritten")
	require.Zero(t, deletes, "conflicting object must never be deleted")

	st = h.singleBatchState(fm)
	require.True(t, st.Outputs[0].Conflict)
	require.False(t, st.Outputs[0].Uploaded)
	require.Equal(t, batchStatusActive, st.Status, "conflict blocks completion and cleanup gating")

	// Recovery keeps the freeze even once network is healthy: conflict wins.
	agg2, fm2 := h.aggregator()
	err = agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2)
	require.Error(t, err)
	require.ErrorIs(t, err, upload.ErrRemoteContentConflict)
	uploads2, deletes2, _ := h.agent.stats()
	require.Zero(t, uploads2)
	require.Zero(t, deletes2)
	require.Len(t, h.activeBatches(fm2), 1)
}

func TestRecovery_AuditFailure_BackfillsAuditOnly(t *testing.T) {
	h := newCutoffHarness(t, "SD-audit", nil)
	h.enqueue("audit-1.ach", 1400, true, 1)

	// Audit storage fails on the first run.
	h.audit.err = errors.New("blob bucket unavailable")
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)

	state := h.singleBatchState(fm1)
	require.True(t, state.Outputs[0].Uploaded, "upload fact retained despite audit failure")
	require.False(t, state.Outputs[0].Audited)
	require.Len(t, h.emitter.uploadedEvents(), 1, "events for an uploaded output still send")

	// Restart with healthy audit storage; upload must not repeat.
	h.audit.err = nil
	agg2, fm2 := h.aggregator()
	require.NoError(t, agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2))

	uploads, _, _ := h.agent.stats()
	require.Equal(t, 1, uploads, "no second upload when backfilling audit")
	require.Equal(t, 2, h.audit.attemptsCount(), "failed then successful SaveFile against the fixed audit path")
	require.Equal(t, 1, h.audit.calls(), "one successful audit copy")
	require.Empty(t, h.activeBatches(fm2))
}

func TestRecovery_EventFailure_ResendsEventsOnly(t *testing.T) {
	h := newCutoffHarness(t, "SD-events", nil)
	h.enqueue("ev-1.ach", 1600, true, 1)
	h.enqueue("ev-2.ach", 1700, true, 2)
	h.emitter.failN = 2 // both first-run events fail

	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)
	require.Empty(t, h.emitter.uploadedEvents())
	require.Equal(t, batchStatusActive, h.singleBatchState(fm1).Status)

	// Restart: no new upload/audit, only events replay with same identity/time.
	beforeUploads, _, _ := h.agent.stats()
	beforeAudit := h.audit.calls()
	agg2, fm2 := h.aggregator()
	require.NoError(t, agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2))

	uploads, _, _ := h.agent.stats()
	require.Equal(t, beforeUploads, uploads)
	require.Equal(t, beforeAudit, h.audit.calls())

	got := h.emitter.uploadedEvents()
	require.Len(t, got, 2)
	for _, e := range got {
		require.Equal(t, "SD-events-20261014-1500", e.BatchID)
		require.False(t, e.UploadedAt.IsZero())
	}
	require.Empty(t, h.activeBatches(fm2))
}

func TestRecovery_PartialSuccess_DoesNotRetransmitCompletedOutputs(t *testing.T) {
	// MaxLines=5 forces one entry per output: three outputs; the middle one
	// fails its first attempt.
	h := newCutoffHarness(t, "SD-partial", &ach.Conditions{MaxLines: 5})
	h.enqueue("p-0.ach", 100, true, 1)
	h.enqueue("p-1.ach", 200, true, 2)
	h.enqueue("p-2.ach", 300, true, 3)

	h.agent.failDropped("-1.ach", 1)
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)

	st := h.singleBatchState(fm1)
	require.Len(t, st.Outputs, 3)
	require.True(t, st.Outputs[0].Uploaded)
	require.False(t, st.Outputs[1].Uploaded)
	require.True(t, st.Outputs[2].Uploaded)

	// Recover: only the pending output is attempted.
	agg2, fm2 := h.aggregator()
	require.NoError(t, agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2))

	uploads, deletes, _ := h.agent.stats()
	require.Equal(t, 3, uploads, "three outputs uploaded exactly once across both runs")
	require.Zero(t, deletes)
	final := h.completedState(fm2)
	for i := range final.Outputs {
		require.True(t, final.Outputs[i].Uploaded && final.Outputs[i].Audited, "output %d", i)
	}
}

func TestRecovery_SameWindowReuseAndSnapshotFixed(t *testing.T) {
	h := newCutoffHarness(t, "SD-snap", nil)
	h.enqueue("snap-1.ach", 1800, true, 1)

	h.agent.failDropped("-0.ach", 1)
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)
	require.Len(t, h.isolatedDirs(), 1)

	// A new file arrives while the window batch is active.
	h.enqueue("snap-2.ach", 1900, true, 2)

	// Same automated window resumes the active batch: no second isolation, and
	// the late file is not part of the fixed snapshot.
	agg2, fm2 := h.aggregator()
	_, err = agg2.merger.WithEachMerged(context.Background(), automatedWindow(), agg2)
	require.NoError(t, err)

	require.Len(t, h.isolatedDirs(), 1, "same window reuses one isolated directory")
	st := h.completedState(fm2)
	require.Len(t, st.Inputs, 1, "snapshot is fixed; late file excluded")

	// The late file remains queued for a later window.
	queued, err := fm2.storage.Glob(fmt.Sprintf("mergable/%s/*.ach", h.shard.Name))
	require.NoError(t, err)
	require.Len(t, queued, 1)

	// A later window isolates the late file into a new batch.
	agg3, _ := h.aggregator()
	_, err = agg3.merger.WithEachMerged(context.Background(), automatedWindow(16), agg3)
	require.NoError(t, err)
	require.Len(t, findIsolatedShardDirs(t, h.root, h.shard.Name), 2)
}

func TestRecovery_SecondSameWindowBatchGetsDistinctID(t *testing.T) {
	h := newCutoffHarness(t, "SD-second", nil)
	h.enqueue("first-1.ach", 2400, true, 1)

	agg, fm := h.aggregator()
	_, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.NoError(t, err)
	firstID := "SD-second-20261014-1500"
	require.Empty(t, h.activeBatches(fm))

	// New inputs arrive and a later recovery pass in the same scheduled
	// window processes them (earlier batch already completed).
	h.enqueue("second-1.ach", 2500, true, 2)
	agg2, fm2 := h.aggregator()
	_, err = agg2.merger.WithEachMerged(context.Background(), automatedWindow(), agg2)
	require.NoError(t, err)

	dirs := h.isolatedDirs()
	require.Len(t, dirs, 2)
	var ids []string
	for _, dir := range dirs {
		b, err := loadBatch(context.Background(), fm2.storage, dir)
		require.NoError(t, err)
		require.NotNil(t, b)
		ids = append(ids, b.ID())
		require.Equal(t, batchStatusCompleted, b.State().Status)
	}
	require.Contains(t, ids, firstID)
	var secondID string
	for _, id := range ids {
		if id != firstID {
			secondID = id
		}
	}
	require.NotEmpty(t, secondID)
	require.NotEqual(t, firstID, secondID, "second same-window batch must not reuse the completed batch ID")
	require.Contains(t, secondID, firstID+"-")
}

func TestRecovery_EmptyCutoffNoManifest(t *testing.T) {
	h := newCutoffHarness(t, "SD-empty", nil)
	agg, fm := h.aggregator()
	require.NoError(t, fm.storage.MkdirAll(fmt.Sprintf("mergable/%s", h.shard.Name)))

	merged, err := fm.WithEachMerged(context.Background(), cutoffTrigger{kind: BatchKindManual, window: time.Now()}, agg)
	require.NoError(t, err)
	require.Empty(t, merged)
	require.Empty(t, h.isolatedDirs())
	require.Empty(t, h.activeBatches(fm))
	uploads, _, _ := h.agent.stats()
	require.Zero(t, uploads)
}

func (h *cutoffHarness) completedState(fm *filesystemMerging) BatchState {
	h.t.Helper()
	dirs := h.isolatedDirs()
	require.NotEmpty(h.t, dirs)
	// Active batches are empty once completed; read the most recent manifest directly.
	var last BatchState
	for _, dir := range dirs {
		b, err := loadBatch(context.Background(), fm.storage, dir)
		require.NoError(h.t, err)
		if b != nil {
			last = b.State()
		}
	}
	require.Equal(h.t, batchStatusCompleted, last.Status)
	return last
}

// F1 regression: a crash after the server accepted bytes but before the
// manifest flushed must not cause a second upload on restart.
func TestRecovery_CrashAfterAck_VerifiedWithoutReupload(t *testing.T) {
	h := newCutoffHarness(t, "SD-crash", nil)
	h.enqueue("crash-1.ach", 2100, true, 1)

	// First attempt: server write "lost" (response error, bytes not kept).
	h.agent.failDropped("-0.ach", 1)
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)
	st := h.singleBatchState(fm1)
	require.False(t, st.Outputs[0].Uploaded)
	require.NotEmpty(t, st.Outputs[0].ArtifactPath)

	// Simulate the server actually having received the bytes before the
	// crash (artifact bytes are exactly what would have been uploaded).
	artifact, err := fm1.storage.Open(st.Outputs[0].ArtifactPath)
	require.NoError(t, err)
	bs, err := io.ReadAll(artifact)
	require.NoError(t, err)
	artifact.Close()
	h.agent.put(st.Outputs[0].RemotePath, bs)

	// Restart: resumed output is pre-checked and confirmed, never re-uploaded.
	agg2, fm2 := h.aggregator()
	require.NoError(t, agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2))

	uploads, deletes, reads := h.agent.stats()
	require.Zero(t, uploads, "accepted remote bytes must not be uploaded twice")
	require.Zero(t, deletes)
	require.NotEmpty(t, reads, "pre-flight remote verification must run")
	final := h.completedState(fm2)
	require.True(t, final.Outputs[0].Uploaded)
	require.True(t, final.Outputs[0].Verified)
}

// F1 regression: resumed preflight against a foreign object freezes the
// output instead of overwriting it.
func TestRecovery_ResumedPreflight_ConflictFreeze(t *testing.T) {
	h := newCutoffHarness(t, "SD-preconflict", nil)
	h.enqueue("pc-1.ach", 2200, true, 1)

	h.agent.failDropped("-0.ach", 1)
	agg1, fm1 := h.aggregator()
	_, err := fm1.WithEachMerged(context.Background(), automatedWindow(), agg1)
	require.Error(t, err)
	st := h.singleBatchState(fm1)

	h.agent.put(st.Outputs[0].RemotePath, []byte("foreign bytes"))
	agg2, fm2 := h.aggregator()
	err = agg2.merger.RecoverActiveBatches(context.Background(), "startup", agg2)
	require.Error(t, err)
	require.ErrorIs(t, err, upload.ErrRemoteContentConflict)

	uploads, deletes, _ := h.agent.stats()
	require.Zero(t, uploads)
	require.Zero(t, deletes)
	require.True(t, h.singleBatchState(fm2).Outputs[0].Conflict)
}

// F2 regression: operator event backfill only merges event flags and cannot
// roll back upload facts; it also stamps the batch identity.
func TestRecovery_AdminBackfill_MergesEventFlags(t *testing.T) {
	h := newCutoffHarness(t, "SD-backfill", nil)
	h.enqueue("bf-1.ach", 2300, true, 1)

	h.agent.failDropped("-0.ach", 1)
	agg, fm := h.aggregator()
	_, err := fm.WithEachMerged(context.Background(), automatedWindow(), agg)
	require.Error(t, err)
	st := h.singleBatchState(fm)

	// Operator confirms the upload events while the batch is still unuploaded.
	require.NoError(t, agg.confirmBackfilledEvents(context.Background(), st.Dir, []string{"bf-1.ach"}))

	after := h.singleBatchState(fm)
	require.True(t, after.Inputs[0].EventSent)
	require.False(t, after.Outputs[0].Uploaded, "backfill must not invent upload facts")
	require.Equal(t, batchStatusActive, after.Status)
}

func TestScheduledCutoff_HolidayDoesNotRecoverOrProcess(t *testing.T) {
	merger := &MockXferMerging{}
	xfagg := &aggregator{
		logger:           log.NewTestLogger(),
		eventEmitter:     &events.MockEmitter{},
		shard:            service.Shard{Name: "SD-hol"},
		merger:           merger,
		alerters:         alerting.Alerters{},
		startupRecovered: make(chan struct{}),
	}
	close(xfagg.startupRecovered)

	holiday := &schedule.Day{IsHoliday: true, IsWeekend: false}
	xfagg.handleScheduledCutoff(holiday)
	require.Zero(t, merger.RecoveryCalls, "holiday must not run recovery")
	require.Equal(t, "", merger.LastTrigger.kind, "holiday must not open a batch")

	banking := &schedule.Day{IsBankingDay: true, Time: automatedWindow().window}
	xfagg.handleScheduledCutoff(banking)
	require.Equal(t, 1, merger.RecoveryCalls)
	require.Equal(t, BatchKindAutomated, merger.LastTrigger.kind)
}

func TestBatch_EncryptedChestRoundTrip(t *testing.T) {
	root := t.TempDir()
	chest, err := storage.New(storage.Config{
		Filesystem: storage.FilesystemConfig{Directory: root},
		Encryption: storage.EncryptionConfig{
			AES: &storage.AESConfig{
				Base64Key: base64.RawStdEncoding.EncodeToString([]byte("1111111111111111")),
			},
			Encoding: "base64",
		},
	})
	require.NoError(t, err)

	b := newBatch(chest, BatchState{
		ID:     "SD-enc-20261014-1500",
		Shard:  "SD-enc",
		Kind:   BatchKindAutomated,
		Dir:    "SD-enc-20261014-150000",
		Inputs: []BatchInput{{Filename: "a.ach", SHA256: "x", OutputIndex: unmappedOutput}},
	})
	require.NoError(t, b.Persist(context.Background()))

	loaded, err := loadBatch(context.Background(), chest, b.State().Dir)
	require.NoError(t, err)
	require.Equal(t, "SD-enc-20261014-1500", loaded.ID())

	rel, err := b.WriteArtifact("0-x.ach", []byte("nachabytes"))
	require.NoError(t, err)
	got, err := b.ReadArtifact(rel)
	require.NoError(t, err)
	require.Equal(t, "nachabytes", string(got))

	// Corrupt manifests do not abort the scan of other active batches.
	other := "SD-other-20261014-140000"
	require.NoError(t, chest.MkdirAll(other))
	require.NoError(t, chest.WriteFile(filepath.Join(other, batchManifestFile), []byte("{broken")))
	batches, err := activeBatches(context.Background(), chest, "SD-enc")
	require.NoError(t, err)
	require.Len(t, batches, 1, "corrupt manifest skipped, healthy batch returned")
}

func remoteDigest(t *testing.T, agent *scriptedAgent, remotePath string) string {
	t.Helper()
	f, err := agent.ReadFile(context.Background(), remotePath)
	require.NoError(t, err)
	bs, err := io.ReadAll(f.Contents)
	require.NoError(t, err)
	return sha256Hex(bs)
}
