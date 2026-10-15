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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moov-io/achgateway/internal/storage"
)

const (
	// batchManifestFile is the per-cutoff manifest stored inside the isolated
	// directory. It is the single source of truth when recovering a cutoff run.
	batchManifestFile = "batch.json"
	// batchArtifactDir holds the exact transformed bytes that were (or will be)
	// uploaded and audited, keyed per output so recovery never re-renders names
	// or re-transforms content.
	batchArtifactDir = "outbox"

	batchStatusActive    = "active"
	batchStatusCompleted = "completed"

	// BatchKindAutomated batches belong to a scheduled cutoff window.
	BatchKindAutomated = "automated"
	// BatchKindManual batches were started by a manual trigger.
	BatchKindManual = "manual"

	// unmappedOutput marks a snapshot input that never matched an uploaded output.
	unmappedOutput = -1
)

// BatchInput is the fixed snapshot of one queued input file within a batch.
type BatchInput struct {
	// Filename is the basename of the input inside the isolated directory.
	Filename string `json:"filename"`
	// SHA256 is the digest of the Nacha input bytes at snapshot time.
	SHA256 string `json:"sha256"`
	// Size is the input byte length at snapshot time.
	Size int64 `json:"size"`

	// OutputIndex is the BatchOutput index this input merged into, or
	// unmappedOutput when no uploaded output contains its entries.
	OutputIndex int `json:"output_index"`
	// EventSent is true once the FileUploaded event for this input was accepted
	// by the event emitter.
	EventSent bool `json:"event_sent"`
}

// BatchOutput records the stage facts for one merged file. Every stage is
// independently idempotent: recovery resumes at the first false flag.
type BatchOutput struct {
	// Index is the merged-file index produced by ach.MergeDir (stable within a batch).
	Index int `json:"index"`

	// MergedPath/MergedSHA256 describe the Nacha merged cache (uploaded/<sha>.ach).
	MergedPath   string `json:"merged_path"`
	MergedSHA256 string `json:"merged_sha256"`

	// ArtifactPath is the chest-relative location of the exact transformed bytes
	// that are uploaded and audited; SHA256 is their digest.
	ArtifactPath string `json:"artifact_path"`
	SHA256       string `json:"sha256"`

	// Filename is the rendered remote filename, fixed on the first prepare and
	// reused forever (the template embeds wall-clock time, so re-rendering after
	// a restart would orphan the first upload).
	Filename string `json:"filename"`
	// RemotePath is the full ODFI path (OutboundPath/Filename).
	RemotePath string `json:"remote_path"`
	// AuditPath is the fixed audit-trail object path for this output.
	AuditPath string `json:"audit_path"`

	// Uploaded is true once the remote outcome is confirmed, either by a clean
	// UploadFile or a path-plus-digest verification after an ambiguous response.
	Uploaded bool `json:"uploaded"`
	// Verified distinguishes "confirmed by remote verification" from a clean
	// upload acknowledgement.
	Verified bool `json:"verified"`
	// UploadedAt is fixed at first confirmation and reused on event re-sends.
	UploadedAt time.Time `json:"uploaded_at"`

	// Audited is true once the retention copy exists at AuditPath.
	Audited bool `json:"audited"`

	// Conflict is true when a different object occupies RemotePath. The output
	// is frozen: no upload, no overwrite, no completion until a human resolves it.
	Conflict bool `json:"conflict"`

	// Inputs lists the snapshot input indices that merged into this output.
	Inputs []int `json:"inputs"`
}

// BatchState is the persisted manifest for one shard cutoff window.
type BatchState struct {
	// ID is the stable batch identity, e.g. "<shard>-20261014-1500".
	ID string `json:"id"`
	// Shard is the owning shard name.
	Shard string `json:"shard"`
	// Window is the cutoff window label (local HH:MM) or the manual trigger label.
	Window string `json:"window"`
	// Kind is BatchKindAutomated or BatchKindManual.
	Kind string `json:"kind"`
	// Dir is the isolated directory (chest-relative) holding the batch.
	Dir string `json:"dir"`
	// Status is batchStatusActive or batchStatusCompleted.
	Status string `json:"status"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Inputs  []BatchInput  `json:"inputs"`
	Outputs []BatchOutput `json:"outputs"`
}

// Batch is the concurrency-safe, persistent handle over one BatchState.
type Batch struct {
	chest storage.Chest

	mu    sync.Mutex
	state BatchState
}

func newBatch(chest storage.Chest, state BatchState) *Batch {
	now := time.Now().UTC()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
	if state.Status == "" {
		state.Status = batchStatusActive
	}
	return &Batch{chest: chest, state: state}
}

// automatedBatchID returns the deterministic identity for a scheduled cutoff.
// window is the fired cutoff time, already located in the shard's timezone.
func automatedBatchID(shardName string, window time.Time) string {
	return fmt.Sprintf("%s-%s", shardName, window.Format("20060102-1504"))
}

// manualBatchID returns the identity for a manual trigger without a scheduled window.
func manualBatchID(shardName string, at time.Time) string {
	return fmt.Sprintf("%s-manual-%s", shardName, at.Format("20060102-150405"))
}

// isolatedDirName keeps the on-disk layout introduced before batch manifests:
// "<shard>-YYYYMMDD-HHMMSS" of the trigger instant. The cleanup regex and the
// admin pipeline API depend on this shape.
func isolatedDirName(shardName string, at time.Time) string {
	return fmt.Sprintf("%s-%s", shardName, at.Format("20060102-150405"))
}

// ID returns the stable batch identity.
func (b *Batch) ID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.ID
}

// Dir returns the batch's isolated chest-relative directory.
func (b *Batch) Dir() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Dir
}

// Kind returns automated or manual.
func (b *Batch) Kind() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Kind
}

// State returns a detached copy of the manifest for reads outside the lock.
func (b *Batch) State() BatchState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneState(b.state)
}

func cloneState(s BatchState) BatchState {
	out := s
	out.Inputs = append([]BatchInput(nil), s.Inputs...)
	out.Outputs = append([]BatchOutput(nil), s.Outputs...)
	for i := range out.Outputs {
		out.Outputs[i].Inputs = append([]int(nil), s.Outputs[i].Inputs...)
	}
	return out
}

// InitializeOutputs records the merged-cache facts and input→output mapping
// discovered by the merge step. Each BatchOutput.Inputs lists snapshot input
// indices; the reverse OutputIndex on inputs is set here too. Outputs already
// recorded (a resuming batch) are left untouched.
func (b *Batch) InitializeOutputs(outputs []BatchOutput) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.state.Outputs) > 0 {
		return
	}
	b.state.Outputs = append([]BatchOutput(nil), outputs...)
	for outIdx := range b.state.Outputs {
		for _, inIdx := range b.state.Outputs[outIdx].Inputs {
			if inIdx >= 0 && inIdx < len(b.state.Inputs) {
				b.state.Inputs[inIdx].OutputIndex = outIdx
			}
		}
	}
}

// MarkPrepared records the fixed filename/path/artifact facts for one output.
// Fields already populated (a resuming batch) are never overwritten so a retry
// cannot rename a remote object.
func (b *Batch) MarkPrepared(idx int, out BatchOutput) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.state.Outputs) <= idx {
		b.state.Outputs = append(b.state.Outputs, BatchOutput{Index: len(b.state.Outputs)})
	}
	cur := &b.state.Outputs[idx]
	cur.Index = idx
	if cur.MergedPath == "" {
		cur.MergedPath = out.MergedPath
		cur.MergedSHA256 = out.MergedSHA256
	}
	if cur.ArtifactPath == "" {
		cur.ArtifactPath = out.ArtifactPath
		cur.SHA256 = out.SHA256
	}
	if cur.Filename == "" {
		cur.Filename = out.Filename
		cur.RemotePath = out.RemotePath
		cur.AuditPath = out.AuditPath
	}
	if len(cur.Inputs) == 0 {
		cur.Inputs = append([]int(nil), out.Inputs...)
	}
}

// MarkUploaded confirms the remote outcome for one output.
func (b *Batch) MarkUploaded(idx int, verified bool, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if idx < 0 || idx >= len(b.state.Outputs) {
		return
	}
	o := &b.state.Outputs[idx]
	o.Uploaded = true
	if verified {
		o.Verified = true
	}
	if o.UploadedAt.IsZero() {
		o.UploadedAt = at
	}
}

// MarkConflict freezes an output whose target path holds different content.
func (b *Batch) MarkConflict(idx int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if idx < 0 || idx >= len(b.state.Outputs) {
		return
	}
	b.state.Outputs[idx].Conflict = true
}

// MarkAudited records the retention copy at the output's AuditPath.
func (b *Batch) MarkAudited(idx int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if idx < 0 || idx >= len(b.state.Outputs) {
		return
	}
	b.state.Outputs[idx].Audited = true
}

// MarkEventsSent flags snapshot inputs whose FileUploaded send succeeded.
func (b *Batch) MarkEventsSent(inputIndices []int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, idx := range inputIndices {
		if idx >= 0 && idx < len(b.state.Inputs) {
			b.state.Inputs[idx].EventSent = true
		}
	}
}

// IsComplete reports whether every output is uploaded and audited, no conflict
// remains, and every input mapped to an uploaded output has its event sent.
func (b *Batch) IsComplete() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.complete()
}

func (s *BatchState) complete() bool {
	for i := range s.Outputs {
		o := &s.Outputs[i]
		if o.Conflict || !o.Uploaded || !o.Audited {
			return false
		}
	}
	for i := range s.Inputs {
		in := &s.Inputs[i]
		if in.OutputIndex >= 0 && !in.EventSent {
			return false
		}
	}
	return true
}

// HasConflict reports whether any output is frozen on a remote content conflict.
func (b *Batch) HasConflict() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.state.Outputs {
		if b.state.Outputs[i].Conflict {
			return true
		}
	}
	return false
}

// PendingOutputs counts outputs still missing an upload confirmation, audit
// copy, or frozen on a conflict — the work a recovery pass may advance.
func (b *Batch) PendingOutputs() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.pendingOutputs()
}

func (s *BatchState) pendingOutputs() int {
	n := 0
	for i := range s.Outputs {
		o := &s.Outputs[i]
		if o.Conflict || !o.Uploaded || !o.Audited {
			n++
		}
	}
	return n
}

// MarkCompleted flips the manifest to completed when every stage is done.
// Callers must verify IsComplete first.
func (b *Batch) MarkCompleted() error {
	b.mu.Lock()
	if !b.state.complete() {
		b.mu.Unlock()
		return errors.New("batch is not complete")
	}
	b.state.Status = batchStatusCompleted
	b.mu.Unlock()
	return nil
}

// manifestTempSeq makes per-process temp names unique so concurrent writers
// (cutoff goroutine vs admin backfill) cannot share a staging file.
var manifestTempSeq atomic.Uint64

// Persist atomically writes the manifest through the chest (unique temp file
// plus rename). Readers always observe either the previous or the new version.
func (b *Batch) Persist(_ context.Context) error {
	b.mu.Lock()
	b.state.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(b.state, "", "  ")
	dir := b.state.Dir
	b.mu.Unlock()
	if err != nil {
		return fmt.Errorf("marshaling batch manifest: %w", err)
	}

	tmp := filepath.Join(dir, fmt.Sprintf(".batch.%d.%d.tmp", os.Getpid(), manifestTempSeq.Add(1)))
	final := filepath.Join(dir, batchManifestFile)
	if err := b.chest.WriteFile(tmp, data); err != nil {
		return fmt.Errorf("writing batch manifest: %w", err)
	}
	if err := b.chest.ReplaceFile(tmp, final); err != nil {
		return fmt.Errorf("committing batch manifest: %w", err)
	}
	return nil
}

// loadBatch reads the manifest in dir. A missing manifest returns (nil, nil) so
// callers can distinguish legacy isolated directories.
func loadBatch(ctx context.Context, chest storage.Chest, dir string) (*Batch, error) {
	fd, err := chest.Open(filepath.Join(dir, batchManifestFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || isChestNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("opening batch manifest in %s: %w", dir, err)
	}
	defer fd.Close()

	var state BatchState
	if err := json.NewDecoder(fd).Decode(&state); err != nil {
		return nil, fmt.Errorf("decoding batch manifest in %s: %w", dir, err)
	}
	if state.Dir == "" {
		state.Dir = dir
	}
	b := &Batch{chest: chest, state: state}
	return b, nil
}

// isChestNotExist covers the plain-filesystem chest's un-typed open errors.
func isChestNotExist(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such file") || strings.Contains(msg, "cannot find the file")
}

// activeBatches scans the storage root for manifests owned by shardName that
// are still active, ordered oldest-first so recovery is starvation-free.
func activeBatches(ctx context.Context, chest storage.Chest, shardName string) ([]*Batch, error) {
	entries, err := chest.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("listing batches: %w", err)
	}

	var out []*Batch
	for i := range entries {
		if !entries[i].IsDir() {
			continue
		}
		dir := entries[i].Name()
		b, err := loadBatch(ctx, chest, dir)
		if err != nil {
			// One unreadable manifest must not starve the other batches:
			// record it and continue scanning.
			recordCorruptBatchManifest(shardName)
			continue
		}
		if b == nil {
			continue
		}
		if shardName != "" && b.state.Shard != shardName {
			continue
		}
		if b.state.Status != batchStatusActive {
			continue
		}
		out = append(out, b)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].state.CreatedAt.Before(out[j].state.CreatedAt)
	})
	return out, nil
}

// findActiveBatchID returns the active batch with the given identity, or nil.
func findActiveBatchID(ctx context.Context, chest storage.Chest, shardName, id string) (*Batch, error) {
	batches, err := activeBatches(ctx, chest, shardName)
	if err != nil {
		return nil, err
	}
	for i := range batches {
		if batches[i].state.ID == id {
			return batches[i], nil
		}
	}
	return nil, nil
}

// newestActiveBatch returns the most recently created active batch for the shard,
// or nil. Manual triggers attach to it so auto and manual runs share work.
func newestActiveBatch(ctx context.Context, chest storage.Chest, shardName string) (*Batch, error) {
	batches, err := activeBatches(ctx, chest, shardName)
	if err != nil || len(batches) == 0 {
		return nil, err
	}
	return batches[len(batches)-1], nil
}

// batchWithIDExists reports whether any (including completed) manifest with
// the given shard/ID exists.
func batchWithIDExists(ctx context.Context, chest storage.Chest, shardName, id string) (bool, error) {
	entries, err := chest.ReadDir(".")
	if err != nil {
		return false, err
	}
	for i := range entries {
		if !entries[i].IsDir() {
			continue
		}
		b, err := loadBatch(ctx, chest, entries[i].Name())
		if err != nil || b == nil {
			continue
		}
		if b.state.Shard == shardName && b.state.ID == id {
			return true, nil
		}
	}
	return false, nil
}

// dirTimestampSuffix returns the time part of an isolated directory name
// ("<shard>-YYYYMMDD-HHMMSS[-N]") for use as a disambiguating batch-ID suffix.
func dirTimestampSuffix(dir, shardName string) string {
	base := filepath.Base(dir)
	prefix := shardName + "-"
	if !strings.HasPrefix(base, prefix) {
		return ""
	}
	return strings.TrimPrefix(base, prefix)
}

// WriteArtifact stores transformed upload bytes under the batch's outbox and
// returns the chest-relative path.
func (b *Batch) WriteArtifact(name string, data []byte) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", fmt.Errorf("invalid artifact name %q", name)
	}
	rel := filepath.Join(b.Dir(), batchArtifactDir, filepath.Base(name))
	if err := b.chest.WriteFile(rel, data); err != nil {
		return "", fmt.Errorf("writing batch artifact %s: %w", rel, err)
	}
	return rel, nil
}

// ReadArtifact loads previously persisted transformed bytes.
func (b *Batch) ReadArtifact(rel string) ([]byte, error) {
	fd, err := b.chest.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("opening batch artifact %s: %w", rel, err)
	}
	defer fd.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(fd); err != nil {
		return nil, fmt.Errorf("reading batch artifact %s: %w", rel, err)
	}
	return buf.Bytes(), nil
}
