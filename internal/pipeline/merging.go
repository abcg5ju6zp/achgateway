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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/storage"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/strx"
	"github.com/moov-io/base/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// cutoffTrigger describes what started one cutoff run: a scheduled window
// (automated) or a manual admin trigger.
type cutoffTrigger struct {
	// kind is BatchKindAutomated or BatchKindManual.
	kind string
	// window is the fired cutoff time in the shard's timezone (automated), or
	// the trigger instant for a manual run.
	window time.Time
}

// OutputHandler drives the externally-owned stages of a cutoff batch. Every
// stage is idempotent: recovery re-invokes only the stages whose facts the
// manifest does not yet record.
type OutputHandler interface {
	// Prepare transforms and formats one merged output. It fixes the remote
	// filename and persists the exact uploadable bytes as a batch artifact, so
	// a later stage can never re-render a name or re-transform content.
	Prepare(ctx context.Context, b *Batch, idx int, agent upload.Agent, file *ach.File) error
	// Upload confirms the prepared artifact at the remote. resumed is true
	// when the batch already recorded outputs on disk, so a prepared-but-
	// unconfirmed output is pre-checked by path and digest before uploading
	// to close the crash-after-ACK window. Ambiguous upload responses are
	// reconciled the same way; the audit copy is recorded after confirmation.
	Upload(ctx context.Context, b *Batch, idx int, agent upload.Agent, resumed bool) error
	// SendEvents sends FileUploaded for snapshot inputs not yet marked sent.
	SendEvents(ctx context.Context, b *Batch) error
}

// XferMerging represents logic for accepting ACH files to be merged together.
//
// The idea is to take Xfers and store them on a filesystem (or other durable storage)
// prior to a cutoff window. The specific storage could be based on the FileHeader.
//
// On the cutoff trigger WithEachMerged is called to merge files together and offer
// each merged file for an upload.
type XferMerging interface {
	HandleXfer(ctx context.Context, xfer incoming.ACHFile) error
	HandleCancel(ctx context.Context, cancel incoming.CancelACHFile) (incoming.FileCancellationResponse, error)

	// WithEachMerged opens or resumes the batch for trigger and drives every
	// merged output through the handler until no more local work remains.
	WithEachMerged(ctx context.Context, trigger cutoffTrigger, h OutputHandler) (mergedFiles, error)
	// RecoverActiveBatches resumes every active batch owned by this shard,
	// e.g. on process startup or before the next cutoff window. reason labels
	// recovery metrics ("startup", "cutoff", "manual").
	RecoverActiveBatches(ctx context.Context, reason string, h OutputHandler) error
}

func NewMerging(logger log.Logger, shard service.Shard, cfg service.UploadAgents) (XferMerging, error) {
	dir := strx.Or(
		cfg.Merging.Storage.Filesystem.Directory,
		cfg.Merging.Directory,
		"storage", // default directory
	)
	cfg.Merging.Storage.Filesystem.Directory = dir

	storage, err := storage.New(cfg.Merging.Storage)
	if err != nil {
		return nil, fmt.Errorf("problem creating %s: %w", dir, err)
	}

	return &filesystemMerging{
		logger:   logger,
		cfg:      cfg,
		storage:  storage,
		shard:    shard,
		newAgent: upload.New,
	}, nil
}

type filesystemMerging struct {
	logger  log.Logger
	cfg     service.UploadAgents
	shard   service.Shard
	storage storage.Chest

	// newAgent constructs the remote upload agent. It is a field so tests can
	// inject scripted agents without network access; production uses upload.New.
	newAgent func(logger log.Logger, cfg service.UploadAgents, id string) (upload.Agent, error)
}

func (m *filesystemMerging) HandleXfer(ctx context.Context, xfer incoming.ACHFile) error {
	if err := m.writeACHFile(ctx, xfer); err != nil {
		telemetry.RecordError(ctx, err)

		return m.logger.LogErrorf("problem writing ACH file: %v", err).Err()
	}
	return nil
}

func (m *filesystemMerging) writeACHFile(ctx context.Context, xfer incoming.ACHFile) error {
	// First, write the Nacha formatted file to disk
	var buf bytes.Buffer
	if err := ach.NewWriter(&buf).Write(xfer.File); err != nil {
		return err
	}

	fileID := strings.TrimSuffix(xfer.FileID, ".ach")
	path := filepath.Join("mergable", m.shard.Name, fileID+".ach")
	if err := m.storage.WriteFile(path, buf.Bytes()); err != nil {
		return err
	}

	// Second, write ValidateOpts to disk as well
	if opts := xfer.File.GetValidation(); opts != nil {
		buf.Reset()
		if err := json.NewEncoder(&buf).Encode(opts); err != nil {
			m.logger.Warn().With(log.Fields{
				"fileID":   log.String(xfer.FileID),
				"shardKey": log.String(xfer.ShardKey),
			}).Logf("ERROR encoding ValidateOpts: %v", err)
		}

		path := filepath.Join("mergable", m.shard.Name, fileID+".json")
		if err := m.storage.WriteFile(path, buf.Bytes()); err != nil {
			m.logger.Warn().With(log.Fields{
				"fileID":   log.String(xfer.FileID),
				"shardKey": log.String(xfer.ShardKey),
			}).Logf("ERROR writing ValidateOpts: %v", err)
		}
	}

	return nil
}

func (m *filesystemMerging) HandleCancel(ctx context.Context, cancel incoming.CancelACHFile) (incoming.FileCancellationResponse, error) {
	_, span := telemetry.StartSpan(ctx, "handle-cancel", trace.WithAttributes(
		attribute.String("achgateway.file_id", cancel.FileID),
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.shard_key", cancel.ShardKey),
	))
	defer span.End()

	fileID := strings.TrimSuffix(cancel.FileID, ".ach")
	path := filepath.Join("mergable", m.shard.Name, fileID+".ach")

	// Probe existence, then close before rename. Windows cannot rename a file
	// that still has an open handle.
	originalFile, _ := m.storage.Open(path)
	originalFileWasFound := originalFile != nil
	if originalFile != nil {
		originalFile.Close()
	}

	var canceledFileWasFound bool
	if !originalFileWasFound {
		canceledFile, _ := m.storage.Open(path + ".canceled")
		canceledFileWasFound = canceledFile != nil
		if canceledFile != nil {
			canceledFile.Close()
		}
	}

	// Write the canceled File
	err := m.storage.ReplaceFile(path, path+".canceled")
	if err != nil {
		span.RecordError(err)
	}

	successfulReplace := err == nil

	span.SetAttributes(
		attribute.Bool("achgateway.canceled_file_found", canceledFileWasFound),
		attribute.Bool("achgateway.cancel_replacement_written", successfulReplace),
		attribute.Bool("achgateway.original_file_found", originalFileWasFound),
		attribute.String("achgateway.path", path),
	)

	// We need a file to be found and we no errors during the rename
	var successful bool = (originalFileWasFound || canceledFileWasFound) && successfulReplace

	out := incoming.FileCancellationResponse{
		FileID:     cancel.FileID,
		ShardKey:   cancel.ShardKey,
		Successful: successful,
	}
	return out, err
}

func (m *filesystemMerging) isolateMergableDir(ctx context.Context) (string, error) {
	newdir := uniqueIsolatedDir(m.storage, fmt.Sprintf("%s-%v", m.shard.Name, time.Now().Format("20060102-150405")))

	_, span := telemetry.StartSpan(ctx, "isolate-mergable-dir", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", newdir),
	))
	defer span.End()

	// Otherwise attempt to isolate the directory
	return newdir, m.storage.ReplaceDir(filepath.Join("mergable", m.shard.Name), newdir)
}

// uniqueIsolatedDir avoids replacing or reusing a same-second directory: a
// missing ReplaceDir source is a no-op MkdirAll, so a colliding name would
// merge a new cutoff into an earlier isolated batch (or overwrite its manifest).
func uniqueIsolatedDir(chest storage.Chest, base string) string {
	name := base
	for n := 1; ; n++ {
		if fd, err := chest.Open(filepath.Join(name, batchManifestFile)); err == nil {
			fd.Close()
			name = fmt.Sprintf("%s-%d", base, n)
			continue
		}
		entries, err := chest.ReadDir(name)
		if err == nil && len(entries) > 0 {
			name = fmt.Sprintf("%s-%d", base, n)
			continue
		}
		return name
	}
}

func (m *filesystemMerging) getCanceledFiles(ctx context.Context, dir string) ([]string, error) {
	_, span := telemetry.StartSpan(ctx, "get-canceled-files", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", dir),
	))
	defer span.End()

	matches, err := m.storage.Glob(dir + "/*.canceled")
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Int("achgateway.canceled_files", len(matches)))

	out := make([]string, len(matches))
	for i := range matches {
		_, filename := filepath.Split(matches[i].RelativePath)
		out[i] = strings.TrimSuffix(filename, ".canceled")
	}
	return out, nil
}

func (m *filesystemMerging) getNonCanceledMatches(ctx context.Context, dir string) ([]string, error) {
	_, span := telemetry.StartSpan(ctx, "get-non-canceled-matches", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", dir),
	))
	defer span.End()

	positiveMatches, err := m.storage.Glob(dir + "/*.ach")
	if err != nil {
		return nil, err
	}
	negativeMatches, err := m.storage.Glob(dir + "/*.canceled")
	if err != nil {
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("achgateway.positive_matches", len(positiveMatches)),
		attribute.Int("achgateway.negative_matches", len(negativeMatches)),
	)

	// Canceled files are written by HandleCancel as "<path>.canceled" where path
	// is the original ".ach" path (e.g. "dir/foo.ach.canceled"). Older/manual
	// layouts may use "dir/foo.canceled" beside "dir/foo.ach".
	//
	// storage.Glob is non-recursive on a single directory, so every RelativePath
	// shares the same dir prefix — full-stem equality is sufficient and avoids
	// basename false positives across unrelated directories.
	canceledStems := make(map[string]struct{}, len(negativeMatches))
	for j := range negativeMatches {
		stem := strings.TrimSuffix(negativeMatches[j].RelativePath, ".canceled")
		canceledStems[stem] = struct{}{}
	}

	out := make([]string, 0, len(positiveMatches))
	for i := range positiveMatches {
		path := positiveMatches[i].RelativePath
		if _, skip := canceledStems[path]; skip {
			continue
		}
		// "dir/foo.ach" canceled as "dir/foo.canceled"
		if stem, ok := stripACHExtension(path); ok {
			if _, skip := canceledStems[stem]; skip {
				continue
			}
		}
		out = append(out, path)
	}
	return out, nil
}

func (m *filesystemMerging) createMergeConditions() ach.Conditions {
	var mergeConditions ach.Conditions
	if m.shard.Mergable.Conditions != nil {
		mergeConditions = *m.shard.Mergable.Conditions
	}

	// Always cap the MaxDollarAmount at Nacha's file-level debit/credit limit.
	if mergeConditions.MaxDollarAmount == 0 || mergeConditions.MaxDollarAmount > ach.NachaFileDebitCreditLimit {
		mergeConditions.MaxDollarAmount = ach.NachaFileDebitCreditLimit
	}

	return mergeConditions
}

func (m *filesystemMerging) createMergeDirOptions(canceledFiles []string) *ach.MergeDirOptions {
	return &ach.MergeDirOptions{
		AcceptFile:            fileAcceptor(canceledFiles),
		ValidateOptsExtension: ".json",
		FS:                    m.storage,
	}
}

// WithEachMerged opens (or resumes) the cutoff batch for trigger, merges the
// isolated inputs once, and drives every merged output through the handler's
// prepare→upload→events stages. Every confirmed fact is recorded in the batch
// manifest before the next stage, so re-running after a crash resumes without
// a second upload of any confirmed output.
func (m *filesystemMerging) WithEachMerged(ctx context.Context, trigger cutoffTrigger, h OutputHandler) (mergedFiles, error) {
	b, err := m.openBatch(ctx, trigger)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return m.runBatch(ctx, b, h)
}

// RecoverActiveBatches resumes every active batch owned by this shard. Each
// batch fails independently; the joined error aggregates failures without
// interrupting the remaining batches.
func (m *filesystemMerging) RecoverActiveBatches(ctx context.Context, reason string, h OutputHandler) error {
	batches, err := activeBatches(ctx, m.storage, m.shard.Name)
	if err != nil {
		return fmt.Errorf("recovering cutoff batches: %w", err)
	}
	if len(batches) > 0 {
		m.logger.Info().With(log.Fields{
			"shard":  log.String(m.shard.Name),
			"reason": log.String(reason),
			"count":  log.Int(len(batches)),
		}).Log("resuming active cutoff batches")
	}
	var el base.ErrorList
	for i := range batches {
		pendingBefore := batches[i].PendingOutputs()
		if _, err := m.runBatch(ctx, batches[i], h); err != nil {
			el.Add(fmt.Errorf("recovering batch %s: %w", batches[i].ID(), err))
		}
		if advanced := pendingBefore - batches[i].PendingOutputs(); advanced > 0 {
			recordRecoveredOutputs(m.shard.Name, reason, advanced)
		}
	}
	if el.Empty() {
		return nil
	}
	return joinStageErrors(el)
}

// openBatch implements find-or-create for a cutoff run: an automated trigger
// resumes the active batch for the same window identity, a manual trigger
// resumes the newest active batch; otherwise the pending mergable directory is
// isolated and a new manifest is written before any remote work starts.
func (m *filesystemMerging) openBatch(ctx context.Context, trigger cutoffTrigger) (*Batch, error) {
	switch trigger.kind {
	case BatchKindAutomated:
		id := automatedBatchID(m.shard.Name, trigger.window)
		existing, err := findActiveBatchID(ctx, m.storage, m.shard.Name, id)
		if err != nil {
			return nil, fmt.Errorf("locating active batch %s: %w", id, err)
		}
		if existing != nil {
			m.logger.Info().With(log.Fields{
				"shard": log.String(m.shard.Name),
				"batch": log.String(id),
			}).Log("reusing active cutoff batch for window")
			return existing, nil
		}
	default:
		existing, err := newestActiveBatch(ctx, m.storage, m.shard.Name)
		if err != nil {
			return nil, fmt.Errorf("locating active batch: %w", err)
		}
		if existing != nil {
			m.logger.Info().With(log.Fields{
				"shard": log.String(m.shard.Name),
				"batch": log.String(existing.ID()),
			}).Log("manual trigger resuming newest active cutoff batch")
			return existing, nil
		}
	}

	// move the current directory so it's isolated and easier to debug later on
	dir, err := m.isolateMergableDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("problem isolating newdir=%s error=%v", dir, err)
	}

	// No positive inputs (empty or canceled-only): preserve the legacy
	// empty-cutoff behavior exactly — no manifest, drop the isolated directory.
	expectedInputs, listErr := m.getNonCanceledMatches(ctx, dir)
	if listErr != nil {
		return nil, fmt.Errorf("listing input files in %s: %w", dir, listErr)
	}
	if len(expectedInputs) == 0 {
		if err := m.storage.RmdirAll(dir); err != nil {
			return nil, err
		}
		return nil, nil
	}

	inputs, err := m.snapshotInputs(dir, expectedInputs)
	if err != nil {
		return nil, err
	}

	var id, window string
	if trigger.kind == BatchKindAutomated {
		id = automatedBatchID(m.shard.Name, trigger.window)
		window = trigger.window.Format("15:04")
		// Inputs arriving after an earlier same-window run get a fresh isolated
		// directory; give them a distinct identity suffixed with that
		// directory's timestamp so two completed batches never share an ID.
		if existing, err := batchWithIDExists(ctx, m.storage, m.shard.Name, id); err == nil && existing {
			id = fmt.Sprintf("%s-%s", id, dirTimestampSuffix(dir, m.shard.Name))
		}
	} else {
		id = manualBatchID(m.shard.Name, trigger.window)
		window = "manual-" + trigger.window.Format("150405")
	}
	kind := trigger.kind
	if kind == "" {
		kind = BatchKindManual
	}

	b := newBatch(m.storage, BatchState{
		ID:     id,
		Shard:  m.shard.Name,
		Window: window,
		Kind:   kind,
		Dir:    dir,
		Inputs: inputs,
	})
	if err := b.Persist(ctx); err != nil {
		return nil, fmt.Errorf("persisting batch %s: %w", id, err)
	}
	m.logger.Info().With(log.Fields{
		"shard":  log.String(m.shard.Name),
		"batch":  log.String(id),
		"dir":    log.String(dir),
		"inputs": log.Int(len(inputs)),
		"kind":   log.String(kind),
	}).Log("cutoff batch created with fixed input snapshot")
	return b, nil
}

// snapshotInputs fixes the input set (name, size, SHA-256) at trigger time.
// Files arriving in mergable/ afterwards belong to a later window.
func (m *filesystemMerging) snapshotInputs(dir string, paths []string) ([]BatchInput, error) {
	inputs := make([]BatchInput, 0, len(paths))
	for i := range paths {
		name := filepath.Base(paths[i])
		full := filepath.Join(dir, name)

		fd, err := m.storage.Open(full)
		if err != nil {
			return nil, fmt.Errorf("snapshotting %s: %w", full, err)
		}
		var buf bytes.Buffer
		_, readErr := buf.ReadFrom(fd)
		fd.Close()
		if readErr != nil {
			return nil, fmt.Errorf("snapshotting %s: %w", full, readErr)
		}
		inputs = append(inputs, BatchInput{
			Filename:    name,
			Size:        int64(buf.Len()),
			SHA256:      hash(buf.Bytes()),
			OutputIndex: unmappedOutput,
		})
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Filename < inputs[j].Filename })
	return inputs, nil
}

// runBatch drives one active batch through merge (once) and the per-output
// prepare/upload stages, then sends pending events and flips the batch to
// completed when every stage is confirmed.
func (m *filesystemMerging) runBatch(ctx context.Context, b *Batch, h OutputHandler) (mergedFiles, error) {
	state := b.State()
	dir := state.Dir
	// resumed tells the handler whether prepared artifacts predate this run:
	// a crash after the upload ACK but before the manifest flushed can only be
	// detected by checking the remote before writing again.
	resumed := len(state.Outputs) > 0

	_, span := telemetry.StartSpan(ctx, "with-each-merged", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", dir),
		attribute.String("achgateway.batch_id", state.ID),
	))
	defer span.End()

	logger := m.logger.Set("shardName", log.String(m.shard.Name))

	var el base.ErrorList
	var parsed []*ach.File

	// Merge phase only runs until outputs have been recorded in the manifest.
	if len(state.Outputs) == 0 {
		files, mergeErrs := m.mergeBatchOutputs(ctx, b, dir, span)
		for _, err := range mergeErrs {
			el.Add(err)
		}
		state = b.State()

		// Merge produced nothing: either a fail-closed error (keep inputs for
		// a retry) or genuinely zero outputs (complete the empty batch so it
		// is not recovered forever).
		if len(state.Outputs) == 0 {
			if el.Empty() {
				if err := b.MarkCompleted(); err != nil {
					el.Add(fmt.Errorf("completing empty batch %s: %w", b.ID(), err))
				} else if err := b.Persist(ctx); err != nil {
					el.Add(fmt.Errorf("persisting empty batch %s: %w", b.ID(), err))
				}
				return nil, joinStageErrors(el)
			}
			return nil, joinStageErrors(el)
		}
		parsed = files // index-aligned with state.Outputs
	}
	if parsed == nil {
		parsed = make([]*ach.File, len(state.Outputs))
	}

	uploadedDir := filepath.Join(dir, "uploaded")
	m.storage.MkdirAll(uploadedDir)

	// Grab our upload Agent
	newAgent := m.newAgent
	if newAgent == nil {
		newAgent = upload.New
	}
	agent, err := newAgent(m.logger, m.cfg, m.shard.UploadAgent)
	if err != nil {
		return nil, fmt.Errorf("%s merging agent: %v", m.shard.Name, err)
	}
	logger.Logf("found %T agent", agent)

	// Per-output stages. Failures are recorded without aborting later outputs,
	// matching the partial-success behavior.
	eligibleUploads := len(state.Outputs)
	for i := range state.Outputs {
		out := state.Outputs[i]
		if out.Conflict {
			el.Add(fmt.Errorf("output %d (%s) frozen: %w", i, out.Filename, upload.ErrRemoteContentConflict))
			continue
		}

		if !out.Uploaded || !out.Audited {
			if out.ArtifactPath == "" {
				// Prefer the file parsed in this run (a failed local cache
				// save must not block the upload); fall back to the cache.
				file := parsed[i]
				if file == nil {
					reloaded, parseErr := m.readMergedFile(out)
					if parseErr != nil {
						el.Add(fmt.Errorf("reading merged file %d for output stage: %w", i, parseErr))
						continue
					}
					file = reloaded
					parsed[i] = file
				}

				// Prepare fixes the remote filename and artifact bytes; persist
				// before any remote call so an ambiguous upload is recoverable.
				if err := h.Prepare(ctx, b, i, agent, file); err != nil {
					el.Add(fmt.Errorf("problem preparing merged file %d: %w", i, err))
					continue
				}
				if err := b.Persist(ctx); err != nil {
					el.Add(err)
					continue
				}
			}

			if err := h.Upload(ctx, b, i, agent, resumed); err != nil {
				span.RecordError(err)
				el.Add(fmt.Errorf("problem uploading merged file %d: %w", i, err))
				_ = b.Persist(ctx)
				continue
			}
			if err := b.Persist(ctx); err != nil {
				el.Add(err)
				continue
			}
		}
	}

	// Events for whatever is confirmed uploaded — partial failures still emit
	// for successful outputs, and SendEvents itself is idempotent per input.
	if err := h.SendEvents(ctx, b); err != nil {
		err = fmt.Errorf("sending FileUploaded events: %w", err)
		span.RecordError(err)
		el.Add(err)
	}
	_ = b.Persist(ctx)

	// Build the result from the persisted facts.
	state = b.State()
	merged := m.buildMergedResult(b, parsed)

	successfulRemoteWrites := 0
	for i := range state.Outputs {
		if state.Outputs[i].Uploaded {
			successfulRemoteWrites++
		}
	}
	logger.Logf("wrote %d of %d files to remote agent", successfulRemoteWrites, eligibleUploads)
	span.SetAttributes(
		attribute.Int("achgateway.successful_remote_writes", successfulRemoteWrites),
		attribute.Int("achgateway.batch_outputs", len(state.Outputs)),
	)

	// Completeness gap for snapshot inputs that never reached an uploaded output.
	// Inputs of not-uploaded outputs carry that output's index, so only true
	// unmapped inputs surface here.
	var unmapped []string
	for i := range state.Inputs {
		if state.Inputs[i].OutputIndex == unmappedOutput {
			unmapped = append(unmapped, state.Inputs[i].Filename)
		}
	}
	sort.Strings(unmapped)
	span.SetAttributes(
		attribute.Int("achgateway.expected_input_files", len(state.Inputs)),
		attribute.Int("achgateway.mapped_input_files", len(state.Inputs)-len(unmapped)),
		attribute.Int("achgateway.unmapped_input_files", len(unmapped)),
	)
	// Only flag genuine mapping gaps when money moved. Total upload failure
	// already surfaces via those errors without an unmapped double-count.
	if len(unmapped) > 0 && successfulRemoteWrites > 0 {
		recordUnmappedInputFiles(m.shard.Name, len(unmapped))
		unmappedErr := fmt.Errorf("%d input file(s) in %s were not mapped to an uploaded file (FileUploaded will not be emitted for them): %s",
			len(unmapped), dir, summarizeFilenames(unmapped, 20))
		logger.Error().LogError(unmappedErr)
		span.RecordError(unmappedErr)
		el.Add(unmappedErr)
	}

	// Flip the batch only when every stage is confirmed; active batches stay
	// eligible for the next recovery pass and ineligible for input cleanup.
	if b.IsComplete() {
		if err := b.MarkCompleted(); err != nil {
			el.Add(err)
		} else {
			logger.Info().With(log.Fields{
				"batch":   log.String(state.ID),
				"outputs": log.Int(len(state.Outputs)),
				"inputs":  log.Int(len(state.Inputs)),
			}).Log("cutoff batch completed")
		}
	}
	if err := b.Persist(ctx); err != nil {
		el.Add(err)
	}

	if el.Empty() {
		return merged, nil
	}
	return merged, joinStageErrors(el)
}

// joinStageErrors turns an ErrorList into a joined error so errors.Is/As
// (e.g. upload.ErrRemoteContentConflict) traverse every stage failure.
func joinStageErrors(el base.ErrorList) error {
	if el.Empty() {
		return nil
	}
	return errors.Join(el...)
}

// mergeBatchOutputs performs the one-time merge step: list canceled inputs,
// merge, optionally flatten, cache every merged file, and record the
// input→output mapping in the manifest. Errors for individual files are
// returned for the ErrorList without aborting sibling work.
func (m *filesystemMerging) mergeBatchOutputs(ctx context.Context, b *Batch, dir string, span trace.Span) ([]*ach.File, []error) {
	logger := m.logger.Set("shardName", log.String(m.shard.Name))

	canceledFiles, err := m.getCanceledFiles(ctx, dir)
	if err != nil {
		return nil, []error{fmt.Errorf("problem listing %s for canceled files: %w", dir, err)}
	}

	mergeConditions := m.createMergeConditions()
	opts := m.createMergeDirOptions(canceledFiles)
	files, mergeErr := m.mergeDir(ctx, dir, mergeConditions, opts)

	var errs []error
	if mergeErr != nil {
		errs = append(errs, fmt.Errorf("unable to merge files: %w", mergeErr))
	}
	if len(files) == 0 {
		return nil, errs
	}

	mappings, mapErrs := m.buildDirMapping(dir, canceledFiles)
	for _, mapErr := range mapErrs {
		span.RecordError(mapErr)
		errs = append(errs, mapErr)
	}
	if dupFiles := crossFileDuplicateIdentityFiles(mappings); len(dupFiles) > 0 {
		// Observability only — do not block upload or fail the cutoff.
		span.SetAttributes(
			attribute.Int("achgateway.duplicate_entry_identity_files", len(dupFiles)),
			attribute.StringSlice("achgateway.duplicate_entry_identity_filenames", sampleStrings(dupFiles, 20)),
		)
		logger.Info().Logf("%d input file(s) share duplicate entry identity (all will upload and map): %s",
			len(dupFiles), summarizeFilenames(dupFiles, 20))
	}

	uploadedDir := filepath.Join(dir, "uploaded")
	m.storage.MkdirAll(uploadedDir)

	paths := make([]string, len(files))
	shas := make([]string, len(files))
	for i := range files {
		// Optionally Flatten Batches. ach.File.FlattenBatches returns a new file
		// on success and does not mutate the receiver on error — keep the original
		// and still upload so money moves; surface the flatten error for ops.
		if m.shard.Mergable.FlattenBatches != nil {
			if file, flatErr := m.flattenBatches(ctx, files[i]); flatErr != nil {
				flatErr = fmt.Errorf("flatten batches for merged file %d: %w", i, flatErr)
				span.RecordError(flatErr)
				errs = append(errs, flatErr)
			} else {
				files[i] = file
			}
		}

		// Best-effort local uploaded/ cache for audit/debug. A save failure must
		// not block the ODFI upload — money movement takes priority.
		path, sha, saveErr := m.saveMergedFile(ctx, uploadedDir, files[i])
		if saveErr != nil {
			saveErr = fmt.Errorf("writing merged file %d to local cache: %w", i, saveErr)
			span.RecordError(saveErr)
			errs = append(errs, saveErr)
			logger.Error().Logf("local cache save failed for merged file %d; continuing with ODFI upload: %v", i, saveErr)
			continue
		}
		paths[i] = path
		shas[i] = sha
	}

	// Map entries back to snapshot inputs. findInputFilepaths consumes the
	// identity queues; clone because it deletes keys as it matches.
	mapped, leftoverEntryKeys := m.findInputFilepaths(maps.Clone(mappings), mergedFilesFromFiles(files, m.shard.Name))
	span.SetAttributes(attribute.Int("achgateway.unmapped_entry_keys", leftoverEntryKeys))

	state := b.State()
	indexByName := make(map[string]int, len(state.Inputs))
	for i := range state.Inputs {
		indexByName[state.Inputs[i].Filename] = i
	}

	outputs := make([]BatchOutput, len(files))
	for i := range mapped {
		bo := BatchOutput{
			Index:        i,
			MergedPath:   paths[i],
			MergedSHA256: shas[i],
		}
		for _, name := range mapped[i].InputFilepaths {
			if idx, ok := indexByName[name]; ok {
				bo.Inputs = append(bo.Inputs, idx)
			}
		}
		outputs[i] = bo
	}
	b.InitializeOutputs(outputs)
	if err := b.Persist(ctx); err != nil {
		errs = append(errs, err)
	}
	return files, errs
}

func mergedFilesFromFiles(files []*ach.File, shardName string) mergedFiles {
	out := make(mergedFiles, len(files))
	for i := range files {
		out[i] = mergedFile{ACHFile: files[i], Shard: shardName}
	}
	return out
}

// readMergedFile loads a merged Nacha file (plus its ValidateOpts sidecar)
// from the uploaded/ cache, used when a restarted batch must re-prepare an
// output whose transform artifact is missing.
func (m *filesystemMerging) readMergedFile(out BatchOutput) (*ach.File, error) {
	if out.MergedPath == "" {
		return nil, fmt.Errorf("merged cache missing for output %d", out.Index)
	}
	fd, err := m.storage.Open(out.MergedPath)
	if err != nil {
		return nil, fmt.Errorf("opening merged cache %s: %w", out.MergedPath, err)
	}
	defer fd.Close()

	var validateOpts *ach.ValidateOpts
	optsPath := strings.TrimSuffix(out.MergedPath, filepath.Ext(out.MergedPath)) + ".json"
	if optsFD, err := m.storage.Open(optsPath); err == nil {
		if optsFD != nil {
			defer optsFD.Close()
		}
		if jsonErr := json.NewDecoder(optsFD).Decode(&validateOpts); jsonErr != nil {
			return nil, fmt.Errorf("reading %s as validate opts: %w", optsPath, jsonErr)
		}
	}

	rdr := ach.NewReader(fd)
	if validateOpts != nil {
		rdr.SetValidation(validateOpts)
	}
	file, err := rdr.Read()
	if err != nil {
		return nil, fmt.Errorf("reading merged cache %s: %w", out.MergedPath, err)
	}
	return &file, nil
}

// buildMergedResult restores the legacy mergedFiles return from the manifest
// facts: only uploaded outputs, with mapped snapshot basenames and parsed files.
func (m *filesystemMerging) buildMergedResult(b *Batch, parsed []*ach.File) mergedFiles {
	state := b.State()
	var out mergedFiles
	for i := range state.Outputs {
		o := state.Outputs[i]
		if !o.Uploaded {
			continue
		}
		var names []string
		for _, inIdx := range o.Inputs {
			if inIdx >= 0 && inIdx < len(state.Inputs) {
				names = append(names, state.Inputs[inIdx].Filename)
			}
		}
		mf := mergedFile{
			UploadedFilename: o.Filename,
			Shard:            m.shard.Name,
			BatchID:          state.ID,
			UploadedAt:       o.UploadedAt,
			InputFilepaths:   names,
		}
		if i < len(parsed) && parsed[i] != nil {
			mf.ACHFile = parsed[i]
		} else if file, err := m.readMergedFile(o); err == nil {
			mf.ACHFile = file
		}
		out = append(out, mf)
	}
	return out
}

func (m *filesystemMerging) mergeDir(ctx context.Context, dir string, mergeConditions ach.Conditions, opts *ach.MergeDirOptions) ([]*ach.File, error) {
	_, span := telemetry.StartSpan(ctx, "ach-merge-dir", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", dir),
		attribute.Int("achgateway.merge_max_lines", mergeConditions.MaxLines),
		attribute.Int64("achgateway.merge_max_dollar_amount", mergeConditions.MaxDollarAmount),
	))
	defer span.End()

	files, err := ach.MergeDir(dir, mergeConditions, opts)
	if err != nil {
		span.RecordError(err)
	}

	span.SetAttributes(attribute.Int("achgateway.merged_files", len(files)))

	return files, err
}

func (m *filesystemMerging) flattenBatches(ctx context.Context, file *ach.File) (*ach.File, error) {
	_, span := telemetry.StartSpan(ctx, "ach-flatten-batches", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
	))
	defer span.End()

	return file.FlattenBatches()
}

// fileAcceptor builds a MergeDir AcceptFile callback.
// canceledFiles must be basenames from getCanceledFiles (e.g. "foo.ach"), already
// stripped of the ".canceled" suffix — matched against filepath.Base of each walk path.
func fileAcceptor(canceledFiles []string) func(string) ach.FileAcceptance {
	// O(1) lookup instead of slices.Contains on every path during MergeDir walks.
	canceled := make(map[string]struct{}, len(canceledFiles))
	for _, name := range canceledFiles {
		// Index basename only — getCanceledFiles always returns basenames.
		canceled[filepath.Base(name)] = struct{}{}
	}

	return func(path string) ach.FileAcceptance {
		// Reject canceled files
		if strings.Contains(path, ".canceled") {
			return ach.SkipFile
		}
		_, filename := filepath.Split(path)
		if _, skip := canceled[filename]; skip {
			return ach.SkipFile
		}

		// Only accept .ach files (case-insensitive, allocation-free suffix check)
		if hasACHExtension(path) {
			return ach.AcceptFile
		}
		return ach.SkipFile
	}
}

// hasACHExtension reports whether path ends with ".ach" ignoring case.
func hasACHExtension(path string) bool {
	return len(path) >= 4 && strings.EqualFold(path[len(path)-4:], ".ach")
}

// stripACHExtension returns path without a trailing ".ach"/".ACH" suffix.
func stripACHExtension(path string) (string, bool) {
	if !hasACHExtension(path) {
		return path, false
	}
	return path[:len(path)-4], true
}

// trimACHExtension returns path without a trailing ".ach"/".ACH" suffix, or
// path unchanged when the suffix is absent.
func trimACHExtension(path string) string {
	if stem, ok := stripACHExtension(path); ok {
		return stem
	}
	return path
}

// entryFileMapping maps entry identity keys to the input filename(s) they came from.
// A key may map to multiple filenames when distinct file_ids carry identical ACH
// entry content — moov-io/ach keeps each entry, and we must emit FileUploaded for each.
type entryFileMapping map[string][]string

// buildDirMapping computes a map of the input files and their entries together so that we can quickly find
// where they were merged into.
//
// Errors reading individual input files are collected and returned without aborting the walk —
// remaining files still contribute to the mapping so FileUploaded can be emitted for them.
// A directory-level ReadDir failure returns a nil map and a single error.
func (m *filesystemMerging) buildDirMapping(dir string, canceledFiles []string) (entryFileMapping, []error) {
	mappings := make(entryFileMapping)
	// seen[identityKey][filename] for O(1) dedupe while building queues.
	seen := make(map[string]map[string]struct{})

	fds, err := m.storage.ReadDir(dir)
	if err != nil {
		return nil, []error{err}
	}

	acceptor := fileAcceptor(canceledFiles)
	var errs []error

	for i := range fds {
		path := fds[i].Name()

		// Ignore directories as ReadDir continues inside of them.
		// .json files contain ValidateOpts which we can skip
		if fds[i].IsDir() || strings.HasSuffix(path, ".json") {
			continue
		}

		// Skip the file if merging would have skipped it
		if acceptor(path) == ach.SkipFile {
			continue
		}

		fullPath := filepath.Join(dir, path)
		if err := m.accumulateMappings(mappings, seen, fullPath); err != nil {
			errs = append(errs, fmt.Errorf("accumulating mappings from %s failed: %w", path, err))
			// Continue so one bad input does not block FileUploaded for the rest.
			continue
		}
	}

	return mappings, errs
}

func (m *filesystemMerging) accumulateMappings(mappings entryFileMapping, seen map[string]map[string]struct{}, path string) error {
	fd, err := m.storage.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s failed: %w", path, err)
	}
	defer fd.Close()

	// Check for validate opts
	validateOptsPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
	var validateOpts *ach.ValidateOpts
	if optsFD, err := m.storage.Open(validateOptsPath); err == nil {
		if optsFD != nil {
			defer optsFD.Close()
		}

		err = json.NewDecoder(optsFD).Decode(&validateOpts)
		if err != nil {
			return fmt.Errorf("reading %s as validate opts failed: %w", validateOptsPath, err)
		}
	}

	rdr := ach.NewReader(fd)
	if validateOpts != nil {
		rdr.SetValidation(validateOpts)
	}

	file, err := rdr.Read()
	if err != nil {
		return fmt.Errorf("reading %s failed: %w", path, err)
	}

	_, filename := filepath.Split(path)

	// Queue each BatchHeader+Entry under its identity key.
	//
	//   - Same entry under different file_ids → each file_id is queued (upload
	//     each entry; FileUploaded for each file_id).
	//   - Same file_id already queued for this key → skip (idempotent re-queue /
	//     duplicate rows in one file only need one FileUploaded for that file_id).
	for i := range file.Batches {
		bh := file.Batches[i].GetHeader()

		entries := file.Batches[i].GetEntries()

		for idx := range entries {
			if entries[idx] == nil {
				continue
			}
			key := makeKey(bh, entries[idx])
			if seen[key] == nil {
				seen[key] = make(map[string]struct{})
			}
			if _, exists := seen[key][filename]; exists {
				continue
			}
			seen[key][filename] = struct{}{}
			mappings[key] = append(mappings[key], filename)
		}
	}

	return nil
}

// crossFileDuplicateIdentityFiles returns sorted unique basenames that share an
// entry identity key with at least one other distinct input file.
func crossFileDuplicateIdentityFiles(mappings entryFileMapping) []string {
	if len(mappings) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	for _, names := range mappings {
		uniq := uniqueSortedStrings(names)
		if len(uniq) < 2 {
			continue
		}
		for _, name := range uniq {
			seen[name] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

func uniqueSortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}

func mappingLeftoverCount(mappings entryFileMapping) int {
	n := 0
	for _, names := range mappings {
		n += len(names)
	}
	return n
}

func makeKey(bh *ach.BatchHeader, entry *ach.EntryDetail) string {
	// Callers skip nil entries. Do not return "" — that would collide in the mapping.
	if entry == nil {
		return "\x00nil-entry"
	}

	// MergeDir combines batches with BatchHeader.Equal, then copies entries under
	// the first matching header. Equal ignores CompanyDiscretionaryData,
	// CompanyDescriptiveDate, SettlementDate, OriginatorStatusCode, and company
	// name case — all of which appear in BatchHeader.String()[:87] (and BatchNumber,
	// which merge also rewrites). Keys built from the original 87-char header
	// therefore miss after merge, so FileUploaded is not emitted for those inputs.
	var b strings.Builder
	if bh != nil {
		b.Grow(96 + 94)
		b.WriteString(strconv.Itoa(bh.ServiceClassCode))
		b.WriteByte('|')
		b.WriteString(strings.ToUpper(bh.CompanyName))
		b.WriteByte('|')
		b.WriteString(bh.CompanyIdentification)
		b.WriteByte('|')
		b.WriteString(bh.StandardEntryClassCode)
		b.WriteByte('|')
		b.WriteString(bh.CompanyEntryDescription)
		b.WriteByte('|')
		b.WriteString(bh.EffectiveEntryDate)
		b.WriteByte('|')
		b.WriteString(bh.ODFIIdentification)
		b.WriteByte('|')
	}
	b.WriteString(entry.String())
	return b.String()
}

// mergedFile represents a single merged file structure used in the merging and uploading process.
type mergedFile struct {
	InputFilepaths   []string
	UploadedFilename string
	ACHFile          *ach.File
	Shard            string
	BatchID          string
	// UploadedAt, when set, is stamped on FileUploaded events instead of now
	// (manual backfill of a batch reuses the batch's recorded upload time).
	UploadedAt time.Time
}

type mergedFiles []mergedFile

// findInputFilepaths matches merged ACH entries back to their input filenames.
// It returns the merged files with InputFilepaths populated and the number of
// leftover mapping slots (input entry identities not matched — typically none
// after a full merge).
//
// When several input files share one entry identity, each matching merged entry
// consumes one queued filename so every file_id can receive FileUploaded.
func (m *filesystemMerging) findInputFilepaths(mappings entryFileMapping, merged mergedFiles) (mergedFiles, int) {
	// Compare each merged file against mappings
	for i := range merged {
		if merged[i].ACHFile == nil {
			continue
		}
		for j := range merged[i].ACHFile.Batches {
			batch := merged[i].ACHFile.Batches[j]

			bh := batch.GetHeader()
			entries := batch.GetEntries()

			for idx := range entries {
				if entries[idx] == nil {
					continue
				}
				key := makeKey(bh, entries[idx])

				names := mappings[key]
				if len(names) == 0 {
					continue
				}
				merged[i].InputFilepaths = append(merged[i].InputFilepaths, names[0])
				if len(names) == 1 {
					delete(mappings, key)
				} else {
					mappings[key] = names[1:]
				}
			}
		}

		// Compact duplicate basenames from multi-entry inputs in one merged file.
		// Safe: isolateMergableDir is flat (no nested ACH dirs), and accumulateMappings
		// stores filepath.Base only — two distinct inputs cannot share a basename.
		slices.Sort(merged[i].InputFilepaths)
		merged[i].InputFilepaths = slices.Compact(merged[i].InputFilepaths)
	}
	return merged, mappingLeftoverCount(mappings)
}

// missingMappedInputFiles returns base filenames from expectedInputs that do not
// appear in any merged file's InputFilepaths. expectedInputs may be relative paths.
func missingMappedInputFiles(expectedInputs []string, merged mergedFiles) []string {
	if len(expectedInputs) == 0 {
		return nil
	}

	mapped := make(map[string]struct{}, len(expectedInputs))
	for i := range merged {
		for _, p := range merged[i].InputFilepaths {
			mapped[filepath.Base(p)] = struct{}{}
		}
	}

	var missing []string
	seen := make(map[string]struct{}, len(expectedInputs))
	for _, p := range expectedInputs {
		name := filepath.Base(p)
		if name == "" || name == "." || name == string(filepath.Separator) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if _, ok := mapped[name]; !ok {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	return missing
}

func countMappedInputFiles(merged mergedFiles) int {
	seen := make(map[string]struct{})
	for i := range merged {
		for _, p := range merged[i].InputFilepaths {
			seen[filepath.Base(p)] = struct{}{}
		}
	}
	return len(seen)
}

func summarizeFilenames(names []string, limit int) string {
	if len(names) == 0 {
		return ""
	}
	if limit <= 0 || len(names) <= limit {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s, ... (+%d more)", strings.Join(names[:limit], ", "), len(names)-limit)
}

func sampleStrings(names []string, limit int) []string {
	if limit <= 0 || len(names) <= limit {
		return names
	}
	return names[:limit]
}

// saveMergedFile writes the merged Nacha file (and ValidateOpts sidecar) into
// the uploaded/ cache and returns its chest-relative path and content digest.
func (m *filesystemMerging) saveMergedFile(ctx context.Context, dir string, file *ach.File) (string, string, error) {
	_, span := telemetry.StartSpan(ctx, "saved-merged-file", trace.WithAttributes(
		attribute.String("achgateway.shard", m.shard.Name),
		attribute.String("achgateway.dir", dir),
	))
	defer span.End()

	var buf bytes.Buffer
	err := ach.NewWriter(&buf).Write(file)
	if err != nil {
		return "", "", fmt.Errorf("unable to buffer ACH file: %w", err)
	}

	name := hash(buf.Bytes())
	path := filepath.Join(dir, name+".ach")

	span.SetAttributes(
		attribute.String("achgateway.merged_filename", path),
		attribute.Int("achgateway.merged_filesize_bytes", buf.Len()),
	)

	err = m.storage.WriteFile(path, buf.Bytes())
	if err != nil {
		return "", "", fmt.Errorf("writing merged ACH file: %w", err)
	}

	validateOpts := file.GetValidation()
	if validateOpts != nil {
		buf.Reset()

		err = json.NewEncoder(&buf).Encode(validateOpts)
		if err != nil {
			return "", "", fmt.Errorf("marshal of merged ACH file validate opts: %w", err)
		}

		optsPath := filepath.Join(dir, name+".json")
		err = m.storage.WriteFile(optsPath, buf.Bytes())
		if err != nil {
			return "", "", fmt.Errorf("writing merged ACH file validate opts: %w", err)
		}
	}

	return path, name, nil
}

func hash(data []byte) string {
	ss := sha256.New()
	ss.Write(data)
	return hex.EncodeToString(ss.Sum(nil))
}
