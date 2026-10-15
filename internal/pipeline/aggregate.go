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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/alerting"
	"github.com/moov-io/achgateway/internal/audittrail"
	"github.com/moov-io/achgateway/internal/events"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/notify"
	"github.com/moov-io/achgateway/internal/output"
	"github.com/moov-io/achgateway/internal/schedule"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/transform"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/stime"
	"github.com/moov-io/base/strx"
	"github.com/moov-io/base/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type aggregator struct {
	logger       log.Logger
	eventEmitter events.Emitter
	shard        service.Shard

	cutoffs       *schedule.CutoffTimes
	cutoffTrigger chan manuallyTriggeredCutoff
	merger        XferMerging

	auditStorage          audittrail.Storage
	preuploadTransformers []transform.PreUpload
	outputFormatter       output.Formatter
	alerters              alerting.Alerters

	uploadAgents service.UploadAgents

	cleanupService *CleanupService

	// batchOpMu serializes cutoff runs/recovery against admin actions that
	// persist the same batch manifest, so a concurrent handler can never
	// roll back Uploaded/Audited facts with a stale full-state write.
	batchOpMu sync.Mutex

	// startupRecovered is closed once the initial active-batch recovery has
	// finished, so callers (and tests swapping seams) can synchronize.
	startupRecovered chan struct{}
}

func newAggregator(logger log.Logger, eventEmitter events.Emitter, shard service.Shard, uploadAgents service.UploadAgents, errorAlerting service.ErrorAlerting) (*aggregator, error) {
	merger, err := NewMerging(logger, shard, uploadAgents)
	if err != nil {
		return nil, fmt.Errorf("error creating xfer merger: %v", err)
	}

	auditStorage, err := audittrail.NewStorage(shard.Audit)
	if err != nil {
		return nil, err
	}
	logger.Info().With(log.Fields{
		"shard": log.String(shard.Name),
	}).Logf("setup %T audit storage", auditStorage)

	preuploadTransformers, err := transform.Multi(logger, shard.PreUpload)
	if err != nil {
		return nil, err
	}
	logger.Info().With(log.Fields{
		"shard": log.String(shard.Name),
	}).Logf("setup %#v pre-upload transformers", preuploadTransformers)

	outputFormatter, err := output.NewFormatter(shard.Output)
	if err != nil {
		return nil, err
	}
	logger.Info().With(log.Fields{
		"shard": log.String(shard.Name),
	}).Logf("setup %T output formatter", outputFormatter)

	timeService := stime.NewSystemTimeService()
	cutoffs, err := schedule.ForCutoffTimes(logger, timeService, shard.Cutoffs.Timezone, shard.Cutoffs.Windows)
	if err != nil {
		return nil, fmt.Errorf("error creating cutoffs: %v", err)
	}

	alerters, err := alerting.NewAlerters(errorAlerting)
	if err != nil {
		return nil, fmt.Errorf("error setting up alerters: %v", err)
	}

	// Create cleanup service if configured
	var cleanupService *CleanupService
	if uploadAgents.Merging.Cleanup != nil && uploadAgents.Merging.Cleanup.Enabled {
		// Get the storage from merger (it's a filesystemMerging which has storage)
		if fm, ok := merger.(*filesystemMerging); ok {
			cleanupService, err = NewCleanupService(logger, fm.storage, shard, uploadAgents.Merging.Cleanup)
			if err != nil {
				return nil, fmt.Errorf("error creating cleanup service: %v", err)
			}
			logger.Info().With(log.Fields{
				"shard": log.String(shard.Name),
			}).Log("setup cleanup service")
		}
	}

	return &aggregator{
		logger:                logger,
		eventEmitter:          eventEmitter,
		shard:                 shard,
		uploadAgents:          uploadAgents,
		cutoffs:               cutoffs,
		cutoffTrigger:         make(chan manuallyTriggeredCutoff, 1),
		merger:                merger,
		auditStorage:          auditStorage,
		preuploadTransformers: preuploadTransformers,
		outputFormatter:       outputFormatter,
		alerters:              alerters,
		cleanupService:        cleanupService,
		startupRecovered:      make(chan struct{}),
	}, nil
}

func (xfagg *aggregator) Start(ctx context.Context) {
	// Start cleanup service if configured
	if xfagg.cleanupService != nil {
		xfagg.cleanupService.Start(ctx)
	}

	// Resume batches left active by a crash or a previous partial window
	// before listening for new triggers, so confirmed remote uploads are never
	// sent a second time. Serialized against concurrent admin manifest writes.
	xfagg.batchOpMu.Lock()
	xfagg.recoverActiveBatches(ctx, "startup")
	xfagg.batchOpMu.Unlock()
	close(xfagg.startupRecovered)

	for {
		select {
		// process automated cutoff time triggering
		case day := <-xfagg.cutoffs.C:
			xfagg.handleScheduledCutoff(day)

		// manually trigger cutoffs
		case waiter := <-xfagg.cutoffTrigger:
			xfagg.manualCutoff(waiter)

		case <-ctx.Done():
			xfagg.cutoffs.Stop()
			xfagg.Shutdown()
			return
		}
	}
}

// handleScheduledCutoff runs the banking-day cutoff or emits the holiday
// notification without touching batches/recovery on holidays.
func (xfagg *aggregator) handleScheduledCutoff(day *schedule.Day) {
	if day.IsBankingDay || xfagg.shard.Cutoffs.On == "all-days" {
		if err := xfagg.withEachFile(day.Time); err != nil {
			// Log separately so the alert still receives the original wrapped
			// error (the logger's Err() flattens the error chain).
			xfagg.logger.Error().LogErrorf("merging files: %v", err)
			xfagg.alertOnError(err)
		}
	} else if day.IsHoliday && !day.IsWeekend {
		xfagg.notifyAboutHoliday(day)
	}
}

// recoverActiveBatches resumes active batches through the merger. Recovery
// failures are logged and alerted but never block the caller: a new window
// still proceeds while the old batch stays active for the next pass.
func (xfagg *aggregator) recoverActiveBatches(ctx context.Context, reason string) {
	logger := xfagg.logger.With(log.Fields{
		"shard":  log.String(xfagg.shard.Name),
		"reason": log.String(reason),
	})
	if err := xfagg.merger.RecoverActiveBatches(ctx, reason, xfagg); err != nil {
		err = fmt.Errorf("recovering active cutoff batches (%s): %w", reason, err)
		logger.Error().LogError(err)
		xfagg.alertOnError(err)
	}
}

func (xfagg *aggregator) Shutdown() {
	xfagg.logger.Info().With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	}).Log("shutting down xfer aggregation")

	if xfagg.cleanupService != nil {
		xfagg.cleanupService.Stop()
	}

	if xfagg.auditStorage != nil {
		xfagg.auditStorage.Close()
	}
}

func (xfagg *aggregator) acceptFile(ctx context.Context, msg incoming.ACHFile) (incoming.QueueACHFileResponse, error) {
	err := xfagg.merger.HandleXfer(ctx, msg)

	now := time.Now().In(time.UTC)
	resp := incoming.QueueACHFileResponse{
		FileID:     msg.FileID,
		ShardKey:   msg.ShardKey,
		AcceptedAt: &now,
	}
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	nextCutoff, cutoffErr := schedule.NextCutoff(xfagg.shard.Cutoffs.Timezone, xfagg.shard.Cutoffs.Windows, xfagg.shard.Cutoffs.On, now)
	if cutoffErr != nil {
		xfagg.logger.Warn().With(log.Fields{
			"fileID":   log.String(msg.FileID),
			"shard":    log.String(xfagg.shard.Name),
			"shardKey": log.String(msg.ShardKey),
		}).Logf("unable to predict next cutoff: %v", cutoffErr)
	} else if !nextCutoff.IsZero() {
		resp.NextCutoff = &nextCutoff
	}

	return resp, nil
}

func (xfagg *aggregator) cancelFile(ctx context.Context, msg incoming.CancelACHFile) (models.FileCancellationResponse, error) {
	response, err := xfagg.merger.HandleCancel(ctx, msg)
	if err != nil {
		return models.FileCancellationResponse{}, err
	}

	// Send the response back
	out := models.FileCancellationResponse(response)
	err = xfagg.eventEmitter.Send(ctx, models.Event{
		Event: out,
	})
	if err != nil {
		return out, fmt.Errorf("problem emitting file cancellation response: %w", err)
	}
	return out, nil
}

func (xfagg *aggregator) withEachFile(when time.Time) error {
	// Hold the batch lock for the full recovery+window run so admin backfill
	// cannot persist a stale manifest over freshly confirmed stage facts.
	xfagg.batchOpMu.Lock()
	defer xfagg.batchOpMu.Unlock()

	window := when.Format("15:04")
	tzname, _ := when.Zone()

	logger := xfagg.logger.With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	})
	logger.Info().Logf("starting %s %s cutoff window processing", window, tzname)

	defer logger.With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	}).Logf("ended %s %s cutoff window processing", window, tzname)

	ctx, span := telemetry.StartSpan(context.Background(), "automated-cutoff", trace.WithAttributes(
		attribute.String("achgateway.shard", xfagg.shard.Name),
		attribute.String("achgateway.timezone", tzname),
		attribute.String("achgateway.window", window),
	))
	defer span.End()

	var mergeErr error

	// Finish anything left open by a previous window/crash before opening this
	// window's batch. Recovery failure must not block the new window.
	if err := xfagg.merger.RecoverActiveBatches(ctx, "cutoff", xfagg); err != nil {
		recoverErr := fmt.Errorf("recovering active batches before cutoff: %w", err)
		logger.Error().LogError(recoverErr)
		span.RecordError(recoverErr)
		mergeErr = errors.Join(mergeErr, recoverErr)
	}

	// WithEachMerged drives prepare→upload→events per output and returns partial
	// results with an error (some files uploaded, others pending recovery).
	_, err := xfagg.merger.WithEachMerged(ctx, cutoffTrigger{
		kind:   BatchKindAutomated,
		window: when,
	}, xfagg)
	if err != nil {
		mergeErr = errors.Join(mergeErr, fmt.Errorf("WithEachMerged: %w", err))
		logger.Error().LogError(mergeErr)
		span.RecordError(mergeErr)
	}

	if mergeErr != nil {
		return fmt.Errorf("merging ACH files: %w", mergeErr)
	}
	return nil
}

func (xfagg *aggregator) manualCutoff(waiter manuallyTriggeredCutoff) {
	xfagg.batchOpMu.Lock()
	defer xfagg.batchOpMu.Unlock()

	logger := xfagg.logger.With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	})
	logger.Info().Log("starting manual cutoff window processing")

	ctx, span := telemetry.StartSpan(context.Background(), "manual-cutoff", trace.WithAttributes(
		attribute.String("achgateway.shard", xfagg.shard.Name),
	))
	defer span.End()

	// Finish active batches first (a manual trigger is the operator's "retry"),
	// then open or resume the current manual batch.
	var mergeErr error
	if err := xfagg.merger.RecoverActiveBatches(ctx, "manual", xfagg); err != nil {
		recoverErr := fmt.Errorf("recovering active batches before manual cutoff: %w", err)
		logger.Error().LogError(recoverErr)
		span.RecordError(recoverErr)
		mergeErr = errors.Join(mergeErr, recoverErr)
	}

	now := time.Now()
	if loc, err := time.LoadLocation(xfagg.shard.Cutoffs.Timezone); err == nil && loc != nil {
		now = now.In(loc)
	}
	_, err := xfagg.merger.WithEachMerged(ctx, cutoffTrigger{
		kind:   BatchKindManual,
		window: now,
	}, xfagg)
	if err != nil {
		mergeErr = errors.Join(mergeErr, fmt.Errorf("manual WithEachMerged: %w", err))
		logger.Error().LogError(mergeErr)
		span.RecordError(mergeErr)
	}
	waiter.C <- mergeErr

	logger.Info().With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	}).Log("ended manual cutoff window processing")
}

// fileUploadedEmitLimit caps concurrent FileUploaded produces.
//
// This limit is about *our* side: avoid spawning one goroutine per input
// (10k–100k on large cutoffs) when the event producer is already the bottleneck.
// 128 is enough to keep the producer fed without a goroutine stampede.
const fileUploadedEmitLimit = 128

// fileUploadedSpec is one pending FileUploaded emission.
type fileUploadedSpec struct {
	fileID     string
	shardKey   string
	filename   string
	batchID    string
	uploadedAt time.Time
}

// produceFileUploaded sends FileUploaded events with bounded concurrency.
// One produce failure never blocks the remaining events; onSent is invoked
// (under a mutex) with the spec index after each successful send so callers can
// persist exactly which inputs were confirmed.
func (xfagg *aggregator) produceFileUploaded(ctx context.Context, specs []fileUploadedSpec, onSent func(index int)) error {
	ctx, span := telemetry.StartSpan(ctx, "emit-file-uploaded", trace.WithAttributes(
		attribute.String("achgateway.shard", xfagg.shard.Name),
	))
	defer span.End()

	uploadedFilenames := make([]string, 0, len(specs))
	seenNames := make(map[string]struct{}, len(specs))
	for i := range specs {
		if _, ok := seenNames[specs[i].filename]; !ok {
			seenNames[specs[i].filename] = struct{}{}
			uploadedFilenames = append(uploadedFilenames, specs[i].filename)
		}
	}

	// Attempt every FileUploaded independently. One produce failure must not
	// prevent the remaining events from being sent. errgroup.Wait only returns
	// the first error, so we also collect every failure in sendErrs for ops.
	var (
		g        errgroup.Group
		mu       sync.Mutex
		sendErrs []error
	)
	g.SetLimit(fileUploadedEmitLimit)

	missingEmitter := xfagg.eventEmitter == nil
	if missingEmitter {
		err := fmt.Errorf("missing event emitter for FileUploaded on shard %v", xfagg.shard.Name)
		xfagg.logger.Error().LogError(err)
		span.RecordError(err)
	}

	for i := range specs {
		i := i
		if missingEmitter {
			continue
		}
		spec := specs[i]

		g.Go(func() error {
			evt := models.Event{
				Event: models.FileUploaded{
					FileID:     spec.fileID,
					ShardKey:   spec.shardKey,
					Filename:   spec.filename,
					UploadedAt: spec.uploadedAt,
					BatchID:    spec.batchID,
				},
			}

			if err := xfagg.eventEmitter.Send(ctx, evt); err != nil {
				err = fmt.Errorf("FileUploaded file_id=%s filename=%s batch=%s: %w", spec.fileID, spec.filename, spec.batchID, err)
				mu.Lock()
				sendErrs = append(sendErrs, err)
				mu.Unlock()
				return err
			}
			if onSent != nil {
				mu.Lock()
				onSent(i)
				mu.Unlock()
			}
			return nil
		})
	}

	// Block until every produce finishes. Discard Wait's first-error return —
	// sendErrs already holds the full multi-error set for the caller.
	_ = g.Wait() //nolint:errcheck // intentional: all per-goroutine errors are collected in sendErrs; Wait's first-error return would drop the rest

	span.SetAttributes(
		attribute.Int("achgateway.input_filepaths", len(specs)),
		attribute.Int("achgateway.file_uploaded_errors", len(sendErrs)),
		attribute.Int("achgateway.file_uploaded_emit_limit", fileUploadedEmitLimit),
		attribute.StringSlice("achgateway.uploaded_filename", uploadedFilenames),
	)

	if missingEmitter {
		return fmt.Errorf("missing event emitter for FileUploaded on shard %v (%d events skipped)", xfagg.shard.Name, len(specs))
	}
	if len(sendErrs) == 0 {
		return nil
	}
	// Prefer a multi-error so operators see how many emits failed, not only the first.
	return errors.Join(sendErrs...)
}

// emitFilesUploaded sends FileUploaded for a merged result without a batch
// manifest. Used by the manual event-backfill admin endpoint. Batch-driven
// windows go through SendEvents so confirmations persist per input.
func (xfagg *aggregator) emitFilesUploaded(ctx context.Context, merged mergedFiles) error {
	specs := make([]fileUploadedSpec, 0, len(merged))
	for i := range merged {
		for k := range merged[i].InputFilepaths {
			// FileID's don't have .ach suffix
			_, filename := filepath.Split(merged[i].InputFilepaths[k])
			filename = trimACHExtension(filename)

			uploadedAt := merged[i].UploadedAt
			if uploadedAt.IsZero() {
				uploadedAt = time.Now()
			}
			specs = append(specs, fileUploadedSpec{
				fileID:     filename,
				shardKey:   merged[i].Shard,
				filename:   merged[i].UploadedFilename,
				batchID:    merged[i].BatchID,
				uploadedAt: uploadedAt,
			})
		}
	}
	return xfagg.produceFileUploaded(ctx, specs, nil)
}

// SendEvents implements OutputHandler: it sends FileUploaded for every snapshot
// input mapped to a confirmed-uploaded output whose send is not yet recorded,
// then flags only the successful inputs so a retry re-sends just the failures.
func (xfagg *aggregator) SendEvents(ctx context.Context, b *Batch) error {
	state := b.State()

	specs := make([]fileUploadedSpec, 0, len(state.Inputs))
	inputIdx := make([]int, 0, len(state.Inputs))
	for i := range state.Outputs {
		out := state.Outputs[i]
		if !out.Uploaded {
			continue
		}
		for _, inIdx := range out.Inputs {
			if inIdx < 0 || inIdx >= len(state.Inputs) {
				continue
			}
			in := state.Inputs[inIdx]
			if in.EventSent {
				continue
			}
			specs = append(specs, fileUploadedSpec{
				fileID:     trimACHExtension(in.Filename),
				shardKey:   state.Shard,
				filename:   out.Filename,
				batchID:    state.ID,
				uploadedAt: out.UploadedAt,
			})
			inputIdx = append(inputIdx, inIdx)
		}
	}
	if len(specs) == 0 {
		return nil
	}

	var (
		mu        sync.Mutex
		confirmed []int
	)
	err := xfagg.produceFileUploaded(ctx, specs, func(i int) {
		confirmed = append(confirmed, inputIdx[i])
	})
	mu.Lock()
	sent := append([]int(nil), confirmed...)
	mu.Unlock()
	b.MarkEventsSent(sent)
	return err
}

// Prepare implements OutputHandler: run pre-upload transforms, render the
// remote filename exactly once, format the uploadable bytes, and persist them
// (plus fixed remote/audit paths) as a batch artifact. Re-running Prepare
// after a crash is a no-op because the facts are already in the manifest.
func (xfagg *aggregator) Prepare(ctx context.Context, b *Batch, index int, agent upload.Agent, outgoing *ach.File) error {
	state := b.State()
	if index < 0 || index >= len(state.Outputs) {
		return fmt.Errorf("prepare: unknown output index %d", index)
	}
	if state.Outputs[index].ArtifactPath != "" {
		return nil
	}
	if outgoing == nil {
		return fmt.Errorf("prepare output %d: missing merged ACH file", index)
	}

	result, err := transform.ForUpload(outgoing, xfagg.preuploadTransformers)
	if err != nil {
		recordFileUploadError(ctx, xfagg.shard.Name, err)
		return err
	}
	if result == nil || result.File == nil {
		return errors.New("prepare: nil transform Result / File")
	}

	data := upload.FilenameData{
		RoutingNumber: result.File.Header.ImmediateDestination,
		GPG:           len(result.Encrypted) > 0,
		ShardName:     prepareShardName(xfagg.shard.Name),
		Index:         index,
	}
	filename, err := upload.RenderACHFilename(xfagg.shard.FilenameTemplate(), data)
	if err != nil {
		recordFileUploadError(ctx, xfagg.shard.Name, err)
		return fmt.Errorf("problem rendering filename template: %w", err)
	}

	ctx, span := telemetry.StartSpan(ctx, "prepare-upload", trace.WithAttributes(
		attribute.String("achgateway.filename", filename),
		attribute.String("achgateway.shard", xfagg.shard.Name),
		attribute.String("achgateway.batch_id", state.ID),
	))
	defer span.End()

	var buf bytes.Buffer
	if err := xfagg.outputFormatter.Format(&buf, result); err != nil {
		recordFileUploadError(ctx, xfagg.shard.Name, err)
		return fmt.Errorf("problem formatting output: %w", err)
	}

	sum := sha256.Sum256(buf.Bytes())
	artifactRel, err := b.WriteArtifact(fmt.Sprintf("%d-%s", index, filepath.Base(filename)), buf.Bytes())
	if err != nil {
		return err
	}

	// Fix the audit object path on first prepare so audit backfill after a
	// date boundary re-saves the same object instead of duplicating it.
	basePath := "outbound"
	if xfagg.shard.Audit != nil {
		basePath = strx.Or(xfagg.shard.Audit.BasePath, basePath)
	}
	auditPath := fmt.Sprintf("%s/%s/%s/%s", basePath, agent.Hostname(), time.Now().Format("2006-01-02"), filename)
	remotePath := filepath.Join(agent.OutboundPath(), filename)

	out := state.Outputs[index]
	b.MarkPrepared(index, BatchOutput{
		Index:        index,
		MergedPath:   out.MergedPath,
		MergedSHA256: out.MergedSHA256,
		ArtifactPath: artifactRel,
		SHA256:       hex.EncodeToString(sum[:]),
		Filename:     filename,
		RemotePath:   remotePath,
		AuditPath:    auditPath,
		Inputs:       out.Inputs,
	})

	span.SetAttributes(
		attribute.String("achgateway.remote_path", remotePath),
		attribute.String("achgateway.audit_path", auditPath),
		attribute.Int("achgateway.artifact_bytes", buf.Len()),
	)
	return nil
}

// Upload implements OutputHandler: confirm the prepared artifact at the remote
// (pre-checking a resumed batch, and reconciling ambiguous responses by path
// and digest), then record the audit copy. The confirmed-upload fact is
// persisted before any further work, so a crash after the ACK cannot turn into
// a second upload. Audit failures keep the upload fact.
func (xfagg *aggregator) Upload(ctx context.Context, b *Batch, index int, agent upload.Agent, resumed bool) error {
	state := b.State()
	if index < 0 || index >= len(state.Outputs) {
		return fmt.Errorf("upload: unknown output index %d", index)
	}
	out := state.Outputs[index]
	if out.Uploaded && out.Audited {
		return nil
	}
	if out.ArtifactPath == "" {
		return fmt.Errorf("upload output %d: artifact not prepared", index)
	}

	payload, err := b.ReadArtifact(out.ArtifactPath)
	if err != nil {
		return err
	}

	ctx, span := telemetry.StartSpan(ctx, "upload-file", trace.WithAttributes(
		attribute.String("achgateway.filename", out.Filename),
		attribute.String("achgateway.shard", xfagg.shard.Name),
		attribute.String("achgateway.batch_id", state.ID),
		attribute.String("achgateway.remote_path", out.RemotePath),
	))
	defer span.End()

	newlyConfirmed := false

	if !out.Uploaded {
		// Resumed batch: the previous process may have been killed after the
		// server accepted the bytes but before the manifest flushed. Verify
		// before writing so the restart can never re-upload a confirmed file.
		if resumed {
			presence, verifyErr := upload.VerifyRemoteFile(ctx, agent, out.RemotePath, out.SHA256)
			switch {
			case verifyErr != nil:
				return fmt.Errorf("pre-flight verification of %s inconclusive, staying pending: %w", out.RemotePath, verifyErr)
			case presence == upload.RemoteMatches:
				b.MarkUploaded(index, true, time.Now().UTC())
				newlyConfirmed = true
				recordRemoteVerifiedUpload(xfagg.shard.Name)
				span.SetAttributes(attribute.Bool("achgateway.remote_verified", true))
			case presence == upload.RemoteMismatch:
				b.MarkConflict(index)
				conflictErr := fmt.Errorf("%w: shard=%s output=%d remote=%s", upload.ErrRemoteContentConflict, xfagg.shard.Name, index, out.RemotePath)
				span.RecordError(conflictErr)
				recordRemoteContentConflict(xfagg.shard.Name)
				return conflictErr
			}
			if err := b.Persist(ctx); err != nil {
				return err
			}
		}

		if !b.State().Outputs[index].Uploaded {
			uploadErr := agent.UploadFile(ctx, upload.File{
				Filepath: out.Filename,
				Contents: io.NopCloser(bytes.NewReader(payload)),
			})
			if uploadErr != nil {
				// The response is ambiguous: the server may already hold the
				// file. Reconcile against the target path and digest before retry.
				xfagg.logger.Warn().With(log.Fields{
					"shard":    log.String(xfagg.shard.Name),
					"filename": log.String(out.Filename),
				}).LogErrorf("upload returned an error; verifying remote before retrying: %v", uploadErr)

				presence, verifyErr := upload.VerifyRemoteFile(ctx, agent, out.RemotePath, out.SHA256)
				if verifyErr != nil {
					recordFileUploadError(ctx, xfagg.shard.Name, uploadErr)
					return errors.Join(
						fmt.Errorf("uploading %s failed and remote state is unconfirmed: %w", out.Filename, uploadErr),
						fmt.Errorf("verifying remote %s: %w", out.RemotePath, verifyErr),
					)
				}

				switch presence {
				case upload.RemoteMatches:
					b.MarkUploaded(index, true, time.Now().UTC())
					newlyConfirmed = true
					recordRemoteVerifiedUpload(xfagg.shard.Name)
					span.SetAttributes(attribute.Bool("achgateway.remote_verified", true))
					xfagg.logger.Info().With(log.Fields{
						"shard":    log.String(xfagg.shard.Name),
						"filename": log.String(out.Filename),
					}).Log("remote already holds identical bytes after ambiguous upload; treating as uploaded")

				case upload.RemoteMismatch:
					b.MarkConflict(index)
					conflictErr := fmt.Errorf("%w: shard=%s output=%d remote=%s", upload.ErrRemoteContentConflict, xfagg.shard.Name, index, out.RemotePath)
					span.RecordError(conflictErr)
					recordRemoteContentConflict(xfagg.shard.Name)
					return conflictErr

				default: // RemoteMissing
					recordFileUploadError(ctx, xfagg.shard.Name, uploadErr)
					if nErr := xfagg.notifyAfterUpload(ctx, out.Filename, xfagg.mergedFileForNotify(b, index), agent, uploadErr); nErr != nil {
						xfagg.alertOnError(xfagg.logger.Error().LogError(nErr).Err())
					}
					return fmt.Errorf("uploading %s: %w", out.Filename, uploadErr)
				}
			} else {
				// Clean acknowledgement.
				b.MarkUploaded(index, false, time.Now().UTC())
				newlyConfirmed = true
			}

			// Flush the confirmed fact immediately: audit/notification failures
			// or a kill must not roll the manifest back to "not uploaded".
			if err := b.Persist(ctx); err != nil {
				return err
			}
		}
	}

	// Audit only after the upload outcome is confirmed. Failure here keeps the
	// uploaded fact; recovery replays SaveFile against the fixed AuditPath only.
	out = b.State().Outputs[index]
	if !out.Audited {
		if err := xfagg.auditStorage.SaveFile(ctx, out.AuditPath, payload); err != nil {
			recordFileUploadError(ctx, xfagg.shard.Name, err)
			return fmt.Errorf("problem saving file in audit record: %w", err)
		}
		b.MarkAudited(index)
	}

	if newlyConfirmed {
		uploadedFilesCounter.With("shard", xfagg.shard.Name).Add(1)
		if nErr := xfagg.notifyAfterUpload(ctx, out.Filename, xfagg.mergedFileForNotify(b, index), agent, nil); nErr != nil {
			xfagg.alertOnError(xfagg.logger.Error().LogError(nErr).Err())
		}
	}
	return nil
}

// confirmBackfilledEvents records FileUploaded sends performed by the admin
// event-backfill endpoint. It runs under batchOpMu and merges only the event
// flags into a freshly loaded manifest, so it cannot roll back upload/audit
// facts the cutoff goroutine records concurrently.
func (xfagg *aggregator) confirmBackfilledEvents(ctx context.Context, dir string, names []string) error {
	xfagg.batchOpMu.Lock()
	defer xfagg.batchOpMu.Unlock()

	fm, ok := xfagg.merger.(*filesystemMerging)
	if !ok {
		return nil
	}
	b, err := loadBatch(ctx, fm.storage, dir)
	if err != nil || b == nil {
		return err
	}

	confirmed := make(map[string]struct{}, len(names))
	for _, name := range names {
		confirmed[name] = struct{}{}
	}
	st := b.State()
	var sent []int
	for i := range st.Inputs {
		if _, ok := confirmed[st.Inputs[i].Filename]; ok {
			sent = append(sent, i)
		}
	}
	b.MarkEventsSent(sent)
	if b.IsComplete() {
		if err := b.MarkCompleted(); err != nil {
			return err
		}
	}
	return b.Persist(ctx)
}

// batchIdentity returns the BatchID and first uploaded output's UploadedAt
// for an isolated directory, or empty values when the directory has no batch.
func (xfagg *aggregator) batchIdentity(ctx context.Context, dir string) (string, time.Time) {
	fm, ok := xfagg.merger.(*filesystemMerging)
	if !ok {
		return "", time.Time{}
	}
	b, err := loadBatch(ctx, fm.storage, dir)
	if err != nil || b == nil {
		return "", time.Time{}
	}
	st := b.State()
	for i := range st.Outputs {
		if st.Outputs[i].Uploaded && !st.Outputs[i].UploadedAt.IsZero() {
			return st.ID, st.Outputs[i].UploadedAt
		}
	}
	return st.ID, time.Now().UTC()
}

// mergedFileForNotify loads the merged Nacha file for notification templates;
// notifications are best effort and a missing cache must not block the upload.
func (xfagg *aggregator) mergedFileForNotify(b *Batch, index int) *ach.File {
	fm, ok := xfagg.merger.(*filesystemMerging)
	if !ok {
		return nil
	}
	state := b.State()
	if index < 0 || index >= len(state.Outputs) {
		return nil
	}
	file, err := fm.readMergedFile(state.Outputs[index])
	if err != nil {
		xfagg.logger.Warn().Logf("notification: unable to load merged file for output %d: %v", index, err)
		return nil
	}
	return file
}

func prepareShardName(shardName string) string {
	return strings.ToUpper(strings.ReplaceAll(shardName, " ", "-"))
}

func (xfagg *aggregator) notifyAfterUpload(ctx context.Context, filename string, file *ach.File, agent upload.Agent, uploadErr error) error {
	_, span := telemetry.StartSpan(ctx, "notify-after-upload", trace.WithAttributes(
		attribute.String("achgateway.filename", filename),
		attribute.String("achgateway.shard", xfagg.shard.Name),
	))
	defer span.End()

	msg := &notify.Message{
		Direction: notify.Upload,
		Filename:  filename,
		File:      file,
		Hostname:  agent.Hostname(),
	}

	uploadAgent := xfagg.uploadAgents.Find(agent.ID())

	if uploadAgent == nil {
		return fmt.Errorf("no uploadAgent found for id=%s", agent.ID())
	}

	logger := xfagg.logger.With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	})

	notifier, err := notify.NewMultiSender(logger, xfagg.shard.Notifications, uploadAgent.Notifications)
	if err != nil {
		return fmt.Errorf("notify: unable to create multi-sender: %v", err)
	}

	if uploadErr != nil {
		if err := notifier.Critical(ctx, msg); err != nil {
			return fmt.Errorf("problem sending critical notification for file=%s: %v", filename, err)
		}
	} else {
		if err := notifier.Info(ctx, msg); err != nil {
			return fmt.Errorf("problem sending info notification for file=%s: %v", filename, err)
		}
	}

	return nil
}

func (xfagg *aggregator) notifyAboutHoliday(day *schedule.Day) {
	logger := xfagg.logger.With(log.Fields{
		"shard": log.String(xfagg.shard.Name),
	})

	if !day.FirstWindow {
		logger.Info().Log("skipping holiday notification")
		return
	}

	uploadAgent := xfagg.uploadAgents.Find(xfagg.shard.UploadAgent)
	if uploadAgent == nil {
		logger.Warn().Logf("skipping holiday log for %v", day.Time.Format("2006-01-02"))
		return
	}

	ctx, span := telemetry.StartSpan(context.Background(), "notify-about-holiday", trace.WithAttributes(
		attribute.String("achgateway.holiday", day.Holiday.Name),
	))
	defer span.End()

	if uploadAgent.Notifications != nil {
		slackConfigs := xfagg.shard.Notifications.FindSlacks(uploadAgent.Notifications.Slack)
		for i := range slackConfigs {
			ss, err := notify.NewSlack(&slackConfigs[i])
			if err != nil {
				logger.Error().LogErrorf("ERROR creating slack holiday notifier: %v", err)
				continue
			}

			err = ss.Info(ctx, &notify.Message{
				Contents: formatHolidayMessage(day, xfagg.shard.Name),
			})
			if err != nil {
				logger.Error().LogErrorf("ERROR sending holiday notification: %v", err)
			} else {
				logger.Info().Log("sent holiday notification")
			}
		}
	}
}

func formatHolidayMessage(day *schedule.Day, shardName string) string {
	name := "is a holiday"
	if day != nil && day.Holiday != nil {
		name = fmt.Sprintf("(%s) is a holiday", day.Holiday.Name)
	}

	hostname, _ := os.Hostname()

	if shardName != "" {
		shardName = "for " + shardName
	}

	return strings.TrimSpace(fmt.Sprintf("%s %s so %s will skip processing %s", day.Time.Format("Jan 02"), name, hostname, shardName))
}

func (xfagg *aggregator) alertOnError(err error) {
	if xfagg == nil {
		return
	}
	if err == nil {
		return
	}

	if err := xfagg.alerters.AlertError(err); err != nil {
		xfagg.logger.Error().LogErrorf("ERROR sending alert: %v", err)
	}
}
