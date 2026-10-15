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
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/base/log"
)

// installPublishedGate gives each durable merger the published file index so
// cutoff merges only queryable/cancellable files.
func (fr *FileReceiver) installPublishedGate() {
	for _, agg := range fr.shardAggregators {
		m, ok := agg.merger.(*filesystemMerging)
		if !ok {
			continue
		}
		m.publishedFileIDs = func(ctx context.Context) (map[string]struct{}, error) {
			return fr.fileRepository.ListPublishedFileIDs(ctx)
		}
	}
}

// recoverInFlight runs once at startup before messages are consumed. It
// classifies every non-terminal reception against staging and mergable disk
// state and either resumes it to published or fails it with a frozen error
// result. Pre-upgrade mergable files are backfilled to published.
func (fr *FileReceiver) recoverInFlight(ctx context.Context) {
	for shardName, agg := range fr.shardAggregators {
		if err := fr.reconcileShard(ctx, agg); err != nil {
			fr.logger.LogErrorf("startup recovery failed for shard %s: %v", shardName, err)
		}
	}
}

func (fr *FileReceiver) reconcileShard(ctx context.Context, agg *aggregator) error {
	m, ok := agg.merger.(*filesystemMerging)
	if !ok {
		// Mock mergers (tests) hold no durable state.
		return nil
	}

	unfinished, err := fr.fileRepository.ListUnfinishedReceptions(ctx)
	if err != nil {
		return fmt.Errorf("listing unfinished receptions: %w", err)
	}
	staged, err := m.ListStaged(ctx)
	if err != nil {
		return fmt.Errorf("listing staged commits: %w", err)
	}
	stagedByCommit := make(map[string]StagedCommit, len(staged))
	for _, commit := range staged {
		stagedByCommit[commit.CommitID] = commit
	}

	logger := fr.logger.With(log.Fields{"shardName": log.String(agg.shard.Name)})

	for i := range unfinished {
		reception := unfinished[i]
		if reception.ShardName != agg.shard.Name {
			continue
		}
		if err := fr.reconcileReception(ctx, logger, agg, m, reception, stagedByCommit); err != nil {
			logger.LogErrorf("recovering commit %s: %v", reception.CommitID, err)
		}
	}

	return fr.backfillLegacyMergable(ctx, logger, agg, m, unfinished)
}

// mergableCandidates returns the on-disk paths a published file may occupy:
// the normal .ach or the pre-arrival-cancel form .ach.canceled. ValidateOpts
// sidecars (.json) never identify the file.
func (m *filesystemMerging) mergableCandidates(reception files.Reception) []string {
	achPath, _ := m.mergablePaths(incoming.ACHFile{FileID: reception.FileID, ShardKey: reception.ShardKey})
	return []string{achPath, achPath + ".canceled"}
}

// reconcileReception drives one unfinished reception to its terminal state.
func (fr *FileReceiver) reconcileReception(
	ctx context.Context,
	logger log.Logger,
	agg *aggregator,
	m *filesystemMerging,
	reception files.Reception,
	stagedByCommit map[string]StagedCommit,
) error {
	candidates := m.mergableCandidates(reception)

	switch reception.Stage {
	case files.StagePublished:
		// Result notification is pending; verify the published content really
		// exists (as .ach or, after a pre-arrival cancel, .ach.canceled).
		// Staged content surviving means the publish marker landed without
		// the move, which the resume path finishes.
		if fr.publishedContentPresent(ctx, m, reception.ContentSHA256, candidates) {
			return nil
		}
		if _, ok := stagedByCommit[reception.CommitID]; ok {
			return fr.resumeToPublished(ctx, agg, m, reception, stagedByCommit)
		}
		// A canceled file's normal end of life is exclusion at cutoff: the
		// isolated directory (and its .canceled marker) is removed once the
		// file has been deliberately suppressed. That is success, not loss —
		// leave the reception published so its (success) result still notifies.
		if reception.CancelRequestedAt != nil {
			logger.Info().With(log.Fields{
				"commitID": log.String(reception.CommitID),
			}).Log("canceled published file consumed by cutoff; reception left published")
			return nil
		}
		return fr.failReception(ctx, agg, reception, "published file missing from mergable and staging")

	case files.StageStaged, files.StageIndexed:
		return fr.resumeToPublished(ctx, agg, m, reception, stagedByCommit)

	case files.StageReceived:
		// Nothing durable was written; either a redelivery finishes the commit
		// or a cancel-only row waits for its file. No recovery action.
		logger.Debug().With(log.Fields{
			"commitID": log.String(reception.CommitID),
		}).Log("received-only commit left for message redelivery")
		return nil

	default:
		return nil
	}
}

// resumeToPublished completes a staged/indexed commit: ensure the index row,
// honor a cancel intent, move staged content when needed, then mark both
// ledgers published with a rebuilt frozen result.
func (fr *FileReceiver) resumeToPublished(
	ctx context.Context,
	agg *aggregator,
	m *filesystemMerging,
	reception files.Reception,
	stagedByCommit map[string]StagedCommit,
) error {
	// The move already happened but the marker missed: trust matching content
	// at either the normal or the pre-arrival-cancel target.
	if !fr.publishedContentPresent(ctx, m, reception.ContentSHA256, m.mergableCandidates(reception)) {
		stagedCommit, ok := stagedByCommit[reception.CommitID]
		if !ok {
			return fr.failReception(ctx, agg, reception, "staged content missing before publication")
		}
		if err := fr.verifyStagedHash(ctx, m, stagedCommit.ACHPath, reception.ContentSHA256); err != nil {
			return fr.failReception(ctx, agg, reception, err.Error())
		}

		if err := fr.ensureIndexed(ctx, reception); err != nil {
			return fmt.Errorf("ensuring index row: %w", err)
		}
		if reception.CancelRequestedAt != nil {
			if err := m.RecordCancelIntent(ctx, incoming.CancelACHFile{
				CommitID: reception.CommitID,
				FileID:   reception.FileID,
				ShardKey: reception.ShardKey,
			}); err != nil {
				return fmt.Errorf("recording cancel intent: %w", err)
			}
		}

		stub := incoming.ACHFile{CommitID: reception.CommitID, FileID: reception.FileID, ShardKey: reception.ShardKey}
		if err := m.PublishXfer(ctx, stub, reception.CommitID); err != nil {
			return fmt.Errorf("publishing during recovery: %w", err)
		}
	}

	// Whether we just moved everything or only the ACH had moved before the
	// crash, make sure the ValidateOpts sidecar is in mergable too.
	stub := incoming.ACHFile{CommitID: reception.CommitID, FileID: reception.FileID, ShardKey: reception.ShardKey}
	if err := m.CompletePendingSidecar(ctx, stub, reception.CommitID); err != nil {
		return fmt.Errorf("completing sidecar during recovery: %w", err)
	}

	if err := fr.ensureIndexed(ctx, reception); err != nil {
		return fmt.Errorf("ensuring index row: %w", err)
	}

	resp := agg.buildQueueResponse(
		incoming.ACHFile{CommitID: reception.CommitID, FileID: reception.FileID, ShardKey: reception.ShardKey},
		reception.CommitID, reception.AcceptedAt, "",
	)
	resultJSON, err := freezeResult(resp)
	if err != nil {
		return fmt.Errorf("freezing recovered result: %w", err)
	}
	if err := fr.fileRepository.MarkPublished(ctx, reception.CommitID, reception.FileID, resultJSON, time.Now().In(time.UTC)); err != nil {
		return fmt.Errorf("marking recovered commit published: %w", err)
	}
	if err := m.CleanupStaging(ctx, reception.CommitID); err != nil {
		agg.logger.Warn().Logf("recovery: unable to remove staging for %s: %v", reception.CommitID, err)
	}
	receptionRecovered.With("shard", agg.shard.Name).Add(1)
	agg.logger.Info().With(log.Fields{
		"commitID": log.String(reception.CommitID),
		"fileID":   log.String(reception.FileID),
	}).Log("recovered staged reception to published")
	return nil
}

// ensureIndexed makes sure an unpublished file index row exists for the commit.
func (fr *FileReceiver) ensureIndexed(ctx context.Context, reception files.Reception) error {
	if _, err := fr.fileRepository.GetFile(ctx, reception.FileID); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	hostname, _ := os.Hostname()
	return fr.fileRepository.Record(ctx, files.AcceptedFile{
		FileID:     reception.FileID,
		ShardKey:   reception.ShardKey,
		Hostname:   hostname,
		AcceptedAt: reception.AcceptedAt,
	})
}

// failReception marks an unrecoverable commit failed and freezes its error
// result so the outbox notifies exactly once.
func (fr *FileReceiver) failReception(ctx context.Context, agg *aggregator, reception files.Reception, reason string) error {
	resp := agg.buildQueueResponse(
		incoming.ACHFile{CommitID: reception.CommitID, FileID: reception.FileID, ShardKey: reception.ShardKey},
		reception.CommitID, reception.AcceptedAt, reason,
	)
	resultJSON, err := freezeResult(resp)
	if err != nil {
		return fmt.Errorf("freezing failure result: %w", err)
	}
	at := time.Now().In(time.UTC)
	if err := fr.fileRepository.MarkFailedWithResult(ctx, reception.CommitID, reason, resultJSON, at); err != nil {
		return fmt.Errorf("marking failed reception: %w", err)
	}
	receptionRecoveryFailures.With("shard", agg.shard.Name).Add(1)
	failure := fmt.Errorf("reception %s unrecoverable: %s", reception.CommitID, reason)
	agg.alertOnError(failure)
	return nil
}

// publishedContentPresent reports whether the published file occupies one of
// the candidate paths. When expectedHash is known it must match; when it is
// empty (backfilled legacy rows) any non-empty candidate file is sufficient.
func (fr *FileReceiver) publishedContentPresent(ctx context.Context, m *filesystemMerging, expectedHash string, paths []string) bool {
	for _, path := range paths {
		got, ok := storageHashIfExists(m, path)
		if !ok {
			continue
		}
		if expectedHash == "" || got == expectedHash {
			return true
		}
	}
	return false
}

func (fr *FileReceiver) verifyStagedHash(ctx context.Context, m *filesystemMerging, stagedPath, expectedHash string) error {
	got, ok := storageHashIfExists(m, stagedPath)
	if !ok {
		return fmt.Errorf("staged file %s missing", stagedPath)
	}
	if expectedHash != "" && got != expectedHash {
		return fmt.Errorf("staged file %s hash mismatch", stagedPath)
	}
	return nil
}

func storageHashIfExists(m *filesystemMerging, path string) (string, bool) {
	fd, err := m.storage.Open(path)
	if err != nil || fd == nil {
		return "", false
	}
	defer fd.Close()
	bs, err := io.ReadAll(fd)
	if err != nil || len(bs) == 0 {
		return "", false
	}
	return hash(bs), true
}

// backfillLegacyMergable upgrades mergable files that predate the reception
// ledger: index rows without published_at (or missing rows) and no ledger row.
func (fr *FileReceiver) backfillLegacyMergable(
	ctx context.Context,
	logger log.Logger,
	agg *aggregator,
	m *filesystemMerging,
	unfinished []files.Reception,
) error {
	matches, err := m.storage.Glob(filepath.Join("mergable", agg.shard.Name, "*"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("listing mergable for backfill: %w", err)
	}

	// Every unfinished ledger row already accounts for its on-disk file.
	claimed := make(map[string]struct{}, len(unfinished))
	for _, reception := range unfinished {
		claimed[fileIDForPath(reception.FileID)] = struct{}{}
	}

	at := time.Now().In(time.UTC)
	for _, match := range matches {
		name := filepath.Base(match.RelativePath)
		if !strings.HasSuffix(name, ".ach") || strings.Contains(name, ".canceled") {
			continue
		}
		fileID := fileIDForPath(name)
		if _, isClaimed := claimed[fileID]; isClaimed {
			continue
		}

		// Index rows may key the raw submitted fileID ("<id>.ach"); accept both.
		indexed, getErr := fr.fileRepository.GetFile(ctx, fileID)
		indexKey := fileID
		if errors.Is(getErr, sql.ErrNoRows) {
			indexed, getErr = fr.fileRepository.GetFile(ctx, fileID+".ach")
			if getErr == nil {
				indexKey = fileID + ".ach"
			}
		}
		if getErr != nil && !errors.Is(getErr, sql.ErrNoRows) {
			return getErr
		}

		// A published ledger row means the new flow owns this file.
		commitID := incoming.DeriveCommitID(agg.shard.Name, fileID)
		if getErr == nil && indexed.PublishedAt != nil {
			if _, err := fr.fileRepository.GetReception(ctx, commitID); err == nil {
				continue
			}
		}

		shardKey := agg.shard.Name
		acceptedAt := at
		if getErr == nil {
			shardKey = indexed.ShardKey
			if !indexed.AcceptedAt.IsZero() {
				acceptedAt = indexed.AcceptedAt
			}
			if indexed.PublishedAt == nil {
				if err := fr.fileRepository.MarkFilePublished(ctx, indexKey, at); err != nil {
					return err
				}
			}
		} else {
			// Unparseable orphan without an index row: leave it alone but alert.
			agg.alertOnError(fmt.Errorf("unindexed mergable file %s in shard %s skipped during recovery", fileID, agg.shard.Name))
			continue
		}

		resp := agg.buildQueueResponse(
			incoming.ACHFile{CommitID: commitID, FileID: fileID, ShardKey: shardKey},
			commitID, acceptedAt, "backfilled after upgrade",
		)
		resultJSON, err := freezeResult(resp)
		if err != nil {
			return err
		}
		reception := files.Reception{
			CommitID:    commitID,
			ShardName:   agg.shard.Name,
			ShardKey:    shardKey,
			FileID:      fileID,
			Source:      files.SourceStream,
			Stage:       files.StagePublished,
			AcceptedAt:  acceptedAt,
			PublishedAt: &at,
			ResultJSON:  resultJSON,
			UpdatedAt:   at,
		}
		if err := fr.fileRepository.RecordPublishedReception(ctx, reception); err != nil {
			return err
		}
		receptionRecovered.With("shard", agg.shard.Name).Add(1)
		logger.Info().With(log.Fields{"fileID": log.String(fileID)}).Log("backfilled legacy mergable file to published")
	}
	return nil
}

// fileIDForPath strips the .ach suffix the way mergable paths encode fileIDs.
func fileIDForPath(name string) string {
	name = filepath.Base(name)
	return strings.TrimSuffix(name, ".ach")
}
