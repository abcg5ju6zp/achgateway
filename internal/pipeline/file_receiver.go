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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/moov-io/achgateway/internal/events"
	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/incoming/stream"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/shards"
	"github.com/moov-io/achgateway/pkg/compliance"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/admin"
	"github.com/moov-io/base/database"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/telemetry"

	"github.com/IBM/sarama"
	"gocloud.dev/pubsub"
)

// FileReceiver accepts an ACH file from a number of pubsub Subscriptions and
// finds the appropriate aggregator for the shardKey.
type FileReceiver struct {
	logger log.Logger
	cfg    *service.Config

	mu     sync.RWMutex
	cancel context.CancelFunc

	defaultShardName string

	eventEmitter events.Emitter

	shardRepository  shards.Repository
	shardAggregators map[string]*aggregator

	fileRepository files.Repository

	httpFiles   stream.Subscription
	streamFiles stream.Subscription

	QueueFileResponses    chan incoming.QueueACHFileResponse
	CancellationResponses chan models.FileCancellationResponse

	transformConfig *models.TransformConfig

	// ackMessage acknowledges one upstream message. It is a seam so tests can
	// observe that acknowledgement only happens once a file is published.
	ackMessage func(msg *pubsub.Message)

	// outboxCancel stops the notification outbox goroutine.
	outboxCancel context.CancelFunc
}

func newFileReceiver(
	logger log.Logger,
	cfg *service.Config,
	eventEmitter events.Emitter,
	shardRepository shards.Repository,
	shardAggregators map[string]*aggregator,
	fileRepository files.Repository,
	httpFiles stream.Subscription,
	transformConfig *models.TransformConfig,
) (*FileReceiver, error) {
	// Create FileReceiver and connect streamFiles
	fr := &FileReceiver{
		logger:                logger,
		cfg:                   cfg,
		eventEmitter:          eventEmitter,
		defaultShardName:      cfg.Sharding.Default,
		shardRepository:       shardRepository,
		shardAggregators:      shardAggregators,
		fileRepository:        fileRepository,
		httpFiles:             httpFiles,
		QueueFileResponses:    make(chan incoming.QueueACHFileResponse, 1000),
		CancellationResponses: make(chan models.FileCancellationResponse, 1000),
		transformConfig:       transformConfig,
	}
	fr.ackMessage = func(msg *pubsub.Message) { msg.Ack() }
	err := fr.reconnect()
	if err != nil {
		return nil, err
	}

	// Finish or fail receptions left behind by a crash before consuming new
	// messages. Recovered/failed results become visible to the outbox below.
	fr.installPublishedGate()
	fr.recoverInFlight(context.Background())

	// Deliver frozen results left behind by crashes or failed event sends.
	outboxCtx, outboxCancel := context.WithCancel(context.Background())
	fr.outboxCancel = outboxCancel
	go fr.runOutbox(outboxCtx, defaultOutboxInterval)

	return fr, nil
}

func (fr *FileReceiver) reconnect() error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Close any existing subscription
	if fr.streamFiles != nil {
		fr.streamFiles.Shutdown(context.Background())
	}

	streamSub, err := stream.OpenSubscription(fr.logger, fr.cfg)
	if err != nil {
		return fmt.Errorf("creating stream subscription: %v", err)
	}
	fr.streamFiles = streamSub

	return nil
}

func (fr *FileReceiver) Start(ctx context.Context) {
	for {
		// Create a context that will be shutdown by its parent or after a read iteration
		innerCtx, cancelFunc := context.WithCancel(ctx)
		fr.cancel = cancelFunc

		select {
		case err := <-fr.handleMessage(innerCtx, fr.httpFiles, files.SourceHTTP):
			incomingHTTPFiles.With().Add(1)
			if err != nil {
				httpFileProcessingErrors.With().Add(1)
				fr.logger.LogErrorf("error handling http file: %v", err)
			}

		case err := <-fr.handleMessage(innerCtx, fr.streamFiles, files.SourceStream):
			incomingStreamFiles.With().Add(1)
			if err != nil {
				streamFileProcessingErrors.With().Add(1)
				fr.logger.LogErrorf("error handling stream file: %v", err)

				// Attempt to reconnect under some conditions
				if isNetworkError(err) {
					fr.logger.Info().Log("attempt to reconnect to stream subscription")
					if err := fr.reconnect(); err != nil {
						fr.logger.LogErrorf("unable to reconnect stream subscription: %v", err)
					}
				}
			}

		case <-ctx.Done():
			cancelFunc()
			fr.Shutdown()
			return
		}

		// After processing a message cancel the inner context to release any resources.
		cancelFunc()
	}
}

func (fr *FileReceiver) Shutdown() {
	fr.logger.Log("shutting down xfer aggregation")

	if fr.outboxCancel != nil {
		fr.outboxCancel()
	}

	// Pass a context that we cancel right away to our subscriptions
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	if fr.httpFiles != nil {
		if err := fr.httpFiles.Shutdown(ctx); err != nil {
			fr.logger.LogErrorf("problem shutting down http file subscription: %v", err)
		}
	}
	if fr.streamFiles != nil {
		if err := fr.streamFiles.Shutdown(ctx); err != nil {
			fr.logger.LogErrorf("problem shutting down stream file subscription: %v", err)
		}
	}
}

func (fr *FileReceiver) RegisterAdminRoutes(r *admin.Server) {
	r.AddHandler("/trigger-cutoff", fr.triggerManualCutoff())

	r.AddHandler("/shards", fr.listShards())

	sub := r.Subrouter("/shards/{shardName}")
	sub.HandleFunc("/files", fr.listShardFiles())
	sub.PathPrefix("/files/{filepath}").Handler(fr.getShardFile())
	sub.HandleFunc("/pipeline/{isolatedDirectory}/file-uploaded", fr.manuallyProduceFileUploaded())
}

type processableMessage struct {
	ctx context.Context
	msg *pubsub.Message
}

// ErrRetriable marks reception failures that leave the durable reception fact
// (and usually the staged content) intact. The message is left unacknowledged
// for redelivery and — unlike parse/validation errors — no InvalidQueueFile
// event is produced because nothing is wrong with the submitted file.
var ErrRetriable = errors.New("retriable reception failure")

func retriablef(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrRetriable, fmt.Sprintf(format, args...))
}

// handleMessage will listen for an incoming.ACHFile to pass off to an aggregator for the shard
// responsible. It does so with a database lookup and the fixed set of Shards from the file config.
func (fr *FileReceiver) handleMessage(ctx context.Context, sub stream.Subscription, source string) chan error {
	fr.mu.RLock()
	defer fr.mu.RUnlock()

	out := make(chan error, 1)
	if sub == nil {
		return out
	}
	cleanup := func() {
		out <- nil
	}
	go func() {
		receiver := make(chan *processableMessage)
		go func() {
			msg, err := sub.Receive(ctx)

			traceCtx, span := telemetry.StartSpan(context.Background(), "file-receiver-handle-message")
			defer span.End()

			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if strings.Contains(err.Error(), "Subscription has been Shutdown") {
					return
				}

				// Include the error in the span since it's interesting
				telemetry.RecordError(traceCtx, err)

				// Bubble up some errors to alerting
				if isNetworkError(err) {
					out <- err
					return
				}
				fr.logger.LogErrorf("ERROR receiving message: %v", err)
			}
			receiver <- &processableMessage{
				ctx: traceCtx,
				msg: msg,
			}
		}()

		select {
		case m := <-receiver:
			if m != nil && m.msg != nil {
				out <- fr.processMessage(m.ctx, m.msg, source)
				return
			} else {
				cleanup()
				return
			}

		case <-ctx.Done():
			cleanup()
			return
		}
	}()
	return out
}

func isNetworkError(err error) bool {
	return contains(err, "connect: ", "write:", "broken pipe", "pubsub", "EOF")
}

func contains(err error, options ...string) bool {
	es := err.Error()
	for i := range options {
		if strings.Contains(es, options[i]) {
			return true
		}
	}
	return false
}

func (fr *FileReceiver) processMessage(ctx context.Context, msg *pubsub.Message, source string) error {
	// AutoCommit is a setting which will acknowledge messages with the pubsub service
	// immediately after receiving it. With this disabled messages are committed after
	// successful processing.
	//
	// Uncommitted messages will be redelivered and reprocessed, which can delay or
	// pause processing.
	committed := fr.shouldAutocommit()
	if committed {
		fr.ackMessage(msg)
	}

	data := msg.Body
	var err error
	logger := fr.msgWrappedLogger(msg)

	// Optionally decode and decrypt message
	data, err = compliance.Reveal(fr.transformConfig, data)
	if err != nil {
		return logger.Error().LogErrorf("unable to reveal event: %v", err).Err()
	}

	event, readErr := models.Read(data)
	if event == nil {
		logger.Error().Log("no event read from data")
		return nil
	}
	if readErr != nil {
		logger.Error().LogErrorf("unable to read %s event: %v", event.Type, readErr)
	}

	var file *incoming.ACHFile
	switch evt := event.Event.(type) {
	case *models.QueueACHFile:
		f := incoming.ACHFile(*evt)
		file = &f

	case incoming.ACHFile:
		file = &evt

	case *models.CancelACHFile:
		err = fr.cancelACHFile(ctx, evt, msg, source)
		if err != nil {
			return logger.With(log.Fields{
				"type": log.String(fmt.Sprintf("%T", evt)),
			}).LogError(err).Err()
		}
		if !committed {
			fr.ackMessage(msg)
		}
		return nil
	}

	logger = logger.With(log.Fields{
		"type": log.String(fmt.Sprintf("%T", file)),
	})

	if file != nil {
		// Quit after we failed to read the event's file
		if readErr != nil {
			producerErr := fr.produceInvalidQueueFile(ctx, logger, *file, readErr)
			if producerErr != nil {
				return logger.LogErrorf("problem producing InvalidQueueFile: %w", producerErr).Err()
			}
			return readErr
		}

		// Drive the staged reception state machine. It resolves the published
		// result (possibly from an earlier attempt) or returns a retriable
		// infrastructure error while keeping the reception resumable.
		resp, accepted, processErr := fr.processACHFile(ctx, *file, msg, source)
		if processErr != nil {
			// Parse/config problems keep the legacy InvalidQueueFile signal;
			// retriable stage/index/publish failures must not brand a valid
			// file invalid — redelivery resumes the same commit.
			if !errors.Is(processErr, ErrRetriable) {
				if producerErr := fr.produceInvalidQueueFile(ctx, logger, *file, processErr); producerErr != nil {
					return logger.LogErrorf("problem producing InvalidQueueFile after processing file: %w", producerErr).Err()
				}
			}
			return processErr
		}

		// Only a published file produces a response; validate-drop attempts
		// behave exactly as before (acknowledged without a response).
		if accepted {
			fr.deliverQueueResponse(logger, resp)
		}

		if !committed {
			fr.ackMessage(msg)
		}

		return nil
	}

	// Unhandled Message
	return logger.Error().LogError(errors.New("unhandled message")).Err()
}

func (fr *FileReceiver) shouldAutocommit() bool {
	kafkaConfig := fr.cfg.Inbound.Kafka
	if kafkaConfig == nil {
		return false
	}
	return kafkaConfig.AutoCommit
}

func (fr *FileReceiver) msgWrappedLogger(msg *pubsub.Message) log.Logger {
	logger := fr.logger.With(log.Fields{
		"loggableID": log.String(msg.LoggableID),
		"length":     log.Int(len(msg.Body)),
	})

	var details *sarama.ConsumerMessage
	ok := msg.As(&details)
	if ok && details != nil {
		logger = logger.With(log.Fields{
			"key":       log.String(string(details.Key)),
			"topic":     log.String(details.Topic),
			"partition": log.Int64(int64(details.Partition)),
			"offset":    log.Int64(details.Offset),
		})
	}

	return logger
}

func (fr *FileReceiver) produceInvalidQueueFile(ctx context.Context, logger log.Logger, file incoming.ACHFile, err error) error {
	logger.Info().Log("producing InvalidQueueFile")

	hostname, _ := os.Hostname()

	return fr.eventEmitter.Send(ctx, models.Event{
		Event: models.InvalidQueueFile{
			File:     models.QueueACHFile(file),
			Error:    err.Error(),
			Hostname: hostname,
		},
	})
}

func (fr *FileReceiver) getAggregator(ctx context.Context, shardKey string) *aggregator {
	shardName, err := fr.shardRepository.Lookup(shardKey)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			fr.logger.Error().LogErrorf("problem looking up shardKey=%s: %v", shardKey, err)
			return nil
		}
	}
	if shardName == "" {
		// Often we have deployments with "SD-live-odfi" that have shard keys of "SD-$uuid".
		// We want "SD-live-odfi" (as a shard key) to represent "SD-live-odfi" (as a shard name).
		shardName = shardKey
	}

	agg, exists := fr.shardAggregators[shardName]
	if !exists {
		fr.logger.Warn().With(log.Fields{
			"shard_key":  log.String(shardKey),
			"shard_name": log.String(shardName),
		}).Log("found no shard so using default shard")

		telemetry.RecordError(ctx, fmt.Errorf("shardKey=%s shardName%s not found", shardKey, shardName))

		agg, exists = fr.shardAggregators[fr.defaultShardName]
		if !exists {
			filesMissingShardAggregators.With("shard", shardName).Add(1)
			fr.logger.Error().LogErrorf("no default shard %s configured after secondary lookup shardKey=%s shardName=%s", fr.defaultShardName, shardKey, shardName)
			return nil
		}
	}
	if agg == nil {
		fr.logger.Error().LogErrorf("nil shardAggregator for shardKey=%s shardName=%s", shardKey, shardName)
		return nil
	}
	return agg
}

// deliverQueueResponse hands a frozen reception result to any in-process HTTP
// waiter. It must never block the publish/ack path: when nobody is waiting
// (Kafka origin, timed-out HTTP call, saturated buffer) the result stays in
// the ledger outbox and the caller retries with the same commitID.
func (fr *FileReceiver) deliverQueueResponse(logger log.Logger, resp incoming.QueueACHFileResponse) {
	select {
	case fr.QueueFileResponses <- resp:
	default:
		logger.Debug().Log("no in-process waiter for QueueACHFile response; result remains in ledger outbox")
	}
}

func (fr *FileReceiver) deliverCancellation(logger log.Logger, resp models.FileCancellationResponse) {
	select {
	case fr.CancellationResponses <- resp:
	default:
		logger.Debug().Log("no in-process waiter for FileCancellationResponse")
	}
}

func (fr *FileReceiver) processACHFile(
	ctx context.Context,
	file incoming.ACHFile,
	msg *pubsub.Message,
	source string,
) (incoming.QueueACHFileResponse, bool, error) {
	if file.FileID == "" || file.ShardKey == "" {
		return incoming.QueueACHFileResponse{}, false, errors.New("missing fileID or shardKey")
	}

	err := file.Validate()
	if err != nil {
		telemetry.RecordError(ctx, err)
		fr.logger.Error().LogErrorf("invalid ACHFile: %v", err)
		// Preserve the existing drop-and-continue behavior.
		return incoming.QueueACHFileResponse{}, false, nil
	}

	// Pull Aggregator from the config
	agg := fr.getAggregator(ctx, file.ShardKey)
	if agg == nil {
		return incoming.QueueACHFileResponse{}, false, fmt.Errorf("no aggregator for shard key %s found", file.ShardKey)
	}

	commitID := resolveCommitID(msg, file)
	file.CommitID = commitID

	logger := fr.logger.With(log.Fields{
		"fileID":    log.String(file.FileID),
		"commitID":  log.String(commitID),
		"shardName": log.String(agg.shard.Name),
		"shardKey":  log.String(file.ShardKey),
	})

	now := time.Now().In(time.UTC)
	hostname, _ := os.Hostname()

	// Stage 1: durable reception fact. Idempotent — a retry or concurrent
	// duplicate finds the first writer's row and continues that commit.
	reception := files.Reception{
		CommitID:   commitID,
		ShardName:  agg.shard.Name,
		ShardKey:   file.ShardKey,
		FileID:     file.FileID,
		Source:     source,
		Hostname:   hostname,
		AcceptedAt: now,
		UpdatedAt:  now,
	}
	if err := fr.fileRepository.RecordReception(ctx, reception); err != nil {
		return incoming.QueueACHFileResponse{}, false, retriablef("recording reception: %v", err)
	}
	existing, err := fr.fileRepository.GetReception(ctx, commitID)
	if err != nil {
		return incoming.QueueACHFileResponse{}, false, retriablef("reading reception: %v", err)
	}

	// A terminal commit returns its frozen result without touching disk again.
	if existing.Stage == files.StagePublished {
		resp, ok := fr.frozenQueueResponse(ctx, logger, agg, file, existing)
		if !ok {
			return incoming.QueueACHFileResponse{}, false, retriablef("reading published result for %s", commitID)
		}
		logger.Debug().Log("already published -- returning frozen result")
		return resp, true, nil
	}
	if existing.Stage == files.StageFailed {
		logger.Debug().Log("previously failed -- returning frozen failure result")
		return agg.buildQueueResponse(file, commitID, existing.AcceptedAt, existing.LastError), true, nil
	}

	// Stage 2: durable staging. A write failure cleans its unpublished content;
	// the reception stays resumable at received.
	if files.StageRank(existing.Stage) < files.StageRank(files.StageStaged) {
		contentHash, stageErr := agg.stageFile(ctx, file, commitID)
		if stageErr != nil {
			telemetry.RecordError(ctx, stageErr)
			if cleanupErr := agg.cleanupStaging(ctx, commitID); cleanupErr != nil {
				logger.Error().LogErrorf("unable to clean up staging: %v", cleanupErr)
			}
			return incoming.QueueACHFileResponse{}, false, retriablef("staging file: %v", stageErr)
		}
		if err := fr.fileRepository.MarkStaged(ctx, commitID, contentHash, now); err != nil {
			return incoming.QueueACHFileResponse{}, false, retriablef("marking staged: %v", err)
		}
		logger.Log("staged ACH file")

		// Re-read in case a concurrent attempt on another instance advanced.
		existing, err = fr.fileRepository.GetReception(ctx, commitID)
		if err != nil {
			return incoming.QueueACHFileResponse{}, false, retriablef("reading reception after staging: %v", err)
		}
	}

	// Stage 3: unpublished file index row. A write failure keeps the staged
	// content and leaves the commit retryable.
	if files.StageRank(existing.Stage) < files.StageRank(files.StageIndexed) {
		acceptanceData := files.AcceptedFile{
			FileID:     file.FileID,
			ShardKey:   file.ShardKey,
			Hostname:   hostname,
			AcceptedAt: existing.AcceptedAt,
		}
		if recordErr := fr.fileRepository.Record(ctx, acceptanceData); recordErr != nil && !database.UniqueViolation(recordErr) {
			return incoming.QueueACHFileResponse{}, false, retriablef("indexing file: %v", recordErr)
		}
		if err := fr.fileRepository.MarkIndexed(ctx, commitID, now); err != nil {
			return incoming.QueueACHFileResponse{}, false, retriablef("marking indexed: %v", err)
		}
		existing, err = fr.fileRepository.GetReception(ctx, commitID)
		if err != nil {
			return incoming.QueueACHFileResponse{}, false, retriablef("reading reception after indexing: %v", err)
		}
	}

	// A cancel that landed before publication is held both in the ledger and
	// (via HandleCancel / this marker) on disk. Make sure the marker exists so
	// the atomic publish lands the file in its canceled form.
	if existing.CancelRequestedAt != nil {
		cancelIntent := incoming.CancelACHFile{
			CommitID: commitID,
			FileID:   file.FileID,
			ShardKey: file.ShardKey,
		}
		if err := agg.recordCancelIntent(ctx, cancelIntent); err != nil {
			return incoming.QueueACHFileResponse{}, false, retriablef("recording cancel intent: %v", err)
		}
	}

	// Stage 4: atomic publish. A failure here leaves staged content intact and
	// never marks anything published — a redelivery (or startup recovery)
	// finishes the same commit.
	if existing.Stage != files.StagePublished {
		if publishErr := agg.publishFile(ctx, file, commitID); publishErr != nil {
			// A concurrent winner may have moved staging already and published.
			if refreshed, getErr := fr.fileRepository.GetReception(ctx, commitID); getErr == nil &&
				(refreshed.Stage == files.StagePublished || refreshed.Stage == files.StageFailed) {
				if refreshed.Stage == files.StageFailed {
					return agg.buildQueueResponse(file, commitID, refreshed.AcceptedAt, refreshed.LastError), true, nil
				}
				resp, ok := fr.frozenQueueResponse(ctx, logger, agg, file, refreshed)
				if !ok {
					return incoming.QueueACHFileResponse{}, false, retriablef("reading published result for %s", commitID)
				}
				return resp, true, nil
			}
			// The move itself may have succeeded before the failure/crash. When
			// the expected bytes are already in mergable the redelivery
			// finalizes the markers instead of retrying the move forever.
			if m, ok := agg.merger.(*filesystemMerging); ok {
				candidates := m.mergableCandidates(*existing)
				if fr.publishedContentPresent(ctx, m, existing.ContentSHA256, candidates) {
					if err := m.CompletePendingSidecar(ctx, file, commitID); err != nil {
						logger.Warn().Logf("completing pending sidecar: %v", err)
					}
					return fr.finalizePublished(ctx, logger, agg, file, existing, now)
				}
			}
			return incoming.QueueACHFileResponse{}, false, retriablef("publishing file: %v", publishErr)
		}
	}

	return fr.finalizePublished(ctx, logger, agg, file, existing, now)
}

// finalizePublished freezes the result against the ledger's acceptance time,
// marks the ledger and the index published together, and drops the staging
// directory. It finalizes both first-time publishes and redeliveries that
// found the file already moved into mergable.
func (fr *FileReceiver) finalizePublished(
	ctx context.Context,
	logger log.Logger,
	agg *aggregator,
	file incoming.ACHFile,
	reception *files.Reception,
	now time.Time,
) (incoming.QueueACHFileResponse, bool, error) {
	resp := agg.buildQueueResponse(file, reception.CommitID, reception.AcceptedAt, "")
	resultJSON, marshalErr := freezeResult(resp)
	if marshalErr != nil {
		return incoming.QueueACHFileResponse{}, false, retriablef("freezing result: %v", marshalErr)
	}
	if err := fr.fileRepository.MarkPublished(ctx, reception.CommitID, reception.FileID, resultJSON, now); err != nil {
		return incoming.QueueACHFileResponse{}, false, retriablef("marking published: %v", err)
	}

	if err := agg.cleanupStaging(ctx, reception.CommitID); err != nil {
		logger.Warn().Logf("unable to remove staging directory after publish: %v", err)
	}

	pendingFiles.With("shard", agg.shard.Name).Add(1)
	logger.Log("published ACH file")

	return resp, true, nil
}

// frozenQueueResponse decodes the result frozen at publish time. When the
// marker landed without the payload (crash window) it rebuilds the identical
// response from the ledger's acceptance time and re-persists it.
func (fr *FileReceiver) frozenQueueResponse(ctx context.Context, logger log.Logger, agg *aggregator, file incoming.ACHFile, reception *files.Reception) (incoming.QueueACHFileResponse, bool) {
	if len(reception.ResultJSON) > 0 {
		var resp incoming.QueueACHFileResponse
		if err := json.Unmarshal(reception.ResultJSON, &resp); err == nil && resp.CommitID == reception.CommitID {
			return resp, true
		}
		logger.Warn().Logf("rebuilding unreadable frozen result for commit %s", reception.CommitID)
	}
	resp := agg.buildQueueResponse(file, reception.CommitID, reception.AcceptedAt, reception.LastError)
	resultJSON, err := freezeResult(resp)
	if err != nil {
		return incoming.QueueACHFileResponse{}, false
	}
	publishedAt := time.Now().In(time.UTC)
	if reception.PublishedAt != nil {
		publishedAt = *reception.PublishedAt
	}
	if err := fr.fileRepository.MarkPublished(ctx, reception.CommitID, reception.FileID, resultJSON, publishedAt); err != nil {
		logger.Warn().Logf("unable to re-freeze rebuilt result: %v", err)
	}
	return resp, true
}

func (fr *FileReceiver) cancelACHFile(ctx context.Context, cancel *models.CancelACHFile, msg *pubsub.Message, source string) error {
	if cancel == nil || cancel.FileID == "" || cancel.ShardKey == "" {
		return errors.New("missing fileID or shardKey")
	}

	// Get the Aggregator from the config
	agg := fr.getAggregator(ctx, cancel.ShardKey)
	if agg == nil {
		return nil
	}

	evt := incoming.CancelACHFile(*cancel)
	if evt.CommitID == "" {
		evt.CommitID = resolveCommitID(msg, incoming.ACHFile{FileID: evt.FileID, ShardKey: evt.ShardKey})
	}

	logger := fr.logger.With(log.Fields{
		"fileID":    log.String(cancel.FileID),
		"commitID":  log.String(evt.CommitID),
		"shardName": log.String(agg.shard.Name),
		"shardKey":  log.String(cancel.ShardKey),
	})

	now := time.Now().In(time.UTC)
	hostname, _ := os.Hostname()

	// Persist the cancel intent even when the file has not arrived yet. The
	// reception row doubles as the "cancel arrived first" marker in the
	// ledger; disk still gets the .canceled placeholder below.
	reception := files.Reception{
		CommitID:   evt.CommitID,
		ShardName:  agg.shard.Name,
		ShardKey:   evt.ShardKey,
		FileID:     evt.FileID,
		Source:     source,
		Hostname:   hostname,
		AcceptedAt: now,
		UpdatedAt:  now,
	}
	if err := fr.fileRepository.RecordReception(ctx, reception); err != nil {
		return logger.Error().LogErrorf("problem recording cancel reception: %v", err).Err()
	}
	if err := fr.fileRepository.RequestCancellation(ctx, evt.CommitID, now); err != nil {
		return logger.Error().LogErrorf("problem recording cancellation intent: %v", err).Err()
	}

	// A published file is canceled in the index; unpublished rows are left
	// for the intent path to apply at publish time.
	if err := fr.fileRepository.Cancel(ctx, evt.FileID); err != nil {
		return logger.Error().LogErrorf("problem recording cancellation: %v", err).Err()
	}
	logger.Log("begin canceling ACH file")

	response, err := agg.cancelFile(ctx, evt)
	if err != nil {
		return logger.Error().LogErrorf("problem canceling file: %v", err).Err()
	}
	response.CommitID = evt.CommitID

	fr.deliverCancellation(logger, response)

	logger.Log("finished cancel of file")
	return nil
}
