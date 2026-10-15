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

package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/incoming/stream"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/pkg/compliance"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/telemetry"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gocloud.dev/pubsub"
)

func NewFilesController(
	logger log.Logger,
	cfg service.HTTPConfig,
	pub stream.Publisher,
	queueFileResponses chan incoming.QueueACHFileResponse,
	cancellationResponses chan models.FileCancellationResponse,
) *FilesController {
	controller := &FilesController{
		logger:    logger,
		cfg:       cfg,
		publisher: pub,

		// Concurrent requests carrying the same commit identity share the
		// response for one in-flight reception.
		activeQueueFiles:   make(map[string][]chan incoming.QueueACHFileResponse),
		queueFileResponses: queueFileResponses,

		activeCancellations:   make(map[string][]chan models.FileCancellationResponse),
		cancellationResponses: cancellationResponses,
	}

	controller.listenForQueueACHFileResponses()
	controller.listenForCancellations()

	return controller
}

type FilesController struct {
	logger    log.Logger
	cfg       service.HTTPConfig
	publisher stream.Publisher

	queueFileLock      sync.Mutex
	activeQueueFiles   map[string][]chan incoming.QueueACHFileResponse
	queueFileResponses chan incoming.QueueACHFileResponse

	cancellationLock      sync.Mutex
	activeCancellations   map[string][]chan models.FileCancellationResponse
	cancellationResponses chan models.FileCancellationResponse
}

func (c *FilesController) listenForQueueACHFileResponses() {
	c.logger.Info().Log("listening for QueueACHFile responses")
	go func() {
		for {
			// Wait for a message
			resp := <-c.queueFileResponses
			logger := c.logger.Info().With(log.Fields{
				"file_id":   log.String(resp.FileID),
				"shard_key": log.String(resp.ShardKey),
			})

			if resp.Error != "" {
				logger.Error().LogErrorf("problem with QueueACHFile: %v", resp.Error)
			} else {
				logger.Info().Logf("received QueueACHFile response")
			}

			// Match on the stable commit identity when the receiver provides
			// it; otherwise derive the same identity the request was
			// registered under. Partial responses without shardKey fall back
			// to the legacy fileID correlation.
			c.queueFileLock.Lock()
			keys := []string{resp.CommitID}
			if resp.CommitID == "" {
				keys = append(keys,
					incoming.DeriveCommitID(resp.ShardKey, resp.FileID),
					strings.TrimSuffix(resp.FileID, ".ach"),
				)
			}
			for _, key := range keys {
				if key == "" {
					continue
				}
				waiters := c.activeQueueFiles[key]
				if len(waiters) > 0 {
					for _, waiter := range waiters {
						waiter <- resp
					}
					delete(c.activeQueueFiles, key)
				}
			}
			c.queueFileLock.Unlock()
		}
	}()
}

func (c *FilesController) listenForCancellations() {
	c.logger.Info().Log("listening for cancellation responses")
	go func() {
		for {
			// Wait for a message
			cancel := <-c.cancellationResponses
			c.logger.Info().With(log.Fields{
				"file_id":    log.String(cancel.FileID),
				"shard_key":  log.String(cancel.ShardKey),
				"successful": log.Bool(cancel.Successful),
			}).Log("received cancellation response")

			c.cancellationLock.Lock()
			keys := []string{cancel.CommitID}
			if cancel.CommitID == "" {
				keys = append(keys,
					incoming.DeriveCommitID(cancel.ShardKey, cancel.FileID),
					strings.TrimSuffix(cancel.FileID, ".ach"),
				)
			}
			for _, key := range keys {
				if key == "" {
					continue
				}
				waiters := c.activeCancellations[key]
				if len(waiters) > 0 {
					for _, waiter := range waiters {
						waiter <- cancel
					}
					delete(c.activeCancellations, key)
				}
			}
			c.cancellationLock.Unlock()
		}
	}()
}

func (c *FilesController) AppendRoutes(router *mux.Router) *mux.Router {
	router.
		Name("Files.create").
		Methods("POST").
		Path("/shards/{shardKey}/files/{fileID}").
		HandlerFunc(c.CreateFileHandler)

	router.
		Name("Files.cancel").
		Methods("DELETE").
		Path("/shards/{shardKey}/files/{fileID}").
		HandlerFunc(c.CancelFileHandler)

	return router
}

func (c *FilesController) CreateFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shardKey, fileID := vars["shardKey"], vars["fileID"]
	if shardKey == "" || fileID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx, span := telemetry.StartSpan(r.Context(), "create-file-handler", trace.WithAttributes(
		attribute.String("achgateway.shardKey", shardKey),
		attribute.String("achgateway.fileID", fileID),
	))
	defer span.End()

	logger := c.logger.With(log.Fields{
		"shard_key": log.String(shardKey),
		"file_id":   log.String(fileID),
	})

	bs, err := c.readBody(r)
	if err != nil {
		logger.Error().LogErrorf("error reading file: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	file, err := ach.NewReader(bytes.NewReader(bs)).Read()
	if err != nil {
		// attempt JSON decode
		f, err := ach.FileFromJSON(bs)
		if f == nil || err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		file = *f
	}

	commitID := c.commitID(r.Header.Get(incoming.IdempotencyKeyHeader), shardKey, fileID)

	waiter := make(chan incoming.QueueACHFileResponse, 1)
	if err := c.publishFile(ctx, commitID, shardKey, fileID, &file, waiter); err != nil {
		logger.Error().LogErrorf("publishing file: %v", err)

		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var response incoming.QueueACHFileResponse
	select {
	case resp := <-waiter:
		response = resp

	case <-time.After(10 * time.Second):
		response = incoming.QueueACHFileResponse{
			CommitID: commitID,
			FileID:   fileID,
			ShardKey: shardKey,
			Error:    "timeout exceeded",
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (c *FilesController) readBody(req *http.Request) ([]byte, error) {
	defer req.Body.Close()

	var reader io.Reader = req.Body
	if c.cfg.MaxBodyBytes > 0 {
		reader = io.LimitReader(reader, c.cfg.MaxBodyBytes)
	}
	bs, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return compliance.Reveal(c.cfg.Transform, bs)
}

// commitID returns the caller-supplied identity or the deterministic fallback
// for one (shardKey, fileID) submission.
func (c *FilesController) commitID(headerValue, shardKey, fileID string) string {
	if id := strings.TrimSpace(headerValue); id != "" {
		return id
	}
	return incoming.DeriveCommitID(shardKey, fileID)
}

func (c *FilesController) publishFile(ctx context.Context, commitID, shardKey, fileID string, file *ach.File, waiter chan incoming.QueueACHFileResponse) error {
	c.queueFileLock.Lock()
	c.activeQueueFiles[commitID] = append(c.activeQueueFiles[commitID], waiter)
	// Transitional correlation slot for responses that still lack commitID.
	if legacyKey := strings.TrimSuffix(fileID, ".ach"); legacyKey != commitID {
		c.activeQueueFiles[legacyKey] = append(c.activeQueueFiles[legacyKey], waiter)
	}
	c.queueFileLock.Unlock()

	bs, err := compliance.Protect(c.cfg.Transform, models.Event{
		Event: incoming.ACHFile{
			CommitID: commitID,
			FileID:   fileID,
			ShardKey: shardKey,
			File:     file,
		},
	})
	if err != nil {
		return fmt.Errorf("unable to protect incoming file event: %v", err)
	}

	meta := map[string]string{
		incoming.CommitIDMetadataKey: commitID,
		"fileID":                     fileID,
		"shardKey":                   shardKey,
	}

	return c.publisher.Send(ctx, &pubsub.Message{
		Body:     bs,
		Metadata: meta,
	})
}

func (c *FilesController) CancelFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shardKey, fileID := vars["shardKey"], vars["fileID"]
	if shardKey == "" || fileID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Remove .ach suffix if the request added it
	fileID = strings.TrimSuffix(fileID, ".ach")

	ctx, span := telemetry.StartSpan(r.Context(), "cancel-file-handler", trace.WithAttributes(
		attribute.String("achgateway.shardKey", shardKey),
		attribute.String("achgateway.fileID", fileID),
	))
	defer span.End()

	commitID := c.commitID(r.Header.Get(incoming.IdempotencyKeyHeader), shardKey, fileID)

	waiter := make(chan models.FileCancellationResponse, 1)
	err := c.cancelFile(ctx, commitID, shardKey, fileID, waiter)
	if err != nil {
		c.logger.With(log.Fields{
			"shard_key": log.String(shardKey),
			"file_id":   log.String(fileID),
		}).Error().LogErrorf("canceling file: %v", err)

		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var response models.FileCancellationResponse
	select {
	case resp := <-waiter:
		response = resp

	case <-time.After(10 * time.Second):
		response = models.FileCancellationResponse{
			CommitID:   commitID,
			FileID:     fileID,
			ShardKey:   shardKey,
			Successful: false,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (c *FilesController) cancelFile(ctx context.Context, commitID, shardKey, fileID string, waiter chan models.FileCancellationResponse) error {
	c.cancellationLock.Lock()
	c.activeCancellations[commitID] = append(c.activeCancellations[commitID], waiter)
	// Transitional correlation slot for responses that still lack commitID.
	if legacyKey := strings.TrimSuffix(fileID, ".ach"); legacyKey != commitID {
		c.activeCancellations[legacyKey] = append(c.activeCancellations[legacyKey], waiter)
	}
	c.cancellationLock.Unlock()

	bs, err := compliance.Protect(c.cfg.Transform, models.Event{
		Event: incoming.CancelACHFile{
			CommitID: commitID,
			FileID:   fileID,
			ShardKey: shardKey,
		},
	})
	if err != nil {
		return fmt.Errorf("unable to protect cancel file event: %v", err)
	}

	meta := map[string]string{
		incoming.CommitIDMetadataKey: commitID,
		"fileID":                     fileID,
		"shardKey":                   shardKey,
	}

	return c.publisher.Send(ctx, &pubsub.Message{
		Body:     bs,
		Metadata: meta,
	})
}
