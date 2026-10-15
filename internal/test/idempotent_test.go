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

package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moov-io/achgateway/internal/files"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/incoming/stream/streamtest"
	"github.com/moov-io/achgateway/internal/incoming/web"
	"github.com/moov-io/achgateway/internal/pipeline"
	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/shards"
	"github.com/moov-io/achgateway/internal/storage"
	"github.com/moov-io/base/database"
	"github.com/moov-io/base/log"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestIdempotentHTTPRetryReturnsSameResult(t *testing.T) {
	ctx := context.Background()
	logger := log.NewTestLogger()

	dir := t.TempDir()
	conf := &service.Config{
		Sharding: service.Sharding{
			Default: "testing",
			Shards: []service.Shard{
				{
					Name:       "testing",
					Cutoffs:    service.Cutoffs{Timezone: "America/New_York", Windows: []string{"12:00"}},
					UploadAgent: "mock",
				},
				{
					Name:       "SD-live-odfi",
					Cutoffs:    service.Cutoffs{Timezone: "America/Chicago", Windows: []string{"5:00"}},
					UploadAgent: "mock",
				},
			},
		},
		Upload: service.UploadAgents{
			Merging: service.Merging{
				Storage: storage.Config{Filesystem: storage.FilesystemConfig{Directory: dir}},
			},
			Agents: []service.UploadAgent{
				{ID: "mock", Mock: &service.MockAgent{}},
			},
		},
	}

	shardRepo := shards.NewInMemoryRepository()
	shardRepo.Add(service.ShardMapping{ShardKey: "testing", ShardName: "testing"}, database.NopInTx)
	shardRepo.Add(service.ShardMapping{ShardKey: "SD-live", ShardName: "SD-live-odfi"}, database.NopInTx)

	httpTopic, httpSub := streamtest.InmemStream(t)
	fileRepo := files.NewMockRepository()

	fileReceiver, err := pipeline.Start(ctx, logger, conf, shardRepo, fileRepo, httpSub)
	require.NoError(t, err)
	t.Cleanup(fileReceiver.Shutdown)

	controller := web.NewFilesController(logger, service.HTTPConfig{}, httpTopic,
		fileReceiver.QueueFileResponses, fileReceiver.CancellationResponses)
	r := mux.NewRouter()
	controller.AppendRoutes(r)

	submit := func(fileID, shardKey, idempotencyKey string) incoming.QueueACHFileResponse {
		// Each POST gets a fresh file handle because the body is consumed.
		req := newPostRequest(t, filepath.Join("..", "..", "testdata", "ppd-debit.ach"), shardKey, fileID, idempotencyKey)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp incoming.QueueACHFileResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		return resp
	}

	fileID := uuid.NewString()
	const idempotencyKey = "http-retry-key-1"
	first := submit(fileID, "testing", idempotencyKey)
	require.Equal(t, idempotencyKey, first.CommitID)
	require.NotNil(t, first.AcceptedAt)
	require.Empty(t, first.Error)

	// Redelivery: same key returns the frozen original result, no rewrite.
	second := submit(fileID, "testing", idempotencyKey)
	require.Equal(t, first.CommitID, second.CommitID)
	require.True(t, first.AcceptedAt.Equal(*second.AcceptedAt))
	require.Equal(t, first.NextCutoff.Unix(), second.NextCutoff.Unix())

	// Exactly one mergable file for the commit.
	require.Eventually(t, func() bool {
		matches, err := filepath.Glob(filepath.Join(dir, "mergable", "testing", fileID+".ach"))
		return err == nil && len(matches) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// Shard routing: a different shard lands in its own mergable directory.
	otherID := uuid.NewString()
	other := submit(otherID, "SD-live", "other-key")
	require.Equal(t, "SD-live", other.ShardKey)
	require.Eventually(t, func() bool {
		matches, err := filepath.Glob(filepath.Join(dir, "mergable", "SD-live-odfi", otherID+".ach"))
		return err == nil && len(matches) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// Cancel semantics remain intact end to end.
	cancelReq := httptest.NewRequest("DELETE", "/shards/testing/files/"+fileID+".ach", nil)
	cancelReq.Header.Set(incoming.IdempotencyKeyHeader, idempotencyKey)
	cancelW := httptest.NewRecorder()
	r.ServeHTTP(cancelW, cancelReq)
	require.Equal(t, http.StatusOK, cancelW.Code)
}

func newPostRequest(t *testing.T, path, shardKey, fileID, idempotencyKey string) *http.Request {
	t.Helper()
	body, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { body.Close() })
	req := httptest.NewRequest("POST", "/shards/"+shardKey+"/files/"+fileID, body)
	req.Header.Set(incoming.IdempotencyKeyHeader, idempotencyKey)
	return req
}


