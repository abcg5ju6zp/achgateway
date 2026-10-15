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
	"encoding/json"
	"time"

	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/pkg/models"
	"github.com/moov-io/base/log"
)

// defaultOutboxInterval bounds how long a frozen result waits before the
// background sweep delivers it. Startup recovery also performs an immediate
// sweep, so restarts do not pay the full interval.
const defaultOutboxInterval = 2 * time.Second

// runOutbox periodically delivers terminal reception results (published and
// failed) that have not been notified yet. Delivery failures leave the row
// pending and never touch the staged/published file.
func (fr *FileReceiver) runOutbox(ctx context.Context, interval time.Duration) {
	// One immediate sweep covers rows recovered during startup.
	fr.pumpOutbox(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fr.pumpOutbox(ctx)
		}
	}
}

// pumpOutbox performs one notification sweep and returns how many results were
// delivered. It is invoked by the background loop, startup recovery and tests.
func (fr *FileReceiver) pumpOutbox(ctx context.Context) int {
	pending, err := fr.fileRepository.ListPendingNotifications(ctx)
	if err != nil {
		fr.logger.LogErrorf("outbox: listing pending notifications: %v", err)
		return 0
	}

	delivered := 0
	for i := range pending {
		reception := pending[i]

		// A result frozen without payload (crash window) is rebuilt by the
		// recovery sweep, which has the file content available. Skip it here
		// rather than fabricating a response.
		if len(reception.ResultJSON) == 0 {
			continue
		}
		var resp incoming.QueueACHFileResponse
		if err := json.Unmarshal(reception.ResultJSON, &resp); err != nil {
			fr.logger.Error().With(log.Fields{
				"commitID": log.String(reception.CommitID),
			}).LogErrorf("outbox: decoding frozen result: %v", err)
			continue
		}
		if resp.CommitID == "" {
			resp.CommitID = reception.CommitID
		}

		if err := fr.eventEmitter.Send(ctx, models.Event{
			Event: models.QueueACHFileResponse(resp),
		}); err != nil {
			receptionResultFailures.With("shard", reception.ShardName).Add(1)
			fr.logger.Error().With(log.Fields{
				"commitID": log.String(reception.CommitID),
				"shard":    log.String(reception.ShardName),
			}).LogErrorf("outbox: sending result (will retry): %v", err)
			continue
		}

		if err := fr.fileRepository.MarkNotified(ctx, reception.CommitID, time.Now().In(time.UTC)); err != nil {
			fr.logger.LogErrorf("outbox: marking notified for %s: %v", reception.CommitID, err)
			continue
		}
		receptionResultsSent.With("shard", reception.ShardName).Add(1)
		delivered++
	}
	return delivered
}

// freezeResult serializes the reception result into the ledger payload.
func freezeResult(resp incoming.QueueACHFileResponse) ([]byte, error) {
	return json.Marshal(resp)
}
