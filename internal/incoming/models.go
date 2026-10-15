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

package incoming

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/moov-io/ach"
)

// CommitIDMetadataKey is the pubsub message metadata key carrying a stable
// commit identity. HTTP requests may supply the same value through the
// Idempotency-Key header; Kafka producers may supply it as the record key
// (see kafkapubsub SubscriptionOptions.KeyName).
const CommitIDMetadataKey = "commitID"

// IdempotencyKeyHeader is the HTTP request header carrying a stable commit
// identity for one submission.
const IdempotencyKeyHeader = "Idempotency-Key"

// DeriveCommitID deterministically derives a stable commit identity from the
// shardKey and fileID. Retries of the same submission converge on the same
// commitID even when the caller supplied no explicit idempotency key.
func DeriveCommitID(shardKey, fileID string) string {
	h := sha256.New()
	h.Write([]byte(strings.TrimSpace(shardKey)))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSuffix(strings.TrimSpace(fileID), ".ach")))
	return hex.EncodeToString(h.Sum(nil))
}

// ResolveCommitID picks the first non-empty stable commit identity among the
// supplied pubsub metadata, event body field, and deterministic fallback.
func ResolveCommitID(metadata map[string]string, eventCommitID, shardKey, fileID string) string {
	if metadata != nil {
		if id := strings.TrimSpace(metadata[CommitIDMetadataKey]); id != "" {
			return id
		}
	}
	if id := strings.TrimSpace(eventCommitID); id != "" {
		return id
	}
	return DeriveCommitID(shardKey, fileID)
}

type ACHFile struct {
	// CommitID is the stable identity of one reception. It is optional and
	// derived deterministically when callers omit it.
	CommitID string    `json:"commitID,omitempty"`
	FileID   string    `json:"id"`
	ShardKey string    `json:"shardKey"`
	File     *ach.File `json:"file"`
}

func (f ACHFile) Validate() error {
	if f.FileID == "" {
		return errors.New("missing fileID")
	}
	if f.ShardKey == "" {
		return errors.New("missing shardKey")
	}
	if f.File == nil {
		return errors.New("missing File")
	}
	return nil
}

type QueueACHFileResponse struct {
	// CommitID carries the reception identity so retries correlate the reply
	// with the original submission.
	CommitID string `json:"commitID,omitempty"`

	FileID   string `json:"id"`
	ShardKey string `json:"shardKey"`
	Error    string `json:"error"`

	// NextCutoff is the next automated cutoff that should upload this file.
	// It is omitted when the time cannot be predicted. Callers may still receive
	// the file earlier via a manual cutoff trigger.
	NextCutoff *time.Time `json:"nextCutoff,omitempty"`
	AcceptedAt *time.Time `json:"acceptedAt,omitempty"`
}

type CancelACHFile struct {
	// CommitID is the stable identity of the original reception. A cancel
	// without it derives the same identity from shardKey/fileID.
	CommitID string `json:"commitID,omitempty"`

	FileID   string `json:"id"`
	ShardKey string `json:"shardKey"`
}

type FileCancellationResponse struct {
	CommitID   string `json:"commitID,omitempty"`
	FileID     string `json:"id"`
	ShardKey   string `json:"shardKey"`
	Successful bool   `json:"successful"`
}
