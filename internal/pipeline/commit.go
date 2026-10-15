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
	"github.com/moov-io/achgateway/internal/incoming"

	"gocloud.dev/pubsub"
)

// resolveCommitID resolves the stable commit identity carried by one received
// message: pubsub metadata first (HTTP Idempotency-Key or Kafka record key),
// then the event body field, then the deterministic (shardKey, fileID)
// fallback.
func resolveCommitID(msg *pubsub.Message, file incoming.ACHFile) string {
	var metadata map[string]string
	if msg != nil {
		metadata = msg.Metadata
	}
	return incoming.ResolveCommitID(metadata, file.CommitID, file.ShardKey, file.FileID)
}
