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
	"testing"

	"github.com/moov-io/achgateway/internal/incoming"

	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
)

func TestDeriveCommitID(t *testing.T) {
	a := incoming.DeriveCommitID("SD-live", "file-1")
	b := incoming.DeriveCommitID("SD-live", "file-1.ach") // suffix-insensitive
	require.Equal(t, a, b, ".ach suffix must not change the identity")
	require.Len(t, a, 64, "expected hex sha256")

	// Deterministic and distinct.
	require.Equal(t, a, incoming.DeriveCommitID("SD-live", "file-1"))
	require.NotEqual(t, a, incoming.DeriveCommitID("SD-other", "file-1"))
	require.NotEqual(t, a, incoming.DeriveCommitID("SD-live", "file-2"))
}

func TestResolveCommitID(t *testing.T) {
	file := incoming.ACHFile{FileID: "f1", ShardKey: "s1"}

	for _, tc := range []struct {
		name     string
		metadata map[string]string
		eventID  string
		want     string
	}{
		{
			name:     "metadata wins",
			metadata: map[string]string{incoming.CommitIDMetadataKey: "from-meta"},
			eventID:  "from-event",
			want:     "from-meta",
		},
		{
			name:    "event field second",
			eventID: "from-event",
			want:    "from-event",
		},
		{
			name:     "empty metadata falls through",
			metadata: map[string]string{incoming.CommitIDMetadataKey: "  "},
			eventID:  "from-event",
			want:     "from-event",
		},
		{
			name: "derived fallback",
			want: incoming.DeriveCommitID("s1", "f1"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := &pubsub.Message{Metadata: tc.metadata}
			file.CommitID = tc.eventID
			require.Equal(t, tc.want, resolveCommitID(msg, file))
		})
	}

	// nil message must not panic and must still derive.
	file.CommitID = ""
	require.Equal(t, incoming.DeriveCommitID("s1", "f1"), resolveCommitID(nil, file))
}
