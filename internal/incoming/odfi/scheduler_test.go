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

package odfi

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base/log"

	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	cfg := &service.Config{
		Logger: log.NewTestLogger(),
		Inbound: service.Inbound{
			ODFI: &service.ODFIFiles{
				Interval:   10 * time.Second,
				ShardNames: []string{"mock"},
				Storage: service.ODFIStorage{
					CleanupLocalDirectory: true,
					KeepRemoteFiles:       false,
					RemoveZeroByteFiles:   true,
				},
			},
		},
		Upload: service.UploadAgents{
			Agents: []service.UploadAgent{
				{
					ID:   "ftp-test",
					Mock: &service.MockAgent{},
				},
			},
			DefaultAgentID: "ftp-test",
		},
	}
	cfg.Logger = log.NewTestLogger()

	processors := SetupProcessors(&MockProcessor{})
	schd, err := NewPeriodicScheduler(cfg.Logger, cfg, processors)
	require.NoError(t, err)
	require.NotNil(t, schd)

	ss, ok := schd.(*PeriodicScheduler)
	if !ok {
		t.Fatalf("unexpected scheduler: %T", schd)
	}

	mock := &service.Shard{
		Name:        "mock",
		UploadAgent: "ftp-test",
	}
	if err := ss.tick(context.Background(), cfg.Logger, mock); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_TickConsumesConfirmsAndDeletesOnce(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("testdata", "forward.ach"))
	require.NoError(t, err)

	storageDir := t.TempDir()
	cfg := &service.Config{
		Logger: log.NewTestLogger(),
		Inbound: service.Inbound{
			ODFI: &service.ODFIFiles{
				Interval:   10 * time.Second,
				ShardNames: []string{"mock"},
				Storage: service.ODFIStorage{
					Directory:             storageDir,
					CleanupLocalDirectory: false,
					KeepRemoteFiles:       false,
				},
			},
		},
		Upload: service.UploadAgents{
			Agents: []service.UploadAgent{
				{
					// upload.MockAgent reports ID() as "mock-agent", so use that
					// as the config ID to reuse the cached agent instance.
					ID:   "mock-agent",
					Mock: &service.MockAgent{},
				},
			},
			DefaultAgentID: "mock-agent",
		},
	}

	proc := &countingProcessor{typ: "mock"}
	schd, err := NewPeriodicScheduler(cfg.Logger, cfg, SetupProcessors(proc))
	require.NoError(t, err)
	ss := schd.(*PeriodicScheduler)

	// Pre-create the cached mock agent so a remote file can be seeded.
	agent, err := upload.New(cfg.Logger, cfg.Upload, "mock-agent")
	require.NoError(t, err)
	mockAgent := agent.(*upload.MockAgent)
	seedFile := func() {
		mockAgent.InboundFilepaths = []string{filepath.Join("inbound", "forward.ach")}
		mockAgent.ReadableFile = &upload.File{
			Filepath: "forward.ach",
			Contents: io.NopCloser(bytes.NewReader(content)),
		}
	}
	seedFile()

	shard := &service.Shard{Name: "mock", UploadAgent: "mock-agent"}

	// First tick consumes and deletes.
	require.NoError(t, ss.tick(context.Background(), cfg.Logger, shard))
	require.Equal(t, 1, proc.count())
	require.Equal(t, filepath.Join("inbound", "forward.ach"), mockAgent.DeletedFile)

	results, err := ss.downloader.ConsumptionResults("")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, stageConfirmed, results[0].Downloaded.Status)
	require.Equal(t, stageConfirmed, results[0].Validated.Status)
	require.Equal(t, stageSkipped, results[0].Audited.Status, "no audit configured")
	require.Equal(t, stageConfirmed, results[0].Processors["mock"].Status)
	require.Equal(t, stageConfirmed, results[0].RemoteDeleted.Status)

	// A second tick with the same file present again must not reprocess or
	// re-delete the already confirmed consumption unit.
	seedFile()
	require.NoError(t, ss.tick(context.Background(), cfg.Logger, shard))
	require.Equal(t, 1, proc.count(), "processor must not run twice")
	require.Equal(t, filepath.Join("inbound", "forward.ach"), mockAgent.DeletedFile)
}
