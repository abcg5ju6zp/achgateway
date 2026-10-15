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

	"github.com/moov-io/base/telemetry"

	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	incomingHTTPFiles = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "incoming_http_files",
		Help: "Counter of ACH files submitted through the http interface",
	}, nil)
	incomingStreamFiles = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "incoming_stream_files",
		Help: "Counter of ACH files received through stream interface",
	}, nil)

	httpFileProcessingErrors = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "http_file_processing_errors",
		Help: "Counter of http submitted ACH files that failed processing",
	}, nil)
	streamFileProcessingErrors = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "stream_file_processing_errors",
		Help: "Counter of stream submitted ACH files that failed processing",
	}, nil)

	pendingFiles = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "pending_files",
		Help: "Counter of ACH files waiting to be uploaded",
	}, []string{"shard"})
	filesMissingShardAggregators = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "files_missing_shard_aggregators",
		Help: "Counter of ACH files unable to be matched with a shard aggregator",
	}, []string{"shard"})

	uploadedFilesCounter = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_uploaded_files",
		Help: "Counter of ACH files uploaded through the pipeline to the ODFI",
	}, []string{"shard"})
	uploadFilesErrors = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_upload_errors",
		Help: "Counter of errors encountered when attempting ACH files upload",
	}, []string{"shard"})

	// unmappedInputFiles counts input .ach files left in an isolated cutoff directory
	// that were never matched to an uploaded merged file (so no FileUploaded event).
	unmappedInputFiles = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_unmapped_input_files",
		Help: "Counter of isolated input ACH files that failed entry-to-upload mapping (no FileUploaded)",
	}, []string{"shard"})

	// remoteVerifiedUploads counts outputs confirmed present at the remote with
	// a matching digest after an upload returned an ambiguous error, so no
	// second upload was issued.
	remoteVerifiedUploads = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_remote_verified_uploads_total",
		Help: "Counter of uploads confirmed at the remote by path and digest after an ambiguous response",
	}, []string{"shard"})

	// remoteContentConflicts counts outputs whose target path holds content
	// whose digest differs from the batch artifact. The output is frozen.
	remoteContentConflicts = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_remote_content_conflicts_total",
		Help: "Counter of remote verification mismatches; the conflicting output is not uploaded or overwritten",
	}, []string{"shard"})

	// recoveredOutputs counts batch outputs advanced during recovery passes
	// (startup or pre-window) that stayed pending after an earlier run.
	recoveredOutputs = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_recovered_outputs_total",
		Help: "Counter of cutoff outputs driven to completion by a recovery pass",
	}, []string{"shard", "reason"})

	// corruptBatchManifests counts unreadable manifests skipped while scanning.
	corruptBatchManifests = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "ach_corrupt_batch_manifests_total",
		Help: "Counter of cutoff batch manifests that could not be decoded during scans",
	}, []string{"shard"})
)

func init() {
	incomingHTTPFiles.With().Add(0)
	incomingStreamFiles.With().Add(0)

	httpFileProcessingErrors.With().Add(0)
	streamFileProcessingErrors.With().Add(0)
}

func recordFileUploadError(ctx context.Context, shardName string, err error) {
	if err == nil {
		return
	}
	telemetry.RecordError(ctx, err)
	uploadFilesErrors.With("shard", shardName).Add(1)
}

func initializeShardMetrics(shardName string) {
	pendingFiles.With("shard", shardName).Add(0)
	filesMissingShardAggregators.With("shard", shardName).Add(0)

	uploadedFilesCounter.With("shard", shardName).Add(0)
	uploadFilesErrors.With("shard", shardName).Add(0)
	unmappedInputFiles.With("shard", shardName).Add(0)
	remoteVerifiedUploads.With("shard", shardName).Add(0)
	remoteContentConflicts.With("shard", shardName).Add(0)
	for _, reason := range []string{"startup", "cutoff", "manual"} {
		recoveredOutputs.With("shard", shardName, "reason", reason).Add(0)
	}
}

func recordUnmappedInputFiles(shardName string, count int) {
	if count <= 0 {
		return
	}
	unmappedInputFiles.With("shard", shardName).Add(float64(count))
}

func recordRemoteVerifiedUpload(shardName string) {
	remoteVerifiedUploads.With("shard", shardName).Add(1)
}

func recordRemoteContentConflict(shardName string) {
	remoteContentConflicts.With("shard", shardName).Add(1)
}

func recordRecoveredOutputs(shardName, reason string, count int) {
	if count <= 0 {
		return
	}
	recoveredOutputs.With("shard", shardName, "reason", reason).Add(float64(count))
}

func recordCorruptBatchManifest(shardName string) {
	corruptBatchManifests.With("shard", shardName).Add(1)
}
