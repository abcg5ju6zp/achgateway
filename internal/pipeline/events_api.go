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
	"net/http"
	"path/filepath"
	"strings"

	"github.com/moov-io/base/log"
	"github.com/moov-io/base/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gorilla/mux"
)

func (fr *FileReceiver) manuallyProduceFileUploaded() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filename := r.URL.Query().Get("filename")

		logger := fr.logger.With(log.Fields{
			"filename": log.String(filename),
			"route":    log.String("manual-file-upload-produce"),
		})

		agg := fr.lookupAggregator(logger, r)
		if agg == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		m, ok := agg.merger.(*filesystemMerging)
		if !ok {
			logger.Error().Logf("unexpected %T merger", agg.merger)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		dir := mux.Vars(r)["isolatedDirectory"]
		if dir == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Reject paths which are trying to traverse the filesystem
		if strings.Contains(dir, "..") || filepath.IsAbs(dir) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ctx, span := telemetry.StartSpan(r.Context(), "pipeline-manual-file-uploaded", trace.WithAttributes(
			attribute.String("achgateway.dir", dir),
			attribute.String("achgateway.filename", filename),
		))
		defer span.End()

		matches, err := m.getNonCanceledMatches(ctx, dir)
		if err != nil {
			logger.Error().LogErrorf("problem listing matches: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(matches) == 0 {
			logger.Logf("%s not found", dir)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// When the directory is a tracked batch, carry its identity on the
	// backfilled events and reuse the recorded upload time.
	batchID, uploadedAt := agg.batchIdentity(ctx, dir)
	mf := mergedFile{
		InputFilepaths:   matches,
		UploadedFilename: filename,
		Shard:            agg.shard.Name,
		BatchID:          batchID,
		UploadedAt:       uploadedAt,
	}
	merged := []mergedFile{mf}

	if err := agg.emitFilesUploaded(ctx, merged); err != nil {
		logger.Error().LogErrorf("problem emitting FileUploaded: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Record the operator-confirmed events under the batch lock, merging only
	// the event flags so concurrent stage facts are never rolled back.
	names := make([]string, 0, len(matches))
	for _, p := range matches {
		names = append(names, filepath.Base(p))
	}
	if err := agg.confirmBackfilledEvents(ctx, dir, names); err != nil {
		logger.Error().LogErrorf("problem recording manual FileUploaded in batch: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	}
}
