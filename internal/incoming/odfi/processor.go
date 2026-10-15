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
	"crypto/sha1" //nolint:gosec
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/alerting"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base"
	"github.com/moov-io/base/log"

	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	processingErrors = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "inbound_processing_errors",
		Help: "Counter of errors encountered when downloading or processing inbound files",
	}, []string{"processor"})
)

type File struct {
	Filepath string
	ACHFile  *ach.File
}

type FileProcessor interface {
	Type() string

	// Handle processes an ACH file with whatever logic is implemented
	Handle(ctx context.Context, logger log.Logger, file File) error
}

type Processors []FileProcessor

func SetupProcessors(pcs ...FileProcessor) Processors {
	var out Processors
	for i := range pcs {
		v := reflect.ValueOf(pcs[i])
		if !v.IsNil() {
			out = append(out, pcs[i])
		}
	}
	return out
}

func (pcs Processors) HandleAll(ctx context.Context, logger log.Logger, file File) error {
	var el base.ErrorList
	for i := range pcs {
		proc := pcs[i]
		if err := proc.Handle(ctx, logger, file); err != nil {
			processingErrors.With("processor", fmt.Sprintf("%T", proc)).Add(1)

			el.Add(fmt.Errorf("%s: %v", proc.Type(), err))
		}
	}
	if el.Empty() {
		return nil
	}
	return el
}

func ProcessFiles(ctx context.Context, logger log.Logger, dl *downloadedFiles, alerters alerting.Alerters, auditSaver *AuditSaver, validation ach.ValidateOpts, fileProcessors Processors, agent upload.Agent) error {
	units, err := dl.allUnits()
	if err != nil {
		return err
	}

	var el base.ErrorList
	for _, u := range units {
		if err := processUnit(ctx, logger, u, alerters, auditSaver, validation, fileProcessors); err != nil {
			el.Add(fmt.Errorf("processUnit %s: %v", u.state.RemotePath, err))
		}
	}

	if el.Empty() {
		return nil
	}
	return el
}

// processUnit drives one durable consumption unit through validation, audit
// persistence and every file processor. Each phase is recorded durably before
// moving on, so a restart only reruns phases that never completed and
// processors whose event handling is not confirmed never fire again.
func processUnit(ctx context.Context, logger log.Logger, u *consumptionUnit, alerters alerting.Alerters, auditSaver *AuditSaver, validation ach.ValidateOpts, fileProcessors Processors) error {
	logger = logger.With(log.Fields{
		"filename":       log.String(u.state.RemotePath),
		"consumption_id": log.String(u.state.ID),
	})

	bs, err := u.readData()
	if err != nil {
		return err
	}

	// Parse and validate the downloaded file.
	file, parseErr := parseACHFile(bs, validation)
	if u.state.Validated.Status != stageConfirmed {
		// Some return files don't contain FileHeader info, but can be processed as there
		// are batches with entries. Let's continue to process those, but skip other errors.
		if parseErr != nil && !base.Has(parseErr, ach.ErrFileHeader) {
			if recordErr := u.failValidation(parseErr); recordErr != nil {
				return recordErr
			}
			return fmt.Errorf("problem parsing %s: %v", u.state.RemotePath, parseErr)
		}
		if err := u.markValidated(parseErr); err != nil {
			return err
		}
	}

	// Persist the file in the audit trail before any events are emitted.
	if !u.state.Audited.terminal() {
		switch {
		case auditSaver == nil:
			if err := u.skipAudited("no audit storage configured"); err != nil {
				return err
			}
		case len(bytes.TrimSpace(bs)) == 0:
			if err := u.skipAudited("empty file"); err != nil {
				return err
			}
		default:
			auditPath := auditPathForUnit(u, strings.TrimSuffix(auditSaver.hostname, "/"))
			if err := auditSaver.save(ctx, auditPath, bs); err != nil {
				if recordErr := u.failAudit(err); recordErr != nil {
					return recordErr
				}
				return fmt.Errorf("audittrail %s error: %w", auditPath, err)
			}
			if err := u.markAudited(auditPath); err != nil {
				return err
			}
		}
	}

	// Files with a tolerated FileHeader error still expose their batches to
	// processors.
	expected := make([]string, 0, len(fileProcessors))
	for i := range fileProcessors {
		expected = append(expected, fileProcessors[i].Type())
	}
	if err := u.setExpectedProcessors(expected); err != nil {
		return err
	}

	var el base.ErrorList
	for i := range fileProcessors {
		proc := fileProcessors[i]
		// Skip processors which already confirmed their events on an earlier run.
		if u.processorConfirmed(proc.Type()) {
			logger.Logf("skipping already confirmed processor %s", proc.Type())
			continue
		}

		if err := proc.Handle(ctx, logger, File{
			Filepath: processorFilepath(u),
			ACHFile:  &file,
		}); err != nil {
			processingErrors.With("processor", fmt.Sprintf("%T", proc)).Add(1)

			if recordErr := u.markProcessorFailed(proc.Type(), err); recordErr != nil {
				return recordErr
			}
			el.Add(fmt.Errorf("%s: %v", proc.Type(), err))
			continue
		}

		if err := u.markProcessorConfirmed(proc.Type()); err != nil {
			return err
		}
	}

	if !el.Empty() {
		// Preserve the existing alerting behavior: one alert per file with the
		// combined processor errors, then bubble the error up to the tick.
		alertErr := alerters.AlertError(el)
		if alertErr != nil {
			return fmt.Errorf("problem alerting on error: %w", alertErr)
		}
		return fmt.Errorf("processing %s error: %v", u.state.RemotePath, el)
	}
	return nil
}

// parseACHFile parses bytes into an ACH file using the same lenient options as
// the historical processing path and populates content hashes.
func parseACHFile(bs []byte, validation ach.ValidateOpts) (ach.File, error) {
	bs = bytes.TrimSpace(bs)

	reader := ach.NewReader(bytes.NewReader(bs))
	// Enable some default ACH ValidateOpts
	validation.AllowMissingFileControl = true
	validation.AllowMissingFileHeader = true
	validation.AllowUnorderedBatchNumbers = true
	reader.SetValidation(&validation)

	file, err := reader.Read()
	file.ID = hash(bs)
	populateHashes(&file)
	return file, err
}

// processorFilepath returns a slash rooted path of the form
// "/<kind-dir>/<filename>", preserving the substring matching that processors
// apply against their PathMatcher configuration.
func processorFilepath(u *consumptionUnit) string {
	return "/" + strings.TrimPrefix(u.state.RemotePath, "/")
}

// auditPathForUnit builds the same audit trail key as persistInboundFile.
func auditPathForUnit(u *consumptionUnit, hostname string) string {
	kind := filepath.Base(filepath.Clean(u.state.KindDir))
	return fmt.Sprintf("odfi/%s/%s/%s/%s", hostname, kind, time.Now().Format("2006-01-02"), u.state.Filename)
}

func processDir(ctx context.Context, logger log.Logger, dir string, alerters alerting.Alerters, auditSaver *AuditSaver, validation ach.ValidateOpts, fileProcessors Processors) error {
	infos, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading %s: %v", dir, err)
	}

	var el base.ErrorList
	for _, info := range infos {
		where := filepath.Join(dir, info.Name())
		logger = logger.With(log.Fields{
			"filename": log.String(where),
		})
		if err := processFile(ctx, logger, where, alerters, auditSaver, validation, fileProcessors); err != nil {
			el.Add(err)
		}
	}

	if el.Empty() {
		return nil
	}
	return el
}

func processFile(ctx context.Context, logger log.Logger, path string, alerters alerting.Alerters, auditSaver *AuditSaver, validation ach.ValidateOpts, fileProcessors Processors) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("problem opening %s: %v", path, err)
	}
	bs := bytes.TrimSpace(raw)

	file, err := parseACHFile(raw, validation)
	if err != nil {
		// Some return files don't contain FileHeader info, but can be processed as there
		// are batches with entries. Let's continue to process those, but skip other errors.
		if !base.Has(err, ach.ErrFileHeader) {
			return fmt.Errorf("problem parsing %s: %v", path, err)
		}
	}

	// Persist the file if needed
	if err := persistInboundFile(ctx, auditSaver, path, bs); err != nil {
		return err
	}

	// Pass the file off to our handler
	err = fileProcessors.HandleAll(ctx, logger, File{
		Filepath: path,
		ACHFile:  &file,
	})
	if err != nil {
		alertErr := alerters.AlertError(err)
		if alertErr != nil {
			return fmt.Errorf("problem alerting on error: %w", alertErr)
		}
		return fmt.Errorf("processing %s error: %v", path, err)
	}

	return nil
}

func persistInboundFile(ctx context.Context, auditSaver *AuditSaver, path string, bs []byte) error {
	if auditSaver == nil || len(bytes.TrimSpace(bs)) == 0 {
		return nil
	}

	dir, filename := filepath.Split(path)
	dir = filepath.Base(dir)
	hostname := strings.TrimSuffix(auditSaver.hostname, "/")
	auditPath := fmt.Sprintf("odfi/%s/%s/%s/%s", hostname, dir, time.Now().Format("2006-01-02"), filename)

	if err := auditSaver.save(ctx, auditPath, bs); err != nil {
		return fmt.Errorf("audittrail %s error: %w", auditPath, err)
	}
	return nil
}

func populateHashes(file *ach.File) {
	if file == nil {
		return
	}

	for i := range file.Batches {
		entries := file.Batches[i].GetEntries()
		for j := range entries {
			entries[j].ID = hash([]byte(entries[j].String()))
		}
	}

	for i := range file.IATBatches {
		entries := file.IATBatches[i].GetEntries()
		for j := range entries {
			entries[j].ID = hash([]byte(entries[j].String()))
		}
	}
}

func hash(data []byte) string {
	return fmt.Sprintf("%x", sha1.Sum(data)) //nolint:gosec
}
