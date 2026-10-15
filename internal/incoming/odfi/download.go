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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/moov-io/achgateway/internal/service"
	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/strx"
	"github.com/moov-io/base/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	filesDownloaded = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Name: "files_downloaded",
		Help: "Counter of files downloaded from a remote server",
	}, []string{"kind"})
)

// Downloader retrieves ODFI files and tracks their durable consumption state.
type Downloader interface {
	CopyFilesFromRemote(ctx context.Context, agent upload.Agent, shard *service.Shard) (*downloadedFiles, error)

	// ConsumptionResults returns the persisted state of consumed files,
	// optionally filtered to a single consumption ID.
	ConsumptionResults(id string) ([]consumptionState, error)
}

func NewDownloader(logger log.Logger, cfg service.ODFIStorage) (Downloader, error) {
	baseDir := strx.Or(cfg.Directory, "storage")
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("problem creating %s: %v", baseDir, err)
	}
	store, err := newConsumptionStore(baseDir)
	if err != nil {
		return nil, err
	}
	sweepStagingDirs(logger, baseDir)

	return &downloaderImpl{
		logger:  logger,
		baseDir: baseDir,
		cfg:     cfg,
		store:   store,
	}, nil
}

type downloaderImpl struct {
	logger  log.Logger
	baseDir string
	cfg     service.ODFIStorage
	store   *consumptionStore
}

// downloadedFiles is the randomly generated staging directory used while
// downloading files during one tick. Downloaded content is committed into the
// durable consumption store, so this directory is always removed when the tick
// finishes.
type downloadedFiles struct {
	dir string

	store *consumptionStore
	host  string

	// units are the consumption slots touched (committed or resumed) while
	// downloading for this host.
	units []*consumptionUnit
}

func (d *downloadedFiles) deleteFiles() error {
	return os.RemoveAll(d.dir)
}

func (d *downloadedFiles) deleteEmptyDirs(ctx context.Context, agent upload.Agent) error {
	count := func(ctx context.Context, path string) int {
		infos, err := os.ReadDir(path)
		if err != nil {
			return -1
		}

		_, span := telemetry.StartSpan(ctx, "odfi-delete-empty-dirs", trace.WithAttributes(
			attribute.String("achgateway.path", path),
			attribute.Int("achgateway.files", len(infos)),
		))
		defer span.End()

		return len(infos)
	}
	if path := filepath.Join(d.dir, agent.InboundPath()); count(ctx, path) == 0 {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("delete inbound: %v", err)
		}
	}
	if path := filepath.Join(d.dir, agent.ReconciliationPath()); count(ctx, path) == 0 {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("delete reconciliation: %v", err)
		}
	}
	if path := filepath.Join(d.dir, agent.ReturnPath()); count(ctx, path) == 0 {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("delete return: %v", err)
		}
	}
	return nil
}

// addUnit registers a consumption unit for this tick, deduplicating by ID.
func (d *downloadedFiles) addUnit(u *consumptionUnit) {
	if u == nil {
		return
	}
	for i := range d.units {
		if d.units[i].state.ID == u.state.ID {
			return
		}
	}
	d.units = append(d.units, u)
}

// allUnits returns the units discovered this tick together with any durable
// units for the host that still have unfinished stages. Resumed units allow
// processing to continue after a restart or failed attempt.
func (d *downloadedFiles) allUnits() ([]*consumptionUnit, error) {
	out := make([]*consumptionUnit, 0, len(d.units))
	seen := make(map[string]bool)
	for _, u := range d.units {
		if !seen[u.state.ID] {
			seen[u.state.ID] = true
			out = append(out, u)
		}
	}

	if d.store == nil || d.host == "" {
		return out, nil
	}
	pending, err := d.store.ListIncomplete(d.host)
	if err != nil {
		return nil, fmt.Errorf("reading unfinished consumption state: %w", err)
	}
	for _, u := range pending {
		if !seen[u.state.ID] {
			seen[u.state.ID] = true
			out = append(out, u)
		}
	}
	return out, nil
}

func (dl *downloaderImpl) setup() (*downloadedFiles, error) {
	dir, err := os.MkdirTemp(dl.baseDir, "download")
	if err != nil {
		return nil, err
	}

	dl.logger.Logf("created directory %s", dir)

	return &downloadedFiles{
		dir:   dir,
		store: dl.store,
	}, nil
}

func (dl *downloaderImpl) ConsumptionResults(id string) ([]consumptionState, error) {
	return dl.store.Results(id)
}

func (dl *downloaderImpl) CopyFilesFromRemote(ctx context.Context, agent upload.Agent, shard *service.Shard) (*downloadedFiles, error) {
	out, err := dl.setup()
	if err != nil {
		return nil, err
	}
	out.host = agent.Hostname()

	logger := dl.logger.With(log.Fields{
		"shard": log.String(shard.Name),
	})

	var el stageErrorList

	// copy down files from our "inbound" directory
	filepaths, err := agent.GetInboundFiles(ctx)
	logger.Logf("%T found %d inbound files in %s", agent, len(filepaths), agent.InboundPath())
	if err != nil {
		return out, fmt.Errorf("problem downloading inbound files: %v", err)
	}
	filesDownloaded.With("kind", "inbound").Add(float64(len(filepaths)))
	stagingDir := filepath.Join(out.dir, agent.InboundPath())
	if err := saveFilepaths(ctx, logger, agent, filepaths, stagingDir); err != nil {
		return out, fmt.Errorf("problem saving inbound files: %v", err)
	}
	if err := dl.commitDownloads(logger, out, agent, agent.InboundPath(), stagingDir); err != nil {
		el.add(err)
	}

	// copy down files from out "reconciliation" directory
	filepaths, err = agent.GetReconciliationFiles(ctx)
	logger.Logf("%T found %d reconciliation files in %s", agent, len(filepaths), agent.ReconciliationPath())
	if err != nil {
		return out, fmt.Errorf("problem downloading reconciliation files: %v", err)
	}
	filesDownloaded.With("kind", "reconciliation").Add(float64(len(filepaths)))
	stagingDir = filepath.Join(out.dir, agent.ReconciliationPath())
	if err := saveFilepaths(ctx, logger, agent, filepaths, stagingDir); err != nil {
		return out, fmt.Errorf("problem saving reconciliation files: %v", err)
	}
	if err := dl.commitDownloads(logger, out, agent, agent.ReconciliationPath(), stagingDir); err != nil {
		el.add(err)
	}

	// copy down files from out "return" directory
	filepaths, err = agent.GetReturnFiles(ctx)
	logger.Logf("%T found %d return files in %s", agent, len(filepaths), agent.ReturnPath())
	if err != nil {
		return out, fmt.Errorf("problem downloading return files: %v", err)
	}
	filesDownloaded.With("kind", "return").Add(float64(len(filepaths)))
	stagingDir = filepath.Join(out.dir, agent.ReturnPath())
	if err := saveFilepaths(ctx, logger, agent, filepaths, stagingDir); err != nil {
		return out, fmt.Errorf("problem saving return files: %v", err)
	}
	if err := dl.commitDownloads(logger, out, agent, agent.ReturnPath(), stagingDir); err != nil {
		el.add(err)
	}

	return out, el.errOrNil()
}

// commitDownloads promotes every fully staged file into its durable
// consumption slot keyed by host, remote path and content digest.
func (dl *downloaderImpl) commitDownloads(logger log.Logger, out *downloadedFiles, agent upload.Agent, kindDir, stagingDir string) error {
	infos, err := os.ReadDir(stagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading staged files in %s: %v", stagingDir, err)
	}

	var failed []string
	var firstErr error
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		remotePath := filepath.ToSlash(filepath.Join(kindDir, info.Name()))
		stagedPath := filepath.Join(stagingDir, info.Name())
		unit, err := out.store.CommitDownload(agent.Hostname(), remotePath, kindDir, stagedPath)
		if err != nil {
			logger.Error().LogError(err)
			failed = append(failed, remotePath)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		logger.Logf("committed %s as consumption %s", remotePath, unit.state.ID)
		out.addUnit(unit)
	}
	if len(failed) > 0 {
		return fmt.Errorf("commitDownloads problem on: %s: %v", strings.Join(failed, ", "), firstErr)
	}
	return nil
}

// stageErrorList collects non-fatal phase errors while still allowing later
// phases to run.
type stageErrorList struct {
	errs []error
}

func (el *stageErrorList) add(err error) {
	if err != nil {
		el.errs = append(el.errs, err)
	}
}

func (el *stageErrorList) errOrNil() error {
	if len(el.errs) == 0 {
		return nil
	}
	var parts []string
	for _, err := range el.errs {
		parts = append(parts, err.Error())
	}
	return fmt.Errorf("%s", strings.Join(parts, "; "))
}

// sweepStagingDirs removes per-tick staging directories left behind by an
// earlier process that was interrupted before downloads could be committed.
func sweepStagingDirs(logger log.Logger, baseDir string) {
	infos, err := os.ReadDir(baseDir)
	if err != nil {
		return
	}
	for _, info := range infos {
		if !info.IsDir() {
			continue
		}
		// os.MkdirTemp prefixes staging directories with "download".
		if !strings.HasPrefix(info.Name(), "download") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(baseDir, info.Name())); err != nil {
			logger.Error().LogErrorf("problem removing stale staging directory %s: %v", info.Name(), err)
		} else {
			logger.Logf("removed stale staging directory %s", info.Name())
		}
	}
}

// saveFilepaths will create files in dir for each file object provided
// The contents of each file struct will always be closed.
func saveFilepaths(ctx context.Context, logger log.Logger, agent upload.Agent, filepaths []string, dir string) error {
	var firstErr error
	var errordFilenames []string

	for i := range filepaths {
		// Skip directories that would create nested directories
		if filepath.Base(filepaths[i]) == filepath.Base(dir) {
			continue
		}
		// Create the path dir only when there is a real file to write
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("creating %s: %w", dir, err)
		}
		outPath := filepath.Join(dir, filepath.Base(filepaths[i]))
		f, err := os.Create(outPath)
		if err != nil {
			err = fmt.Errorf("os.create on %s failed: %w", outPath, err)
			if firstErr == nil {
				firstErr = err
			} else {
				logger.Error().LogError(err)
			}
			errordFilenames = append(errordFilenames, filepaths[i])
			continue
		}

		file, err := agent.ReadFile(ctx, filepaths[i])
		if err != nil {
			// Save the error if it's our first, otherwise log
			err = fmt.Errorf("reading %s failed: %w", filepaths[i], err)
			if firstErr == nil {
				firstErr = err
			} else {
				logger.Error().LogError(err)
			}
		}
		if file == nil || err != nil {
			// Record the failure and skip copy
			errordFilenames = append(errordFilenames, filepaths[i])
			f.Close()
			os.Remove(outPath)
			continue
		}
		if _, err = io.Copy(f, file.Contents); err != nil {
			err = fmt.Errorf("copying %s failed: %w", file.Filepath, err)
			if firstErr == nil {
				firstErr = err
			} else {
				logger.Error().LogError(err)
			}
			errordFilenames = append(errordFilenames, filepaths[i])
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync on %s failed: %w", file.Filepath, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close on %s failed: %w", file.Filepath, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("closing %s had problem: %w", file.Filepath, err)
		}
		logger.Logf("saved %s at %s", filepaths[i], outPath)
	}
	if len(errordFilenames) != 0 {
		return fmt.Errorf("saveFilepaths problem on: %s: %v", strings.Join(errordFilenames, ", "), firstErr)
	}
	return nil
}
