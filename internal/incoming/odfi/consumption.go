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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// consumptionDirName is the directory under the ODFI storage directory
	// which holds durable, per-file consumption state.
	consumptionDirName = "consumption"

	consumptionDataFile  = "data"
	consumptionStateFile = "state.json"
	consumptionErrorFile = "error.txt"
)

// Stage statuses recorded for each phase of consuming a remote file.
const (
	stageConfirmed stageStatus = "confirmed"
	stageFailed    stageStatus = "failed"
	stageSkipped   stageStatus = "skipped"
	stageUncertain stageStatus = "uncertain"
)

type stageStatus string

// fileStage tracks a single phase of file consumption.
type fileStage struct {
	// Status is empty when the stage has not been attempted.
	Status stageStatus `json:"status,omitempty"`

	// At is when the stage reached its current status.
	At *time.Time `json:"at,omitempty"`

	// Detail records stage specific information, e.g. the audit trail path,
	// a tolerated parse error, or the failure cause.
	Detail string `json:"detail,omitempty"`
}

func (s fileStage) terminal() bool {
	return s.Status == stageConfirmed || s.Status == stageSkipped
}

// processorStage tracks the outcome of running one FileProcessor on a file.
type processorStage struct {
	Status stageStatus `json:"status,omitempty"`
	At     *time.Time  `json:"at,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// consumptionState is the durable record of one remote file's consumption.
type consumptionState struct {
	// ID is the stable consumption identity derived from host, remote path
	// and the downloaded content digest.
	ID string `json:"id"`

	Host       string `json:"host"`
	RemotePath string `json:"remote_path"`
	KindDir    string `json:"kind_dir"`
	Filename   string `json:"filename"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Downloaded    fileStage                 `json:"downloaded"`
	Validated     fileStage                 `json:"validated"`
	Audited       fileStage                 `json:"audited"`
	Processors    map[string]processorStage `json:"processors,omitempty"`
	RemoteDeleted fileStage                 `json:"remote_deleted"`

	// ProcessorsExpected records the processor types observed the first time
	// the file was processed and gates when remote deletion may happen.
	ProcessorsExpected []string `json:"processors_expected,omitempty"`

	// LastError is the most recent failure encountered while consuming the file.
	LastError string `json:"last_error,omitempty"`
}

// consumptionUnit is one remote file's on-disk consumption slot.
type consumptionUnit struct {
	store *consumptionStore
	dir   string
	state consumptionState
}

func (u *consumptionUnit) lock()            { u.store.lock(u.state.ID) }
func (u *consumptionUnit) unlock()          { u.store.unlock(u.state.ID) }
func (u *consumptionUnit) dataPath() string { return filepath.Join(u.dir, consumptionDataFile) }

// complete reports whether every stage, including remote deletion, reached
// a terminal status.
func (u *consumptionUnit) complete() bool {
	return u.state.RemoteDeleted.terminal()
}

// failed reports whether any stage is currently failed or uncertain.
func (u *consumptionUnit) failed() bool {
	if u.state.Validated.Status == stageFailed ||
		u.state.Audited.Status == stageFailed ||
		u.state.RemoteDeleted.Status == stageFailed ||
		u.state.RemoteDeleted.Status == stageUncertain {
		return true
	}
	for _, p := range u.state.Processors {
		if p.Status == stageFailed {
			return true
		}
	}
	return false
}

// eligibleForRemoteDelete reports whether the file was downloaded, validated,
// audited and handled by every expected processor so that its remote copy may
// be removed.
func (u *consumptionUnit) eligibleForRemoteDelete() bool {
	if u.state.Downloaded.Status != stageConfirmed ||
		u.state.Validated.Status != stageConfirmed ||
		!u.state.Audited.terminal() ||
		u.state.RemoteDeleted.terminal() {
		return false
	}
	for _, name := range u.state.ProcessorsExpected {
		p, ok := u.state.Processors[name]
		if !ok || !p.statusTerminal() {
			return false
		}
	}
	for _, p := range u.state.Processors {
		if !p.statusTerminal() {
			return false
		}
	}
	return true
}

func (p processorStage) statusTerminal() bool {
	return p.Status == stageConfirmed || p.Status == stageSkipped
}

// readData returns the durable local copy of the downloaded file.
func (u *consumptionUnit) readData() ([]byte, error) {
	bs, err := os.ReadFile(u.dataPath())
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", consumptionDataFile, err)
	}
	return bs, nil
}

func (u *consumptionUnit) touch() {
	u.state.UpdatedAt = time.Now()
}

// persist atomically writes the unit state and, when errMsg is non-empty, the
// failure evidence file.
func (u *consumptionUnit) persist(errMsg string) error {
	u.touch()
	if errMsg != "" {
		u.state.LastError = errMsg
		path := filepath.Join(u.dir, consumptionErrorFile)
		if err := os.WriteFile(path, []byte(errMsg+"\n"), 0600); err != nil {
			return fmt.Errorf("writing error evidence: %w", err)
		}
	}

	bs, err := json.MarshalIndent(u.state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling consumption state: %w", err)
	}
	tmp, err := os.CreateTemp(u.dir, ".state.json.*")
	if err != nil {
		return fmt.Errorf("creating consumption state temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(bs); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("writing consumption state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("syncing consumption state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("closing consumption state: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(u.dir, consumptionStateFile)); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("committing consumption state: %w", err)
	}
	syncDir(u.dir)
	return nil
}

// consumptionStore owns the durable consumption directory for one ODFI
// storage configuration.
type consumptionStore struct {
	root string

	musLock sync.Mutex
	mus     map[string]*sync.Mutex
}

// newConsumptionStore opens (creating if needed) the durable consumption
// directory under baseDir.
func newConsumptionStore(baseDir string) (*consumptionStore, error) {
	root := filepath.Join(baseDir, consumptionDirName)
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, fmt.Errorf("creating %s: %w", root, err)
	}
	return &consumptionStore{
		root: root,
		mus:  make(map[string]*sync.Mutex),
	}, nil
}

func (s *consumptionStore) lock(id string) {
	s.musLock.Lock()
	mu, ok := s.mus[id]
	if !ok {
		mu = &sync.Mutex{}
		s.mus[id] = mu
	}
	s.musLock.Unlock()
	mu.Lock()
}

func (s *consumptionStore) unlock(id string) {
	s.musLock.Lock()
	mu, ok := s.mus[id]
	s.musLock.Unlock()
	if ok {
		mu.Unlock()
	}
}

func normalizeRemotePath(path string) string {
	return strings.TrimPrefix(filepath.ToSlash(filepath.Clean(filepath.ToSlash(path))), "/")
}

// consumptionID builds the stable identity of a remote file from its host,
// normalized remote path and downloaded content digest.
func consumptionID(host, remotePath, digest string) string {
	sum := sha256.Sum256([]byte(host + "\x00" + normalizeRemotePath(remotePath) + "\x00" + digest))
	return hex.EncodeToString(sum[:])
}

// hostDir returns the on-disk directory used for one remote host. QueryEscape
// keeps separators such as the host:port colon out of directory names.
func (s *consumptionStore) hostDir(host string) string {
	return filepath.Join(s.root, url.QueryEscape(host))
}

func (s *consumptionStore) slotDir(host, remotePath, digest string) string {
	return filepath.Join(s.hostDir(host), filepath.FromSlash(normalizeRemotePath(remotePath)), digest)
}

// CommitDownload promotes a fully downloaded staging file into a durable
// consumption slot. It is idempotent for an identical
// (host, remotePath, content) tuple. The staging file is removed on success.
func (s *consumptionStore) CommitDownload(host, remotePath, kindDir, stagingPath string) (*consumptionUnit, error) {
	remotePath = normalizeRemotePath(remotePath)

	f, err := os.Open(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("opening staged download %s: %w", stagingPath, err)
	}
	h := sha256.New()
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stating staged download %s: %w", stagingPath, err)
	}
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, fmt.Errorf("hashing staged download %s: %w", stagingPath, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing staged download %s: %w", stagingPath, err)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	id := consumptionID(host, remotePath, digest)

	s.lock(id)
	defer s.unlock(id)

	dir := s.slotDir(host, remotePath, digest)
	if existing, err := s.loadLocked(dir); err == nil {
		// Same content was downloaded and committed before - reuse it.
		os.Remove(stagingPath)
		return existing, nil
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("creating consumption directory %s: %w", dir, err)
	}
	dataPath := filepath.Join(dir, consumptionDataFile)
	if err := os.Rename(stagingPath, dataPath); err != nil {
		// Fall back to a copy in case the rename crossed filesystems.
		if copyErr := copyFile(stagingPath, dataPath); copyErr != nil {
			return nil, fmt.Errorf("committing download to %s: %w", dataPath, err)
		}
		os.Remove(stagingPath)
	}
	syncDir(dir)

	now := time.Now()
	u := &consumptionUnit{
		store: s,
		dir:   dir,
		state: consumptionState{
			ID:         id,
			Host:       host,
			RemotePath: remotePath,
			KindDir:    kindDir,
			Filename:   filepath.Base(remotePath),
			SHA256:     digest,
			Size:       info.Size(),
			CreatedAt:  now,
			UpdatedAt:  now,
			Processors: make(map[string]processorStage),
			Downloaded: fileStage{Status: stageConfirmed, At: &now},
		},
	}
	if err := u.persist(""); err != nil {
		return nil, err
	}
	return u, nil
}

// loadLocked reads a slot directory. The caller must hold the unit lock when
// mutating the returned unit; loadLocked itself performs no locking.
func (s *consumptionStore) loadLocked(dir string) (*consumptionUnit, error) {
	bs, err := os.ReadFile(filepath.Join(dir, consumptionStateFile))
	if err != nil {
		return nil, err
	}
	var state consumptionState
	if err := json.Unmarshal(bs, &state); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", consumptionStateFile, err)
	}
	if state.Processors == nil {
		state.Processors = make(map[string]processorStage)
	}
	return &consumptionUnit{
		store: s,
		dir:   dir,
		state: state,
	}, nil
}

// listUnits walks the durable store and returns every stored unit.
func (s *consumptionStore) listUnits(host string) ([]*consumptionUnit, error) {
	root := s.root
	if host != "" {
		root = s.hostDir(host)
	}
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []*consumptionUnit
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || d.Name() != consumptionStateFile {
			return nil
		}
		u, err := s.loadLocked(filepath.Dir(path))
		if err != nil {
			return err
		}
		if host == "" || u.state.Host == host {
			out = append(out, u)
		}
		return nil
	})
	return out, err
}

// ListIncomplete returns stored units that still have unfinished stages for a
// host so processing can resume after a restart or failed attempt.
func (s *consumptionStore) ListIncomplete(host string) ([]*consumptionUnit, error) {
	units, err := s.listUnits(host)
	if err != nil {
		return nil, err
	}
	var out []*consumptionUnit
	for _, u := range units {
		if !u.complete() {
			out = append(out, u)
		}
	}
	return out, nil
}

// Results returns the durable state of every stored unit, optionally scoped to
// a single consumption ID.
func (s *consumptionStore) Results(id string) ([]consumptionState, error) {
	units, err := s.listUnits("")
	if err != nil {
		return nil, err
	}
	out := make([]consumptionState, 0)
	for _, u := range units {
		if id != "" && u.state.ID != id {
			continue
		}
		out = append(out, u.state)
	}
	return out, nil
}

// RemoveComplete removes slot directories of fully consumed units for a host
// and prunes the empty directories left behind. Failed units and their
// evidence are always retained.
func (s *consumptionStore) RemoveComplete(host string) error {
	root := s.hostDir(host)
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// First collect completed slot directories and all visited directories.
	// Filesystem mutations happen outside the walk callback to avoid racing
	// with concurrently changing directory contents.
	var completeUnits []string
	var pathDirs []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if path != root {
				pathDirs = append(pathDirs, path)
			}
			if _, statErr := os.Stat(filepath.Join(path, consumptionStateFile)); statErr == nil {
				u, loadErr := s.loadLocked(path)
				if loadErr != nil {
					return loadErr
				}
				if u.complete() && !u.failed() {
					completeUnits = append(completeUnits, path)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove completed slot directories.
	for _, dir := range completeUnits {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("removing consumed unit %s: %w", dir, err)
		}
	}

	// Prune directories which are now empty (deepest first).
	for i := len(pathDirs) - 1; i >= 0; i-- {
		infos, readErr := os.ReadDir(pathDirs[i])
		if readErr != nil || len(infos) > 0 {
			continue
		}
		os.Remove(pathDirs[i])
	}
	return nil
}

// markValidated records the validation outcome. A tolerated parse error
// (missing ACH FileHeader) still leaves the stage confirmed so batches can be
// processed, matching historical behavior.
func (u *consumptionUnit) markValidated(toleratedErr error) error {
	u.lock()
	defer u.unlock()
	now := time.Now()
	stage := fileStage{Status: stageConfirmed, At: &now}
	if toleratedErr != nil {
		stage.Detail = toleratedErr.Error()
	}
	u.state.Validated = stage
	u.state.LastError = ""
	return u.persist("")
}

// failValidation records a hard parse failure and preserves the evidence.
func (u *consumptionUnit) failValidation(parseErr error) error {
	return u.failStage(func(msg string) {
		now := time.Now()
		u.state.Validated = fileStage{Status: stageFailed, At: &now, Detail: msg}
	}, parseErr)
}

func (u *consumptionUnit) failAudit(auditErr error) error {
	return u.failStage(func(msg string) {
		now := time.Now()
		u.state.Audited = fileStage{Status: stageFailed, At: &now, Detail: msg}
	}, auditErr)
}

// markAudited records a successful audit trail save.
func (u *consumptionUnit) markAudited(auditPath string) error {
	u.lock()
	defer u.unlock()
	now := time.Now()
	u.state.Audited = fileStage{Status: stageConfirmed, At: &now, Detail: auditPath}
	u.state.LastError = ""
	return u.persist("")
}

// skipAudited records that no audit save was required (empty content or no
// audit storage).
func (u *consumptionUnit) skipAudited(reason string) error {
	u.lock()
	defer u.unlock()
	now := time.Now()
	u.state.Audited = fileStage{Status: stageSkipped, At: &now, Detail: reason}
	u.state.LastError = ""
	return u.persist("")
}

// setExpectedProcessors records which processor types participate in handling
// the file.
func (u *consumptionUnit) setExpectedProcessors(names []string) error {
	u.lock()
	defer u.unlock()
	if len(u.state.ProcessorsExpected) == 0 && len(names) > 0 {
		u.state.ProcessorsExpected = append(u.state.ProcessorsExpected, names...)
		return u.persist("")
	}
	return nil
}

// processorConfirmed reports whether a processor previously completed.
func (u *consumptionUnit) processorConfirmed(name string) bool {
	p, ok := u.state.Processors[name]
	return ok && p.statusTerminal()
}

// markProcessorConfirmed records that a processor handled the file without
// error. Recovering runs skip confirmed processors and never resend events.
func (u *consumptionUnit) markProcessorConfirmed(name string) error {
	u.lock()
	defer u.unlock()
	now := time.Now()
	u.state.Processors[name] = processorStage{Status: stageConfirmed, At: &now}
	u.state.LastError = ""
	return u.persist("")
}

// markProcessorFailed records a processor error and preserves evidence so the
// remote file is retained and only this processor reruns on recovery.
func (u *consumptionUnit) markProcessorFailed(name string, handleErr error) error {
	return u.failStage(func(msg string) {
		now := time.Now()
		u.state.Processors[name] = processorStage{Status: stageFailed, At: &now, Error: msg}
	}, handleErr)
}

// markRemoteDeleted records the outcome of removing the remote file. Only
// uncertain outcomes are treated as failure evidence.
func (u *consumptionUnit) markRemoteDeleted(status stageStatus, detail string) error {
	u.lock()
	defer u.unlock()
	now := time.Now()
	u.state.RemoteDeleted = fileStage{Status: status, At: &now, Detail: detail}
	if status == stageConfirmed || status == stageSkipped {
		u.state.LastError = ""
	}
	evidence := ""
	if status == stageUncertain || status == stageFailed {
		evidence = detail
	}
	return u.persist(evidence)
}

func (u *consumptionUnit) failStage(set func(msg string), err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	u.lock()
	defer u.unlock()
	set(msg)
	return u.persist(msg)
}

// syncDir fsyncs a directory so that recently renamed or created entries are
// durable. Failures on platforms without directory fsync are ignored.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// copyFile copies src to dst and fsyncs the destination.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
