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
	"os"
	"path/filepath"
	"strings"

	"github.com/moov-io/achgateway/internal/upload"
	"github.com/moov-io/base"
	"github.com/moov-io/base/log"
	"github.com/moov-io/base/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Cleanup deletes files on remote servers if enabled via config. Only files
// whose download, validation, audit and processor stages are all confirmed
// are deleted; unfinished and failed files remain on the remote server.
func Cleanup(ctx context.Context, logger log.Logger, agent upload.Agent, dl *downloadedFiles) error {
	units, err := dl.allUnits()
	if err != nil {
		return err
	}

	var el base.ErrorList
	for _, u := range units {
		if u.state.RemoteDeleted.terminal() || !u.eligibleForRemoteDelete() {
			continue
		}
		if err := deleteRemoteUnit(ctx, logger, agent, u); err != nil {
			el.Add(err)
		}
	}
	if el.Empty() {
		return nil
	}
	return el
}

// CleanupEmptyFiles deletes empty ACH files if file is older than value in config.
func CleanupEmptyFiles(ctx context.Context, logger log.Logger, agent upload.Agent, dl *downloadedFiles) error {
	units, err := dl.allUnits()
	if err != nil {
		return err
	}

	var el base.ErrorList
	for _, u := range units {
		if u.state.Size != 0 || u.state.RemoteDeleted.terminal() {
			continue
		}
		// Only remove zero byte files which have finished processing.
		if !u.eligibleForRemoteDelete() {
			continue
		}
		if err := deleteRemoteUnit(ctx, logger, agent, u); err != nil {
			el.Add(err)
		}
	}
	if el.Empty() {
		return nil
	}
	return el
}

// markRemoteFilesKept records remote retention for units which are not subject
// to any remote deletion so their consumption reaches a terminal state while
// KeepRemoteFiles is enabled.
func markRemoteFilesKept(dl *downloadedFiles) error {
	units, err := dl.allUnits()
	if err != nil {
		return err
	}
	for _, u := range units {
		if u.state.RemoteDeleted.terminal() {
			continue
		}
		// Uncertain and unfinished units stay incomplete so a later tick can
		// finish or retry them.
		if u.state.RemoteDeleted.Status == stageUncertain || !u.eligibleForRemoteDelete() {
			continue
		}
		if err := u.markRemoteDeleted(stageSkipped, "remote files kept by configuration"); err != nil {
			return err
		}
	}
	return nil
}

// deleteRemoteUnit removes one remote file, distinguishing confirmed deletion
// from an uncertain response. When a Delete call times out or fails for an
// unknown reason the remote directory is queried before the outcome is
// recorded, so an actually-deleted file is not deleted or processed twice.
func deleteRemoteUnit(ctx context.Context, logger log.Logger, agent upload.Agent, u *consumptionUnit) error {
	ctx, span := telemetry.StartSpan(ctx, "odfi-delete-files-on-remote", trace.WithAttributes(
		attribute.String("achgateway.hostname", agent.Hostname()),
		attribute.String("achgateway.path", u.state.RemotePath),
	))
	defer span.End()

	// On recovery from an uncertain delete, confirm the remote state before
	// issuing another delete.
	if u.state.RemoteDeleted.Status == stageUncertain {
		exists, err := remoteFileExists(ctx, agent, u)
		if err != nil {
			detail := fmt.Sprintf("unable to determine remote state of %s after uncertain delete: %v", u.state.RemotePath, err)
			if recordErr := u.markRemoteDeleted(stageUncertain, detail); recordErr != nil {
				return recordErr
			}
			return fmt.Errorf("%s: %w", detail, err)
		}
		if !exists {
			logger.Logf("cleanup: remote file %s already absent, skipping delete", u.state.RemotePath)
			return u.markRemoteDeleted(stageConfirmed, "remote file absent when retrying uncertain delete")
		}
	}

	if err := agent.Delete(ctx, u.state.RemotePath); err != nil {
		// A missing file is a confirmed deletion.
		if os.IsNotExist(err) {
			logger.Logf("cleanup: remote file %s already gone", u.state.RemotePath)
			return u.markRemoteDeleted(stageConfirmed, fmt.Sprintf("remote file missing: %v", err))
		}

		// The delete outcome is unknown (timeout, dropped connection, ...).
		// Query the remote server to learn whether the delete actually happened.
		exists, listErr := remoteFileExists(ctx, agent, u)
		if listErr != nil {
			detail := fmt.Sprintf("delete %s failed (%v) and remote state is unavailable: %v", u.state.RemotePath, err, listErr)
			if recordErr := u.markRemoteDeleted(stageUncertain, detail); recordErr != nil {
				return recordErr
			}
			return fmt.Errorf("%s: %w", detail, listErr)
		}
		if !exists {
			logger.Logf("cleanup: delete of %s returned an error but the file is absent", u.state.RemotePath)
			return u.markRemoteDeleted(stageConfirmed, fmt.Sprintf("delete returned an error but the remote file is absent: %v", err))
		}

		detail := fmt.Sprintf("delete %s outcome uncertain, file still present: %v", u.state.RemotePath, err)
		if recordErr := u.markRemoteDeleted(stageUncertain, detail); recordErr != nil {
			return recordErr
		}
		return fmt.Errorf("%s: %w", detail, err)
	}

	logger.Logf("cleanup: deleted remote file %s", u.state.RemotePath)
	return u.markRemoteDeleted(stageConfirmed, "")
}

// remoteFileExists lists the containing remote directory and reports whether
// the unit's file is still present on the remote server.
func remoteFileExists(ctx context.Context, agent upload.Agent, u *consumptionUnit) (bool, error) {
	list, err := remoteLister(agent, u.state.KindDir)
	if err != nil {
		return false, err
	}
	paths, err := list(ctx)
	if err != nil {
		return false, fmt.Errorf("listing remote directory %s: %w", u.state.KindDir, err)
	}

	target := normalizeRemotePath(u.state.RemotePath)
	for _, p := range paths {
		np := normalizeRemotePath(p)
		switch {
		case np == target:
			return true, nil
		case filepath.Base(np) == u.state.Filename && filepath.Dir(np) == filepath.Dir(target):
			return true, nil
		}
	}
	return false, nil
}

// remoteLister returns the agent's listing function for the kind directory the
// file was downloaded from.
func remoteLister(agent upload.Agent, kindDir string) (func(context.Context) ([]string, error), error) {
	candidates := []struct {
		dir  string
		list func(context.Context) ([]string, error)
	}{
		{agent.InboundPath(), agent.GetInboundFiles},
		{agent.ReconciliationPath(), agent.GetReconciliationFiles},
		{agent.ReturnPath(), agent.GetReturnFiles},
	}
	for _, c := range candidates {
		if sameRemoteDir(c.dir, kindDir) {
			return c.list, nil
		}
	}
	return nil, fmt.Errorf("no remote listing for directory %q", kindDir)
}

// sameRemoteDir compares two remote directory paths tolerating leading or
// trailing slashes.
func sameRemoteDir(a, b string) bool {
	clean := func(p string) string {
		return filepath.ToSlash(filepath.Clean("/" + strings.TrimPrefix(filepath.ToSlash(p), "/")))
	}
	return clean(a) == clean(b)
}
