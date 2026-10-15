// Copyright 2020 The Moov Authors
// Use of this source code is governed by an Apache License
// license that can be found in the LICENSE file.

package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
)

// ErrRemoteContentConflict is returned when an object already exists at the
// target path with different content than expected. Callers must not overwrite
// or delete the remote object: money may already have moved from it.
var ErrRemoteContentConflict = errors.New("remote object exists with different content")

// RemotePresence is the outcome of verifying a target path against an expected
// content digest after an upload whose server response was ambiguous.
type RemotePresence int

const (
	// RemoteMissing means no object exists at the target path, so the upload
	// still needs to happen.
	RemoteMissing RemotePresence = iota
	// RemoteMatches means an object exists at the target path and its SHA-256
	// equals the expected digest — the server received our bytes even though
	// the upload call reported an error. No second upload is allowed.
	RemoteMatches
	// RemoteMismatch means an object exists at the target path with different
	// content. The batch must stay open and surface ErrRemoteContentConflict.
	RemoteMismatch
)

// VerifyRemoteFile reads the object at remotePath through agent and compares
// its SHA-256 digest with wantSHA256 (lowercase hex). ReadFile errors that
// unambiguously indicate a missing object resolve to RemoteMissing; every
// other read failure is returned so the caller keeps the upload unconfirmed
// and retries on the next recovery pass.
//
// remotePath must be the full remote path (OutboundPath joined with the
// rendered filename), matching the path UploadFile writes to.
func VerifyRemoteFile(ctx context.Context, agent Agent, remotePath, wantSHA256 string) (RemotePresence, error) {
	f, err := agent.ReadFile(ctx, remotePath)
	if err != nil {
		if isRemoteNotExist(err) {
			return RemoteMissing, nil
		}
		return 0, fmt.Errorf("reading remote %s for verification: %w", remotePath, err)
	}
	if f == nil {
		// Defensive: agents should return an error for missing objects, but
		// treat a nil/empty response the same way.
		return RemoteMissing, nil
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f.Contents); err != nil {
		return 0, fmt.Errorf("hashing remote object %s: %w", remotePath, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if strings.EqualFold(got, strings.TrimSpace(wantSHA256)) {
		return RemoteMatches, nil
	}
	return RemoteMismatch, nil
}

// isRemoteNotExist classifies "object not found" errors from FTP and SFTP
// clients, which surface different concrete error types. A marker that does
// not match leaves the outcome unknown (the caller must not assume missing).
func isRemoteNotExist(err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"no such file", // SFTP StatusError: "no such file" (SSH_FX_NO_SUCH_FILE)
		"does not exist",
		"not found",
		"550", // FTP reply 550: file unavailable / not found
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
