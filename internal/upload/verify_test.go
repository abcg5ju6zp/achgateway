// Copyright 2020 The Moov Authors
// Use of this source code is governed by an Apache License
// license that can be found in the LICENSE file.

package upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// verifyStubAgent embeds MockAgent so only the methods verification touches
// need overriding. It also records write attempts that must never happen.
type verifyStubAgent struct {
	MockAgent

	readFile *File
	readErr  error

	uploadCount int
	deleteCount int
}

func (a *verifyStubAgent) ReadFile(_ context.Context, _ string) (*File, error) {
	return a.readFile, a.readErr
}

func (a *verifyStubAgent) UploadFile(_ context.Context, f File) error {
	a.uploadCount++
	io.Copy(io.Discard, f.Contents)
	return nil
}

func (a *verifyStubAgent) Delete(_ context.Context, _ string) error {
	a.deleteCount++
	return nil
}

func TestVerifyRemoteFile(t *testing.T) {
	payload := []byte("001 ach outbound payload")
	sum := sha256.Sum256(payload)
	want := hex.EncodeToString(sum[:])

	const path = "outbound/20261014-150000-987654320-0.ach"

	tests := []struct {
		name    string
		file    *File
		readErr error
		want    RemotePresence
		wantErr bool
	}{
		{
			name: "matches - server kept identical bytes despite upload error",
			file: &File{Contents: io.NopCloser(bytes.NewReader(payload))},
			want: RemoteMatches,
		},
		{
			name:    "missing - SFTP no such file",
			readErr: errors.New(`sftp open outbound/x failed: sftp: "no such file" (SSH_FX_NO_SUCH_FILE)`),
			want:    RemoteMissing,
		},
		{
			name:    "missing - FTP 550",
			readErr: errors.New("ftp open outbound/x failed: ftp: 550 Failed to open file"),
			want:    RemoteMissing,
		},
		{
			name:    "read EOF is unclassified - stay unconfirmed",
			readErr: io.EOF,
			wantErr: true,
		},
		{
			name:    "connection error is unclassified - stay unconfirmed",
			readErr: errors.New("connection reset by peer"),
			wantErr: true,
		},
		{
			name: "mismatch - different bytes at target path",
			file: &File{Contents: io.NopCloser(bytes.NewReader([]byte("different content")))},
			want: RemoteMismatch,
		},
		{
			name: "nil file treated as missing",
			file: nil,
			want: RemoteMissing,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agent := &verifyStubAgent{readFile: tc.file, readErr: tc.readErr}

			got, err := VerifyRemoteFile(context.Background(), agent, path, want)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}

			// Verification is strictly read-only.
			require.Zero(t, agent.uploadCount, "verification must never upload")
			require.Zero(t, agent.deleteCount, "verification must never delete")
		})
	}
}

func TestIsRemoteNotExist(t *testing.T) {
	require.True(t, isRemoteNotExist(errors.New("sftp: \"no such file\" (SSH_FX_NO_SUCH_FILE)")))
	require.True(t, isRemoteNotExist(errors.New("ftp: bad response: 550 File not found")))
	require.False(t, isRemoteNotExist(errors.New("connection refused")))
	require.False(t, isRemoteNotExist(io.EOF))
}
