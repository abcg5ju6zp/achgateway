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

	"github.com/moov-io/ach"
	"github.com/moov-io/achgateway/internal/incoming"
	"github.com/moov-io/achgateway/internal/upload"
)

type MockXferMerging struct {
	LatestFile           *incoming.ACHFile
	LatestCancel         *incoming.CancelACHFile
	CancellationResponse incoming.FileCancellationResponse
	merged               mergedFiles

	Err error

	// Staging/publish hooks used by the staged reception state machine.
	StageErr           error
	PublishErr         error
	CleanupErr         error
	ListStagedErr      error
	CancelIntentErr    error
	LatestCancelIntent *incoming.CancelACHFile

	StagedHash   string
	stagedFiles  map[string]*incoming.ACHFile
	published    map[string]*incoming.ACHFile
	cleanupCalls []string
	stagedList   []StagedCommit
}

func (merge *MockXferMerging) HandleXfer(_ context.Context, xfer incoming.ACHFile) error {
	merge.LatestFile = &xfer
	return merge.Err
}

func (merge *MockXferMerging) StageXfer(_ context.Context, xfer incoming.ACHFile, commitID string) (string, error) {
	if merge.StageErr != nil {
		return "", merge.StageErr
	}
	if merge.stagedFiles == nil {
		merge.stagedFiles = make(map[string]*incoming.ACHFile)
	}
	f := xfer
	merge.stagedFiles[commitID] = &f
	return merge.StagedHash, nil
}

func (merge *MockXferMerging) PublishXfer(_ context.Context, xfer incoming.ACHFile, commitID string) error {
	if merge.PublishErr != nil {
		return merge.PublishErr
	}
	if merge.published == nil {
		merge.published = make(map[string]*incoming.ACHFile)
	}
	f := xfer
	merge.published[commitID] = &f
	delete(merge.stagedFiles, commitID)
	return nil
}

func (merge *MockXferMerging) CleanupStaging(_ context.Context, commitID string) error {
	merge.cleanupCalls = append(merge.cleanupCalls, commitID)
	delete(merge.stagedFiles, commitID)
	return merge.CleanupErr
}

func (merge *MockXferMerging) CompletePendingSidecar(_ context.Context, _ incoming.ACHFile, _ string) error {
	return nil
}

func (merge *MockXferMerging) ListStaged(_ context.Context) ([]StagedCommit, error) {
	if merge.ListStagedErr != nil {
		return nil, merge.ListStagedErr
	}
	return merge.stagedList, nil
}

// StagedCommitIDs returns the commitIDs currently held in staging.
func (merge *MockXferMerging) StagedCommitIDs() []string {
	var out []string
	for id := range merge.stagedFiles {
		out = append(out, id)
	}
	return out
}

// PublishedCommitIDs returns the commitIDs that reached PublishXfer.
func (merge *MockXferMerging) PublishedCommitIDs() []string {
	var out []string
	for id := range merge.published {
		out = append(out, id)
	}
	return out
}

func (merge *MockXferMerging) RecordCancelIntent(_ context.Context, cancel incoming.CancelACHFile) error {
	c := cancel
	merge.LatestCancelIntent = &c
	return merge.CancelIntentErr
}

// SetStagedList primes what ListStaged returns (recovery tests).
func (merge *MockXferMerging) SetStagedList(staged []StagedCommit) {
	merge.stagedList = staged
}

func (merge *MockXferMerging) HandleCancel(_ context.Context, cancel incoming.CancelACHFile) (incoming.FileCancellationResponse, error) {
	merge.LatestCancel = &cancel
	return merge.CancellationResponse, merge.Err
}

func (merge *MockXferMerging) WithEachMerged(ctx context.Context, f func(context.Context, int, upload.Agent, *ach.File) (string, error)) (mergedFiles, error) {
	// Match production: partial merged results may accompany an error so
	// callers can still emit FileUploaded for what succeeded.
	return merge.merged, merge.Err
}
