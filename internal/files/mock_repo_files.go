package files

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// MockRepository is an in-memory Repository implementing the same monotonic
// stage machine as the SQL and Spanner backends. Its zero value is ready to
// use.
type MockRepository struct {
	mu  sync.Mutex
	Err error

	files      map[string]AcceptedFile
	receptions map[string]Reception

	// RecordFileHook is consulted when writing the file index, letting tests
	// fail an index write without breaking the reception ledger.
	RecordFileHook func(file AcceptedFile) error
}

// NewMockRepository returns an initialized in-memory Repository.
func NewMockRepository() Repository {
	return &MockRepository{}
}

func (m *MockRepository) init() {
	if m.files == nil {
		m.files = make(map[string]AcceptedFile)
	}
	if m.receptions == nil {
		m.receptions = make(map[string]Reception)
	}
}

func duplicateEntryErr() error {
	// Keep the same shape as MySQL 1062 so database.UniqueViolation recognizes it.
	return fmt.Errorf("Error 1062: Duplicate entry")
}

func (m *MockRepository) Record(_ context.Context, file AcceptedFile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	if m.RecordFileHook != nil {
		if err := m.RecordFileHook(file); err != nil {
			return err
		}
	}
	m.init()
	if _, exists := m.files[file.FileID]; exists {
		return duplicateEntryErr()
	}
	m.files[file.FileID] = file
	return nil
}

func (m *MockRepository) Cleanup(_ context.Context, file AcceptedFile) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	delete(m.files, file.FileID)
	return nil
}

func (m *MockRepository) Cancel(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.init()
	file, exists := m.files[fileID]
	if !exists || file.PublishedAt == nil || !file.CanceledAt.IsZero() {
		return nil
	}
	file.CanceledAt = time.Now().In(time.UTC)
	m.files[fileID] = file
	return nil
}

func (m *MockRepository) GetFile(_ context.Context, fileID string) (*AcceptedFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	m.init()
	file, exists := m.files[fileID]
	if !exists {
		return nil, fmt.Errorf("getting file %s: %w", fileID, sql.ErrNoRows)
	}
	out := file
	return &out, nil
}

func (m *MockRepository) MarkFilePublished(_ context.Context, fileID string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.init()
	file, exists := m.files[fileID]
	if !exists {
		return fmt.Errorf("marking file published: %w", sql.ErrNoRows)
	}
	if file.PublishedAt == nil {
		ts := at
		file.PublishedAt = &ts
		m.files[fileID] = file
	}
	return nil
}

func (m *MockRepository) ListPublishedFileIDs(_ context.Context) (map[string]struct{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	m.init()
	out := make(map[string]struct{})
	for fileID, file := range m.files {
		if file.PublishedAt != nil {
			out[fileID] = struct{}{}
		}
	}
	return out, nil
}

func (m *MockRepository) RecordReception(_ context.Context, reception Reception) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.init()
	if _, exists := m.receptions[reception.CommitID]; exists {
		// Idempotent: the first writer's row is authoritative.
		return nil
	}
	now := time.Now().In(time.UTC)
	if reception.AcceptedAt.IsZero() {
		reception.AcceptedAt = now
	}
	if reception.UpdatedAt.IsZero() {
		reception.UpdatedAt = now
	}
	reception.Stage = StageReceived
	m.receptions[reception.CommitID] = reception
	return nil
}

func (m *MockRepository) RecordPublishedReception(_ context.Context, reception Reception) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.init()
	if _, exists := m.receptions[reception.CommitID]; exists {
		return nil
	}
	now := time.Now().In(time.UTC)
	if reception.AcceptedAt.IsZero() {
		reception.AcceptedAt = now
	}
	if reception.UpdatedAt.IsZero() {
		reception.UpdatedAt = now
	}
	ts := now
	reception.Stage = StagePublished
	if reception.PublishedAt == nil {
		reception.PublishedAt = &ts
	}
	m.receptions[reception.CommitID] = reception
	return nil
}

func (m *MockRepository) GetReception(_ context.Context, commitID string) (*Reception, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	m.init()
	reception, exists := m.receptions[commitID]
	if !exists {
		return nil, fmt.Errorf("%s: %w", commitID, ErrReceptionNotFound)
	}
	out := reception
	return &out, nil
}

// lockedReception returns the reception for commitID. Callers hold m.mu.
func (m *MockRepository) lockedReception(commitID string) (Reception, error) {
	reception, exists := m.receptions[commitID]
	if !exists {
		return Reception{}, fmt.Errorf("%s: %w", commitID, ErrReceptionNotFound)
	}
	return reception, nil
}

// guardStage mirrors the guarded SQL UPDATEs: rows in allowedStages advance,
// rows already beyond wantStage are a no-op, and earlier stages are errors.
// mutate is only called for rows inside allowedStages.
func (m *MockRepository) guardStage(commitID, wantStage string, allowedStages []string, mutate func(*Reception)) error {
	reception, err := m.lockedReception(commitID)
	if err != nil {
		return err
	}
	allowed := false
	for _, stage := range allowedStages {
		if reception.Stage == stage {
			allowed = true
			break
		}
	}
	switch {
	case allowed:
		mutate(&reception)
		m.receptions[commitID] = reception
		return nil
	case stageRank(reception.Stage) >= stageRank(wantStage):
		return nil
	default:
		return fmt.Errorf("transition to %s: %w (stage=%s)", wantStage, ErrInvalidStage, reception.Stage)
	}
}

func (m *MockRepository) MarkStaged(_ context.Context, commitID, contentSHA256 string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	return m.guardStage(commitID, StageStaged, []string{StageReceived, StageStaged}, func(r *Reception) {
		if r.Stage != StageStaged {
			r.Stage = StageStaged
			ts := at
			r.StagedAt = &ts
			r.ContentSHA256 = contentSHA256
			r.UpdatedAt = at
		}
	})
}

func (m *MockRepository) MarkIndexed(_ context.Context, commitID string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	return m.guardStage(commitID, StageIndexed, []string{StageStaged, StageIndexed}, func(r *Reception) {
		if r.Stage == StageStaged {
			r.Stage = StageIndexed
			r.UpdatedAt = at
		}
	})
}

func (m *MockRepository) MarkPublished(_ context.Context, commitID, fileID string, resultJSON []byte, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.init()
	file, exists := m.files[fileID]
	if !exists {
		return fmt.Errorf("marking file published: %w", sql.ErrNoRows)
	}

	return m.guardStage(commitID, StagePublished, []string{StageStaged, StageIndexed, StagePublished}, func(r *Reception) {
		ts := at
		if file.PublishedAt == nil {
			file.PublishedAt = &ts
			m.files[fileID] = file
		}
		if r.Stage != StagePublished {
			r.Stage = StagePublished
			r.PublishedAt = &ts
			r.ResultJSON = append([]byte(nil), resultJSON...)
			r.UpdatedAt = at
		}
	})
}

func (m *MockRepository) MarkNotified(_ context.Context, commitID string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	reception, err := m.lockedReception(commitID)
	if err != nil {
		return err
	}
	if reception.Stage != StagePublished && reception.Stage != StageFailed {
		return fmt.Errorf("mark notified: %w (stage=%s)", ErrInvalidStage, reception.Stage)
	}
	if reception.NotifiedAt == nil {
		ts := at
		reception.NotifiedAt = &ts
		reception.UpdatedAt = at
		m.receptions[commitID] = reception
	}
	return nil
}

func (m *MockRepository) MarkFailed(_ context.Context, commitID, failure string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	reception, err := m.lockedReception(commitID)
	if err != nil {
		return err
	}
	if reception.Stage == StageFailed {
		return nil
	}
	reception.Stage = StageFailed
	reception.LastError = failure
	reception.UpdatedAt = at
	m.receptions[commitID] = reception
	return nil
}

func (m *MockRepository) MarkFailedWithResult(_ context.Context, commitID, failure string, resultJSON []byte, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	reception, err := m.lockedReception(commitID)
	if err != nil {
		return err
	}
	if reception.Stage != StageFailed {
		reception.Stage = StageFailed
	}
	reception.LastError = failure
	reception.ResultJSON = append([]byte(nil), resultJSON...)
	reception.UpdatedAt = at
	m.receptions[commitID] = reception
	return nil
}

func (m *MockRepository) RequestCancellation(_ context.Context, commitID string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	reception, err := m.lockedReception(commitID)
	if err != nil {
		return err
	}
	if reception.CancelRequestedAt == nil {
		ts := at
		reception.CancelRequestedAt = &ts
		reception.UpdatedAt = at
		m.receptions[commitID] = reception
	}
	return nil
}

func (m *MockRepository) ListPendingNotifications(_ context.Context) ([]Reception, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	m.init()
	var out []Reception
	for _, reception := range m.receptions {
		if (reception.Stage == StagePublished || reception.Stage == StageFailed) && reception.NotifiedAt == nil {
			out = append(out, reception)
		}
	}
	return out, nil
}

func (m *MockRepository) ListUnfinishedReceptions(_ context.Context) ([]Reception, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	m.init()
	var out []Reception
	for _, reception := range m.receptions {
		switch reception.Stage {
		case StageReceived, StageStaged, StageIndexed:
			out = append(out, reception)
		case StagePublished:
			if reception.NotifiedAt == nil {
				out = append(out, reception)
			}
		}
	}
	return out, nil
}
