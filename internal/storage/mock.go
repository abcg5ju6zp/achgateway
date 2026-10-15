package storage

import (
	"io/fs"
)

// MockStorage wraps a Chest with per-method error injection. Every method is
// safe to use with a nil embedded Chest (returning zero values), which lets a
// test fault-inject one operation without standing up a real filesystem.
type MockStorage struct {
	Chest

	OpenErr error
	GlobErr error

	ReadDirErr error

	ReplaceFileErr error
	ReplaceDirErr  error
	MoveFileErr    error

	MkdirAllErr error
	RmdirAllErr error

	WriteFileErr error
}

func (m *MockStorage) Open(path string) (fs.File, error) {
	if m.OpenErr != nil {
		return nil, m.OpenErr
	}
	if m.Chest == nil {
		return nil, fs.ErrNotExist
	}
	return m.Chest.Open(path)
}

func (m *MockStorage) Glob(pattern string) ([]FileStat, error) {
	if m.GlobErr != nil {
		return nil, m.GlobErr
	}
	if m.Chest == nil {
		return nil, nil
	}
	return m.Chest.Glob(pattern)
}

func (m *MockStorage) ReadDir(name string) ([]fs.DirEntry, error) {
	if m.ReadDirErr != nil {
		return nil, m.ReadDirErr
	}
	if m.Chest == nil {
		return nil, fs.ErrNotExist
	}
	return m.Chest.ReadDir(name)
}

func (m *MockStorage) ReplaceFile(oldpath, newpath string) error {
	if m.ReplaceFileErr != nil {
		return m.ReplaceFileErr
	}
	if m.Chest != nil {
		return m.Chest.ReplaceFile(oldpath, newpath)
	}
	return nil
}

func (m *MockStorage) ReplaceDir(oldpath, newpath string) error {
	if m.ReplaceDirErr != nil {
		return m.ReplaceDirErr
	}
	if m.Chest != nil {
		return m.Chest.ReplaceDir(oldpath, newpath)
	}
	return nil
}

func (m *MockStorage) MoveFile(oldpath, newpath string) error {
	if m.MoveFileErr != nil {
		return m.MoveFileErr
	}
	if m.Chest != nil {
		return m.Chest.MoveFile(oldpath, newpath)
	}
	return nil
}

func (m *MockStorage) MkdirAll(path string) error {
	if m.MkdirAllErr != nil {
		return m.MkdirAllErr
	}
	if m.Chest != nil {
		return m.Chest.MkdirAll(path)
	}
	return nil
}

func (m *MockStorage) RmdirAll(path string) error {
	if m.RmdirAllErr != nil {
		return m.RmdirAllErr
	}
	if m.Chest != nil {
		return m.Chest.RmdirAll(path)
	}
	return nil
}

func (m *MockStorage) RemoveEmptyDir(path string) error {
	if m.Chest != nil {
		return m.Chest.RemoveEmptyDir(path)
	}
	return nil
}

func (m *MockStorage) WriteFile(path string, contents []byte) error {
	if m.WriteFileErr != nil {
		return m.WriteFileErr
	}
	if m.Chest != nil {
		return m.Chest.WriteFile(path, contents)
	}
	return nil
}
