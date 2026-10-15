package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type filesystem struct {
	root string
	fsys fs.FS
}

func NewFilesystem(root string) (Chest, error) {
	fullpath, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(fullpath, 0750); err != nil {
		return nil, err
	}
	return &filesystem{
		fsys: os.DirFS(fullpath),
		root: fullpath,
	}, nil
}

// invalidPath rejects the same escapes Open already blocked: parent
// segments and absolute paths. filepath.Join treats an absolute element
// as a new root, so WriteFile("/tmp/x") and WriteFile("../x") would
// otherwise land outside f.root.
func invalidPath(path string) bool {
	return strings.Contains(path, "..") || strings.HasPrefix(path, "/")
}

func (f *filesystem) Open(path string) (fs.File, error) {
	if invalidPath(path) {
		return nil, errors.New("invalid path")
	}

	path = filepath.Join(f.root, path)

	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	_, name := filepath.Split(path)

	return &file{
		File:     fd,
		filename: name,
		fullpath: fd.Name(),
	}, nil
}

func (f *filesystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if rd, ok := f.fsys.(fs.ReadDirFS); ok {
		return rd.ReadDir(name)
	}
	return os.ReadDir(name)
}

var _ fs.ReadDirFS = (&filesystem{})

const (
	readdirChunkSize = 512
)

func (f *filesystem) Glob(pattern string) ([]FileStat, error) {
	if invalidPath(pattern) {
		return nil, errors.New("invalid path")
	}
	subdir, suffix := filepath.Split(pattern)
	where := filepath.Join(f.root, subdir)

	dir, err := os.Open(where)
	if err != nil {
		return nil, fmt.Errorf("opening %s failed: %w", where, err)
	}
	defer dir.Close()

	var matches []string
	for {
		names, err := dir.Readdirnames(readdirChunkSize)
		if len(names) == 0 {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing filesnames from %s failed: %w", where, err)
		}
		for i := range names {
			match, err := filepath.Match(suffix, names[i])
			if err != nil {
				return nil, fmt.Errorf("using %s as pattern failed: %w", suffix, err)
			}
			if match {
				matches = append(matches, filepath.Join(f.root, subdir, names[i]))
			}
		}
	}

	out := make([]FileStat, 0, len(matches))
	for i := range matches {
		stat, _ := os.Stat(matches[i])
		if stat == nil {
			continue
		}
		out = append(out, FileStat{
			RelativePath: strings.TrimPrefix(matches[i], f.root+"/"),
			ModTime:      stat.ModTime(),
		})
	}
	return out, nil
}

func (f *filesystem) ReplaceFile(oldpath, newpath string) error {
	if invalidPath(oldpath) || invalidPath(newpath) {
		return errors.New("invalid path")
	}
	oldpath = filepath.Join(f.root, oldpath)
	newpath = filepath.Join(f.root, newpath)

	// Create the new dir(s)
	dir, _ := filepath.Split(newpath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	// file doesn't exist, so write newpath
	if _, err := os.Stat(oldpath); err != nil && os.IsNotExist(err) {
		return write(newpath, nil)
	}

	// move the existing file
	return os.Rename(oldpath, newpath)
}

func (f *filesystem) ReplaceDir(oldpath, newpath string) error {
	if invalidPath(oldpath) || invalidPath(newpath) {
		return errors.New("invalid path")
	}
	path := filepath.Join(f.root, oldpath)

	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		// dir doesn't exist, so write newpath
		return os.MkdirAll(filepath.Join(f.root, newpath), 0750)
	}

	// move the existing file
	return os.Rename(path, filepath.Join(f.root, newpath))
}

// MoveFile renames oldpath to newpath. Both paths are validated, the
// destination's parent directory is created, and an existing destination is
// replaced (recovery may republish the identical staged file). The source and
// destination parent directories are fsync'd so the directory entry change is
// durable.
func (f *filesystem) MoveFile(oldpath, newpath string) error {
	if invalidPath(oldpath) || invalidPath(newpath) {
		return errors.New("invalid path")
	}
	src := filepath.Join(f.root, oldpath)
	dst := filepath.Join(f.root, newpath)

	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("moving %s: source not found: %w", oldpath, err)
	}

	dstDir, _ := filepath.Split(dst)
	if err := os.MkdirAll(dstDir, 0750); err != nil {
		return fmt.Errorf("creating %s failed: %w", dstDir, err)
	}

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("renaming %s to %s failed: %w", oldpath, newpath, err)
	}

	srcDir, _ := filepath.Split(src)
	if err := syncDir(srcDir); err != nil {
		return fmt.Errorf("syncing source directory %s: %w", srcDir, err)
	}
	if err := syncDir(dstDir); err != nil {
		return fmt.Errorf("syncing destination directory %s: %w", dstDir, err)
	}
	return nil
}

// syncDir flushes a directory's entry changes to disk. Renames only become
// crash-durable once the affected directories have been fsync'd.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (f *filesystem) MkdirAll(path string) error {
	if invalidPath(path) {
		return errors.New("invalid path")
	}
	return os.MkdirAll(filepath.Join(f.root, path), 0750)
}

func (f *filesystem) RmdirAll(path string) error {
	if invalidPath(path) {
		return errors.New("invalid path")
	}
	return os.RemoveAll(filepath.Join(f.root, path))
}

func (f *filesystem) RemoveEmptyDir(path string) error {
	if invalidPath(path) {
		return errors.New("invalid path")
	}
	return os.Remove(filepath.Join(f.root, path))
}

func (f *filesystem) WriteFile(path string, contents []byte) error {
	if invalidPath(path) {
		return errors.New("invalid path")
	}
	dir, path := filepath.Split(path)
	dir = filepath.Join(f.root, dir)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	if err := write(filepath.Join(dir, path), contents); err != nil {
		return err
	}

	// fsync the containing directory so the new directory entry is durable.
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func write(where string, data []byte) error {
	fd, err := os.OpenFile(where, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("creating %s failed: %w", where, err)
	}
	defer fd.Close()

	_, err = fd.Write(data)
	if err != nil {
		return fmt.Errorf("writing %s failed: %w", where, err)
	}

	return fd.Sync()
}
