package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilesystem_MoveFile(t *testing.T) {
	dir := t.TempDir()
	chest, err := NewFilesystem(dir)
	require.NoError(t, err)

	t.Run("moves into freshly created parent and removes source", func(t *testing.T) {
		require.NoError(t, chest.WriteFile("staging/shard-a/abc/file.ach", []byte("nacha")))

		require.NoError(t, chest.MoveFile("staging/shard-a/abc/file.ach", "mergable/shard-a/file.ach"))

		_, err := chest.Open("staging/shard-a/abc/file.ach")
		require.Error(t, err)

		fd, err := chest.Open("mergable/shard-a/file.ach")
		require.NoError(t, err)
		require.NoError(t, fd.Close())
	})

	t.Run("missing source errors and creates nothing", func(t *testing.T) {
		err := chest.MoveFile("staging/missing/nope.ach", "mergable/shard-a/nope.ach")
		require.Error(t, err)

		_, statErr := os.Stat(filepath.Join(dir, "mergable", "shard-a", "nope.ach"))
		require.True(t, os.IsNotExist(statErr))
	})

	t.Run("replaces existing destination", func(t *testing.T) {
		require.NoError(t, chest.WriteFile("staging/replace/new.ach", []byte("new-bytes")))
		require.NoError(t, chest.WriteFile("mergable/replace/old.ach", []byte("old-bytes")))

		require.NoError(t, chest.MoveFile("staging/replace/new.ach", "mergable/replace/old.ach"))

		fd, err := chest.Open("mergable/replace/old.ach")
		require.NoError(t, err)
		defer fd.Close()
	})

	t.Run("rejects invalid paths", func(t *testing.T) {
		require.EqualError(t, chest.MoveFile("../escape.ach", "mergable/x.ach"), "invalid path")
		require.EqualError(t, chest.MoveFile("mergable/x.ach", "../escape.ach"), "invalid path")
	})
}
