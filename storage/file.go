package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	gopath "path"

	"github.com/google/uuid"
)

type FileStorageBackend struct {
	Root string
}

// // Create a new path by appending `path_to_join` as a new component to `path`.
func (fsb *FileStorageBackend) JoinPath(path, pathToJoin string) string {
	return gopath.Join(path, pathToJoin)
}

// More efficient path join for multiple path components. Use this method if you need to
// combine more than two path components.
func (fsb *FileStorageBackend) JoinPaths(path string, paths []string) string {
	newPaths := make([]string, len(paths)+1)
	newPaths = append(newPaths, path)
	newPaths = append(newPaths, paths...)
	return gopath.Join(newPaths...)
}

// Returns trimed path with trailing path separator removed.
func (fsb *FileStorageBackend) TrimPath(path string) string {
	return gopath.Clean(path)
}

// Fetch object metadata without reading the actual content
func (fsb *FileStorageBackend) HeadObj(path string) (ObjectMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return ObjectMeta{}, fmt.Errorf("unable to open file: %w", err)
	}

	fInfo, err := f.Stat()
	if err != nil {
		return ObjectMeta{}, fmt.Errorf("unable to stat file: %w", err)
	}

	return ObjectMeta{
		Path:     path,
		Modified: fInfo.ModTime(),
	}, nil
}

// Fetch object content
func (fsb *FileStorageBackend) GetObj(path string) ([]byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, &ErrNotFound{Inner: err}
	} else if err != nil {
		return nil, fmt.Errorf("unable to open file: %w", err)
	}

	bytes, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}

	return bytes, nil
}

// Return a list of objects by `path` prefix in an async stream.
func (fsb *FileStorageBackend) ListObjs(path string) ([]ObjectMeta, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("unable to read dir: %w", err)
	}

	results := make([]ObjectMeta, len(entries))
	for _, e := range entries {
		fPath := gopath.Join(path, e.Name())

		fileInfo, err := os.Stat(fPath)
		if err != nil {
			return nil, fmt.Errorf("unable to stat file %s: %w", fPath, err)
		}

		meta := ObjectMeta{
			Path:     fPath,
			Modified: fileInfo.ModTime(),
		}

		results = append(results, meta)
	}

	return results, nil
}

// Create new object with `obj_bytes` as content.
//
// Implementation note:
//
// To support safe concurrent read, if `path` already exists, `put_obj` needs to update object
// content in backing store atomically, i.e. reader of the object should never read a partial
// write.
func (fsb *FileStorageBackend) PutObj(path string, data []byte) error {
	if err := os.MkdirAll(gopath.Dir(path), 0750); err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}

	tmpSuffix := uuid.New()
	tmpPath := fmt.Sprintf("%s_%s", path, tmpSuffix.String())

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("unable to open temp file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("unable to write to temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		if err := os.Remove(tmpPath); err != nil {
			return fmt.Errorf("unable to remove temp file after rename failed: %w", err)
		}
		return fmt.Errorf("unable to rename temp file: %w", err)
	}

	return nil
}

// Moves object from `src` to `dst`.
//
// Implementation note:
//
// For a multi-writer safe backend, `rename_obj_noreplace` needs to implement rename if not exists semantic.
// In other words, if the destination path already exists, rename should return a
// [StorageError::AlreadyExists] error.
func (fsb *FileStorageBackend) RenameObjNoReplace(src, dst string) error {
	_, err := os.Stat(dst)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("unable to rename file: %w", err)
		}
		return nil
	}
	if err == nil {
		return &ErrAlreadyExists{
			Inner: err,
			Path:  dst,
		}
	}
	return fmt.Errorf("unable to check if destination path alraedy exists: %w", err)
}

// Deletes object by `path`.
func (fsb *FileStorageBackend) DeleteObj(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("unable to delete object: %w", err)
	}

	return nil
}

// Deletes object by `paths`.
func (fsb *FileStorageBackend) DeleteObjs(paths []string) error {
	for _, p := range paths {
		err := fsb.DeleteObj(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("unable to delete file '%s': %w", p, err)
		}
	}

	return nil
}
