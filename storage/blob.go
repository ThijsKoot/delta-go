package storage

import (
	"context"
	"fmt"
	"io"
	gopath "path"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var _ StorageBackend = &BlobBackend{}

type BlobBackend struct {
	bucket *blob.Bucket
}

// DeleteObjs implements StorageBackend
func (b *BlobBackend) DeleteObjs(paths ...string) error {
	for _, p := range paths {
		if err := b.bucket.Delete(context.TODO(), p); err != nil {
			return fmt.Errorf("error while deleting key '%s': %w", p, err)
		}
	}
	return nil
}

// GetObj implements StorageBackend
func (b *BlobBackend) GetObj(path string) ([]byte, error) {
	data, err := b.bucket.ReadAll(context.TODO(), path)

	switch code := gcerrors.Code(err); code {
	case gcerrors.OK:
		return data, nil
	case gcerrors.NotFound:
		return nil, ErrNotFound{Inner: err}
	default:
		return nil, err
	}
}

// HeadObj implements StorageBackend
func (b *BlobBackend) HeadObj(path string) (ObjectMeta, error) {
	attr, err := b.bucket.Attributes(context.Background(), path)

	switch code := gcerrors.Code(err); code {
	case gcerrors.OK:
		return ObjectMeta{
			Path:     path,
			Modified: attr.ModTime,
		}, nil
	case gcerrors.NotFound:
		return ObjectMeta{}, ErrNotFound{Inner: err}
	default:
		return ObjectMeta{}, err
	}
}

// JoinPaths implements StorageBackend
func (*BlobBackend) JoinPaths(path string, paths ...string) string {
	p := append([]string{path}, paths...)
	return gopath.Join(p...)
}

// ListObjs implements StorageBackend
func (b *BlobBackend) ListObjs(path string) ([]ObjectMeta, error) {
	ctx := context.TODO()
	listOpts := blob.ListOptions{Prefix: path, Delimiter: "/"}
	iter := b.bucket.List(&listOpts)

	var results []ObjectMeta
	for {
		o, err := iter.Next(ctx)
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("error while listing objects: %w", err)
		}

		meta := ObjectMeta{
			Path:     o.Key,
			Modified: o.ModTime,
		}

		results = append(results, meta)
	}
}

// PutObj implements StorageBackend
func (b *BlobBackend) PutObj(path string, data []byte) error {
	w, err := b.bucket.NewWriter(context.TODO(), path, nil)
	if err != nil {
		return fmt.Errorf("received error while creating writer: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("received error while writing data: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("received error while closing writer: %w", err)
	}

	return nil
}

// RenameObjNoReplace implements StorageBackend
func (b *BlobBackend) RenameObjNoReplace(src string, dst string) error {
	ctx := context.TODO()

	exists, err := b.bucket.Exists(ctx, dst)
	if err != nil {
		return fmt.Errorf("received error while checking if dst exists: %w", err)
	}
	if exists {
		return ErrAlreadyExists{Inner: err}
	}

	if err := b.bucket.Copy(ctx, dst, src, nil); err != nil {
		return fmt.Errorf("unable to copy src to dst: %w", err)
	}

	if err := b.bucket.Delete(ctx, src); err != nil {
		return fmt.Errorf("unable to delete src: %w", err)
	}

	return nil
}

// TrimPath implements StorageBackend
func (*BlobBackend) TrimPath(path string) string {
	return gopath.Clean(path)
}
