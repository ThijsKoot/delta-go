package storage

import (
	"fmt"
	"time"

	"gocloud.dev/blob"
)

func NewBackend(bucket *blob.Bucket) (StorageBackend, error) {
	if bucket == nil {
		return nil, fmt.Errorf("bucket cannnot be nil")
	}

	return &BlobBackend{
		bucket: bucket,
	}, nil
}

type StorageBackend interface {
	// More efficient path join for multiple path components. Use this method if you need to
	// combine more than two path components.
	JoinPaths(path string, paths ...string) string
	// Returns trimed path with trailing path separator removed.
	TrimPath(path string) string
	// Fetch object metadata without reading the actual content
	HeadObj(path string) (ObjectMeta, error)
	// Fetch object content
	GetObj(path string) ([]byte, error)

	// Return a list of objects by `path` prefix in an async stream.
	ListObjs(path string) ([]ObjectMeta, error) // Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,

	// Create new object with `obj_bytes` as content.
	//
	// Implementation note:
	//
	// To support safe concurrent read, if `path` already exists, `put_obj` needs to update object
	// content in backing store atomically, i.e. reader of the object should never read a partial
	// write.
	PutObj(path string, data []byte) error

	// Moves object from `src` to `dst`.
	//
	// Implementation note:
	//
	// For a multi-writer safe backend, `rename_obj_noreplace` needs to implement rename if not exists semantic.
	// In other words, if the destination path already exists, rename should return a
	// [StorageError::AlreadyExists] error.
	RenameObjNoReplace(src, dst string) error
	// Deletes object by `paths`.
	DeleteObjs(paths ...string) error
}

// Describes metadata of a storage object.
type ObjectMeta struct {
	// The path where the object is stored. This is the path component of the object URI.
	//
	// For example:
	//   * path for `s3://bucket/foo/bar` should be `foo/bar`.
	//   * path for `dir/foo/bar` should be `dir/foo/bar`.
	//
	// Given a table URI, object URI can be constructed by joining table URI with object path.
	Path string

	// The last time the object was modified in the storage backend.
	// The timestamp of a commit comes from the remote storage `lastModifiedTime`, and can be
	// adjusted for clock skew.
	Modified time.Time
}

// // Create a new path by appending `path_to_join` as a new component to `path`.
// #[inline]
// fn join_path(&self, path: &str, path_to_join: &str) -> String {
//     let normalized_path = path.trim_end_matches('/');
//     format!("{}/{}", normalized_path, path_to_join)
// }

// More efficient path join for multiple path components. Use this method if you need to
// combine more than two path components.
// #[inline]
// fn join_paths(&self, paths: &[&str]) -> String {
//     paths
//         .iter()
//         .map(|s| s.trim_end_matches('/'))
//         .collect::<Vec<_>>()
//         .join("/")
// }

// // Returns trimed path with trailing path separator removed.
// #[inline]
// fn trim_path(&self, path: &str) -> String {
//     path.trim_end_matches('/').to_string()
// }

// Fetch object metadata without reading the actual content
// async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError>;

// Fetch object content
// async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError>;

// Return a list of objects by `path` prefix in an async stream.
// async fn list_objs<'a>(
//     &'a self,
//     path: &'a str,
// ) -> Result<
//     Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
//     StorageError,
// >;

// Create new object with `obj_bytes` as content.
//
// Implementation note:
//
// To support safe concurrent read, if `path` already exists, `put_obj` needs to update object
// content in backing store atomically, i.e. reader of the object should never read a partial
// write.
// async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError>;

// Moves object from `src` to `dst`.
//
// Implementation note:
//
// For a multi-writer safe backend, `rename_obj_noreplace` needs to implement rename if not exists semantic.
// In other words, if the destination path already exists, rename should return a
// [StorageError::AlreadyExists] error.
// async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError>;

// Deletes object by `path`.
// async fn delete_obj(&self, path: &str) -> Result<(), StorageError>;

// Deletes object by `paths`.
// async fn delete_objs(&self, paths: &[String]) -> Result<(), StorageError> {
//     for path in paths {
//         match self.delete_obj(path).await {
//             Ok(_) => continue,
//             Err(StorageError::NotFound) => continue,
//             Err(e) => return Err(e),
//         }
//     }
//     Ok(())
// }
