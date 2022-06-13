package writer

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/thijskoot/delta-go/storage"
)
type RecordBatchWriter struct {
	Storage          storage.StorageBackend
	TableUri         string
	ArrowSchema      arrow.Schema
	WriterProperties WriterProperties
	PartitionColumns []string
	ArrowWriters     map[string]DataArrowWriter
}
