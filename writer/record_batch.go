package writer

import (
	"fmt"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/parquet/metadata"
	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/storage"
)

var (
	_ DeltaWriter[arrow.Record] = &RecordBatchWriter{}
)

type RecordBatchWriter struct {
	Storage          storage.StorageBackend
	TableUri         string
	ArrowSchema      arrow.Schema
	WriterProperties WriterProperties
	PartitionColumns []string
	ArrowWriters     map[string]DataArrowWriter
	Table            *delta.Table
}

func NewRecordBatchWriter(table *delta.Table) (*RecordBatchWriter, error) {
	meta := table.State.CurrentMetadata
	if meta == nil {
		return nil, fmt.Errorf("table has no current metadata")
	}

	return &RecordBatchWriter{
		Storage:          table.Storage,
		TableUri:         table.TableUri,
		ArrowSchema:      *meta.Schema.ToArrowSchema(),
		PartitionColumns: meta.PartitionColumns,
		ArrowWriters:     make(map[string]DataArrowWriter),
		Table:            table,
	}, nil
}

// Flush implements DeltaWriter
func (w *RecordBatchWriter) Flush() ([]delta.Action, error) {
	actions := make([]delta.Action, 0)
	for _, writer := range w.ArrowWriters {
		if err := writer.ParquetWriter.Close(); err != nil {
			return nil, fmt.Errorf("unable to close parquetwriter: %w", err)
		}

		path, err := nextDataPath(w.PartitionColumns, writer.PartitionValues, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to create next data path: %w", err)
		}

		data := writer.Buffer.Bytes()
		size := len(data)
		storagePath := w.Storage.JoinPaths(w.TableUri, path)

		if err := w.Storage.PutObj(storagePath, data); err != nil {
			return nil, fmt.Errorf("unable to put data: %w", err)
		}

		meta, err := metadata.NewFileMetaData(data, nil)
		if err != nil {
			return nil, fmt.Errorf("error parsing parquet metadata: %w", err)
		}

		add, err := createAdd(writer.PartitionValues, NullCounts{}, path, int64(size), meta)
		if err != nil {
			return nil, fmt.Errorf("unable to create Add action: %w", err)
		}

		actions = append(actions, delta.Action{Add: &add})
	}

	return actions, nil
}

// FlushAndCommit implements DeltaWriter
func (*RecordBatchWriter) FlushAndCommit() (int64, error) {
	panic("unimplemented")
}

// Write implements DeltaWriter
func (*RecordBatchWriter) Write(values arrow.Record) error {
	panic("unimplemented")
}
