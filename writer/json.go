package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/parquet/file"
	jsoniter "github.com/json-iterator/go"
	"github.com/thijskoot/delta-go"
	"github.com/thijskoot/delta-go/storage"
)

var (
	_ DeltaWriter[[]json.RawMessage] = &JsonWriter{}
)

type JsonWriter struct {
	Storage          storage.StorageBackend
	TableUri         string
	ArrowSchema      arrow.Schema
	WriterProperties WriterProperties
	PartitionColumns []string
	ArrowWriters     map[string]DataArrowWriter
	Table            *delta.Table
}

func NewJsonWriterForTable(table *delta.Table) (*JsonWriter, error) {
	meta := table.State.CurrentMetadata
	if meta == nil {
		return nil, fmt.Errorf("table has no current metadata")
	}

	arrowSchema := meta.Schema.ToArrowSchema()
	partitionCols := meta.PartitionColumns

	return &JsonWriter{
		Storage:          table.Storage,
		TableUri:         table.TableUri,
		ArrowSchema:      *arrowSchema,
		PartitionColumns: partitionCols,
		ArrowWriters:     make(map[string]DataArrowWriter),
		Table:            table,
	}, nil
}

// Flush implements DeltaWriter
func (w *JsonWriter) Flush() ([]delta.Action, error) {
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

		preader, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("unable to open parquet buffer for stats extraction: %w", err)
		}
		meta := preader.MetaData()

		if err := w.Storage.PutObj(storagePath, data); err != nil {
			return nil, fmt.Errorf("unable to put data: %w", err)
		}

		add, err := createAdd(writer.PartitionValues, writer.NullCounts, path, int64(size), meta)
		if err != nil {
			return nil, fmt.Errorf("unable to create Add action: %w", err)
		}

		actions = append(actions, delta.Action{Add: &add})
	}

	return actions, nil
}

// FlushAndCommit implements DeltaWriter
func (w *JsonWriter) FlushAndCommit() (int64, error) {
	adds, err := w.Flush()
	if err != nil {
		return 0, err
	}

	tx := w.Table.CreateTransaction(nil)
	tx.AddActions(adds)
	version, err := tx.Commit(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("unable to flush and commit: %w", err)
	}

	return version, nil
}

// Write implements DeltaWriter
func (w *JsonWriter) Write(records []json.RawMessage) error {
	partitioned, err := w.divideByPartitionValues(records)
	if err != nil {
		return fmt.Errorf("unable to write: %w", err)
	}

	for k, values := range partitioned {
		_, ok := w.ArrowWriters[k]
		if !ok {
			writer, err := NewDataArrowWriter(w.ArrowSchema, w.WriterProperties)
			if err != nil {
				return fmt.Errorf("unable to create new DataArrowWriter: %w", err)
			}
			w.ArrowWriters[k] = *writer
		}

		writer := w.ArrowWriters[k]

		if err := writer.WriteValues(w.PartitionColumns, w.ArrowSchema, values); err != nil {
			return fmt.Errorf("unable to write values: %w", err)
		}
	}
	return nil
}

func (w *JsonWriter) divideByPartitionValues(records []json.RawMessage) (map[string][]json.RawMessage, error) {
	partitionedRecords := make(map[string][]json.RawMessage)
	for _, r := range records {
		partitionValue, err := w.jsonToPartitionValues(r)
		if err != nil {
			return nil, fmt.Errorf("unable to divide records into partitions: %w", err)
		}

		partitionedRecords[partitionValue] = append(partitionedRecords[partitionValue], r)
	}
	return partitionedRecords, nil
}

func (w *JsonWriter) jsonToPartitionValues(value json.RawMessage) (string, error) {
	values := make([]string, len(w.PartitionColumns))
	for i, c := range w.PartitionColumns {
		jsonValue := jsoniter.Get(value, c)

		if err := jsonValue.LastError(); err == nil {
			values[i] = jsonValue.ToString()
		} else if err.Error() == fmt.Sprintf("[%s] not found", c) {
			values[i] = "null"
		} else {
			return "", fmt.Errorf("unable to get partition value: %w", err)
		}
	}

	return strings.Join(values, "/"), nil
}
