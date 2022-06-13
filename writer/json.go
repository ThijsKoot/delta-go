package writer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	jsoniter "github.com/json-iterator/go"
	"github.com/thijskoot/delta-go/delta"
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
}

func NewJsonWriterForTable(table *delta.DeltaTable) (*JsonWriter, error) {
	storage, err := storage.NewBackendForUri(table.TableUri)
	if err != nil {
		return nil, fmt.Errorf("unable to create storage backend for json writer: %w", err)
	}

	meta := table.State.CurrentMetadata
	if meta == nil {
		return nil, fmt.Errorf("table has no current metadata")
	}

	arrowSchema := meta.Schema.ToArrowSchema()
	partitionCols := meta.PartitionColumns

	return &JsonWriter{
		Storage:          storage,
		TableUri:         table.TableUri,
		ArrowSchema:      *arrowSchema,
		PartitionColumns: partitionCols,
		ArrowWriters:     make(map[string]DataArrowWriter),
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
		storagePath := w.Storage.JoinPath(w.TableUri, path)

		if err := w.Storage.PutObj(storagePath, data); err != nil {
			return nil, fmt.Errorf("unable to put data: %w", err)
		}

		// meta, err := metadata.NewFileMetaData(data, nil)
		// if err != nil {
		// 	return nil, fmt.Errorf("error parsing parquet metadata: %w", err)
		//
		// }

		add, err := CreateAdd(writer.PartitionValues, NullCounts{}, path, int64(size))
		if err != nil {
			return nil, fmt.Errorf("unable to create Add action: %w", err)
		}

		actions = append(actions, delta.Action{Add: &add})
	}

	return actions, nil
}

// FlushAndCommit implements DeltaWriter
func (w *JsonWriter) FlushAndCommit(table delta.DeltaTable) (int64, error) {
	adds, err := w.Flush()
	if err != nil {
		return 0, err
	}
	// actions := make([]delta.Action, len(adds))
	// for i := range adds {
	// 	action := delta.Action{
	// 		Add: &adds[i],
	// 	}
	// 	actions = append(actions, adds...)
	// }

	tx := table.CreateTransaction(nil)
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

//
// type action interface {
// 	delta.Add | delta.Protocol | delta.Metadata | delta.Txn | delta.Cdc | delta.Remove
// }
//
