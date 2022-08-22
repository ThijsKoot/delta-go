package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/pqarrow"
	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/storage"
)

type Writeable interface {
	[]json.RawMessage
}

type DeltaWriter[T Writeable] interface {
	Write(values T) error
	Flush() ([]delta.Action, error)
	FlushAndCommit(table delta.DeltaTable) (delta.DeltaDataTypeVersion, error)
}

type DataWriter struct {
	Storage          storage.StorageBackend
	ArrowSchema      arrow.Schema
	WriterProperties WriterProperties
	PartitionColumns []string
	ArrowWriters     map[string]DataArrowWriter
}

type WriterProperties struct {
}

type DataArrowWriter struct {
	ArrowSchema      arrow.Schema
	WriterProperties WriterProperties
	PartitionValues  map[string]*string
	NullCounts       map[string]delta.ColumnValueStat
	ParquetWriter    *pqarrow.FileWriter

	Buffer *bytes.Buffer

	BufferedRecordBatchCount int
}

func NewDataArrowWriter(schema arrow.Schema, writerProperties WriterProperties) (*DataArrowWriter, error) {
	var buf bytes.Buffer
	parquetProps := parquet.NewWriterProperties()
	pqarrowProps := pqarrow.DefaultWriterProps()

	writer, err := newParquetWriter(&schema, &buf, parquetProps, pqarrowProps)
	if err != nil {
		return nil, fmt.Errorf("unable to create DataArrowWriter: %w", err)
	}

	return &DataArrowWriter{
		ArrowSchema:              schema,
		WriterProperties:         writerProperties,
		PartitionValues:          make(map[string]*string),
		NullCounts:               make(map[string]delta.ColumnValueStat),
		ParquetWriter:            writer,
		Buffer:                   &buf,
		BufferedRecordBatchCount: 0,
	}, nil
}

func newParquetWriter(schema *arrow.Schema, buf *bytes.Buffer, pqProps *parquet.WriterProperties, pqaProps pqarrow.ArrowWriterProperties) (*pqarrow.FileWriter, error) {
	writer, err := pqarrow.NewFileWriter(schema, buf, pqProps, pqaProps)
	if err != nil {
		return nil, fmt.Errorf("unable to create parquetwriter: %w", err)
	}
	return writer, nil
}

func (w *DataArrowWriter) WriteValues(partitionColumns []string, schema arrow.Schema, jsonBuffer []json.RawMessage) error {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, &schema)
	for _, m := range jsonBuffer {
		err := builder.UnmarshalJSON(m)
		if err != nil {
			return fmt.Errorf("unable to convert json to arrow record: %w", err)
		}
	}
	rec := builder.NewRecord()

	// TODO: following logic not implemented yet
	// if record_batch.schema() != arrow_schema {
	//     return Err(DeltaWriterError::SchemaMismatch {
	//         record_batch_schema: record_batch.schema(),
	//         expected_schema: arrow_schema,
	//     });
	// }

	if err := w.WriteRecord(partitionColumns, rec); err != nil {
		// TODO: quarantining bad rows not implemented yet
		return fmt.Errorf("error writing record: %w", err)
	}
	return nil
}

func (w *DataArrowWriter) WriteRecord(partitionColumns []string, rec arrow.Record) error {
	if len(w.PartitionValues) == 0 {
		partitionValues, err := extractPartitionValues(partitionColumns, rec)
		if err != nil {
			return fmt.Errorf("unable to extract partition values while writing record: %w", err)
		}
		w.PartitionValues = partitionValues
	}

	if err := w.ParquetWriter.Write(rec); err != nil {
		// reset ParquetWriter if write fails
		w.Buffer.Reset()
		writer, err := newParquetWriter(&w.ArrowSchema, w.Buffer, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
		if err != nil {
			return fmt.Errorf("unable to create new parquetwriter: %w", err)
		}
		w.ParquetWriter = writer
		return fmt.Errorf("unable to write to ParquetWriter: %w", err)
	}

	w.BufferedRecordBatchCount += 1
	// TODO: not implemented yet
	// apply_null_counts(&record_batch.into(), &mut self.null_counts, 0);
	return nil
}

func extractPartitionValues(partitionColumns []string, rec arrow.Record) (map[string]*string, error) {
	partitionValues := make(map[string]*string)
	for _, c := range partitionColumns {
		schema := rec.Schema()
		i := schema.FieldIndices(c)
		if i == nil {
			return nil, fmt.Errorf("partition column %s not found in arrow schema", c)
		}

		col := rec.Column(i[0])
		partitionString, err := stringifiedPartitionValue(col)
		if err != nil {
			return nil, fmt.Errorf("unable to create partition value: %w", err)
		}

		partitionValues[c] = partitionString
	}

	return partitionValues, nil
}

func stringifiedPartitionValue(arr arrow.Array) (*string, error) {
	dt := arr.DataType()
	if arr.IsNull(0) {
		return nil, nil
	}

	i64arr := arr.(*array.Int64)
	i64arr.Value(0)

	var val string
	switch dt {
	case arrow.PrimitiveTypes.Int8:
		v := arr.(*array.Int8).Value(0)
		val = strconv.FormatInt(int64(v), 10)
	case arrow.PrimitiveTypes.Int16:
		v := arr.(*array.Int16).Value(0)
		val = strconv.FormatInt(int64(v), 10)
	case arrow.PrimitiveTypes.Int32:
		v := arr.(*array.Int32).Value(0)
		val = strconv.FormatInt(int64(v), 10)
	case arrow.PrimitiveTypes.Int64:
		v := arr.(*array.Int64).Value(0)
		val = strconv.FormatInt(int64(v), 10)
	case arrow.PrimitiveTypes.Uint8:
		v := arr.(*array.Uint8).Value(0)
		val = strconv.FormatUint(uint64(v), 10)
	case arrow.PrimitiveTypes.Uint16:
		v := arr.(*array.Uint16).Value(0)
		val = strconv.FormatUint(uint64(v), 10)
	case arrow.PrimitiveTypes.Uint32:
		v := arr.(*array.Uint32).Value(0)
		val = strconv.FormatUint(uint64(v), 10)
	case arrow.PrimitiveTypes.Uint64:
		v := arr.(*array.Uint64).Value(0)
		val = strconv.FormatUint(uint64(v), 10)
	case arrow.BinaryTypes.String:
		val = arr.(*array.String).Value(0)
	default:
		return nil, fmt.Errorf("unimplemented data type: %s", dt.Name())
	}

	return &val, nil
}
