package writertest

import (
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/thijskoot/delta-go/delta"
	"github.com/thijskoot/delta-go/delta/schema"
	"github.com/thijskoot/delta-go/storage"
	"gocloud.dev/blob/fileblob"
)

func GetRecordBatch(partition *string, withNull bool) arrow.Record {
	var intData *array.Int32
	var stringData, modData *array.String

	if withNull {
		intData, stringData, modData = dataWithNull()
	} else {
		intData, stringData, modData = dataWithoutNull()
	}

	var key string
	var idxValues []int

	if partition != nil {
		key = *partition
	}

	switch key {
	case "":
		idxValues = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	case "modified=2021-02-01":
		idxValues = []int{3, 4, 5, 6, 7, 8, 9, 10}
	case "modified=2021-02-01/id=B":
		idxValues = []int{3, 7, 8}
	case "modified=2021-02-02":
		idxValues = []int{0, 1, 2}
	case "modified=2021-02-02/id=A":
		idxValues = []int{0, 2}
	case "modified=2021-02-02/id=B":
		idxValues = []int{1}
	}

	intData = takeInt(intData, idxValues)
	stringData = takeString(stringData, idxValues)
	modData = takeString(modData, idxValues)

	var fields []arrow.Field
	var columns []arrow.Array

	switch {
	case strings.Contains(key, "/id="):
		fields = []arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}
		columns = []arrow.Array{intData}
	case strings.Contains(key, "modified="):
		fields = []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}
		columns = []arrow.Array{stringData, intData}
	default:
		fields = []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "modified", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		columns = []arrow.Array{stringData, intData, modData}
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, columns, int64(intData.Len()))
}

func takeString(arr *array.String, indices []int) *array.String {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	for _, idx := range indices {
		builder.Append(arr.Value(idx))
	}
	return builder.NewStringArray()
}

func takeInt(arr *array.Int32, indices []int) *array.Int32 {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	for _, idx := range indices {
		builder.Append(arr.Value(idx))
	}
	return builder.NewInt32Array()
}

// Int32Array, StringArray, StringArray
func dataWithNull() (*array.Int32, *array.String, *array.String) {
	baseInt := array.NewInt32Builder(memory.DefaultAllocator)
	baseInt.AppendValues(
		[]int32{1, 2, 3, 4, 5, 99999, 7, 8, 9, 10, 11},
		[]bool{true, true, true, true, true, false, true, true, true, true, true, true},
	)

	baseStr := array.NewStringBuilder(memory.DefaultAllocator)
	baseStr.AppendValues(
		[]string{"A", "B", "", "B", "A", "A", "", "", "B", "A", "A"},
		[]bool{true, true, false, true, true, true, false, false, true, true, true},
	)

	baseMod := array.NewStringBuilder(memory.DefaultAllocator)
	baseMod.AppendValues(
		[]string{"2021-02-02", "2021-02-02", "2021-02-02", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01"},
		nil,
	)

	return baseInt.NewInt32Array(), baseStr.NewStringArray(), baseMod.NewStringArray()
}

func dataWithoutNull() (*array.Int32, *array.String, *array.String) {
	baseInt := array.NewInt32Builder(memory.DefaultAllocator)
	baseInt.AppendValues(
		[]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		nil,
	)

	baseStr := array.NewStringBuilder(memory.DefaultAllocator)
	baseStr.AppendValues(
		[]string{"A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"},
		nil,
	)

	baseMod := array.NewStringBuilder(memory.DefaultAllocator)
	baseMod.AppendValues(
		[]string{"2021-02-02", "2021-02-02", "2021-02-02", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01", "2021-02-01"},
		nil,
	)

	return baseInt.NewInt32Array(), baseStr.NewStringArray(), baseStr.NewStringArray()
}

func GetDeltaSchema() schema.Schema {
	return schema.Schema{
		Type: string(schema.DataTypeStruct),
		Fields: []schema.SchemaField{
			{
				Name:     "id",
				Type:     schema.NewPrimitiveType(schema.DataTypeString),
				Nullable: true,
			},
			{
				Name:     "value",
				Type:     schema.NewPrimitiveType(schema.DataTypeInteger),
				Nullable: true,
			},
			{
				Name:     "modified",
				Type:     schema.NewPrimitiveType(schema.DataTypeString),
				Nullable: true,
			},
		},
	}
}

func GetDeltaMetadata(partitionCols []string) delta.TableMetadata {
	schema := GetDeltaSchema()
	return delta.TableMetadata{
		Schema:           &schema,
		PartitionColumns: partitionCols,
	}
}

func CreateBareTable(dir string) *delta.Table {
	bucket, err := fileblob.OpenBucket(dir, nil)
	if err != nil {
		panic(err)
	}

	backend, err := storage.NewBackend(bucket)
	if err != nil {
		panic(err)
	}

	table, err := delta.NewTable("mytable", backend, delta.TableConfig{})
	if err != nil {
		panic(err)
	}

	return table
}

func CreateInitializedTable(dir string, partitionCols []string) *delta.Table {
	table := CreateBareTable(dir)
	schema := GetDeltaSchema()

	protocol := delta.ActionProtocol{
		MinReaderVersion: 1,
		MinWriterVersion: 1,
	}

	metadata := delta.TableMetadata {
		Schema: &schema,
		PartitionColumns: partitionCols,
	}

	if err := table.Create(metadata, protocol, make(map[string]string), nil); err != nil {
		panic(err)
	}

	return table
}

//     let base_int = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
//     let base_str = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
//     let base_mod = StringArray::from(vec![
//         "2021-02-02",
//         "2021-02-02",
//         "2021-02-02",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//     ]);

//     let base_int = Int32Array::from(vec![
//         Some(1),
//         Some(2),
//         Some(3),
//         Some(4),jkkkk
//         Some(5),
//         None,
//         Some(7),
//         Some(8),
//         Some(9),
//         Some(10),
//         Some(11),
//     ]);
//     let base_str = StringArray::from(vec![
//         Some("A"),
//         Some("B"),
//         None,
//         Some("B"),
//         Some("A"),
//         Some("A"),
//         None,
//         None,
//         Some("B"),
//         Some("A"),
//         Some("A"),
//     ]);
//     let base_mod = StringArray::from(vec![
//         Some("2021-02-02"),
//         Some("2021-02-02"),
//         Some("2021-02-02"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//     ]);

// pub fn get_record_batch(part: Option<String>, with_null: bool) -> RecordBatch {
//     let (base_int, base_str, base_mod) = if with_null {
//         data_with_null()
//     } else {
//         data_without_null()
//     };
//
//     let indices = match &part {
//         Some(key) if key == "modified=2021-02-01" => {
//             UInt32Array::from(vec![3, 4, 5, 6, 7, 8, 9, 10])
//         }
//         Some(key) if key == "modified=2021-02-01/id=A" => UInt32Array::from(vec![4, 5, 6, 9, 10]),
//         Some(key) if key == "modified=2021-02-01/id=B" => UInt32Array::from(vec![3, 7, 8]),
//         Some(key) if key == "modified=2021-02-02" => UInt32Array::from(vec![0, 1, 2]),
//         Some(key) if key == "modified=2021-02-02/id=A" => UInt32Array::from(vec![0, 2]),
//         Some(key) if key == "modified=2021-02-02/id=B" => UInt32Array::from(vec![1]),
//         _ => UInt32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
//     };
//
//     let int_values = take(&base_int, &indices, None).unwrap();
//     let str_values = take(&base_str, &indices, None).unwrap();
//     let mod_values = take(&base_mod, &indices, None).unwrap();
//
//     match &part {
//         Some(key) if key.contains("/id=") => {
//             let schema = Arc::new(ArrowSchema::new(vec![Field::new(
//                 "value",
//                 DataType::Int32,
//                 true,
//             )]));
//             RecordBatch::try_new(schema, vec![int_values]).unwrap()
//         }
//         Some(_) => {
//             let schema = Arc::new(ArrowSchema::new(vec![
//                 Field::new("id", DataType::Utf8, true),
//                 Field::new("value", DataType::Int32, true),
//             ]));
//             RecordBatch::try_new(schema, vec![str_values, int_values]).unwrap()
//         }
//         _ => {
//             let schema = Arc::new(ArrowSchema::new(vec![
//                 Field::new("id", DataType::Utf8, true),
//                 Field::new("value", DataType::Int32, true),
//                 Field::new("modified", DataType::Utf8, true),
//             ]));
//             RecordBatch::try_new(schema, vec![str_values, int_values, mod_values]).unwrap()
//         }
//     }
// }
//
// fn data_with_null() -> (Int32Array, StringArray, StringArray) {
//     let base_int = Int32Array::from(vec![
//         Some(1),
//         Some(2),
//         Some(3),
//         Some(4),
//         Some(5),
//         None,
//         Some(7),
//         Some(8),
//         Some(9),
//         Some(10),
//         Some(11),
//     ]);
//     let base_str = StringArray::from(vec![
//         Some("A"),
//         Some("B"),
//         None,
//         Some("B"),
//         Some("A"),
//         Some("A"),
//         None,
//         None,
//         Some("B"),
//         Some("A"),
//         Some("A"),
//     ]);
//     let base_mod = StringArray::from(vec![
//         Some("2021-02-02"),
//         Some("2021-02-02"),
//         Some("2021-02-02"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//         Some("2021-02-01"),
//     ]);
//
//     (base_int, base_str, base_mod)
// }

// fn data_without_null() -> (Int32Array, StringArray, StringArray) {
//     let base_int = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
//     let base_str = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
//     let base_mod = StringArray::from(vec![
//         "2021-02-02",
//         "2021-02-02",
//         "2021-02-02",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//         "2021-02-01",
//     ]);
//
//     (base_int, base_str, base_mod)
// }

// pub fn get_delta_schema() -> Schema {
//     Schema::new(vec![
//         SchemaField::new(
//             "id".to_string(),
//             SchemaDataType::primitive("string".to_string()),
//             true,
//             HashMap::new(),
//         ),
//         SchemaField::new(
//             "value".to_string(),
//             SchemaDataType::primitive("integer".to_string()),
//             true,
//             HashMap::new(),
//         ),
//         SchemaField::new(
//             "modified".to_string(),
//             SchemaDataType::primitive("string".to_string()),
//             true,
//             HashMap::new(),
//         ),
//     ])
// }

// pub fn get_delta_metadata(partition_cols: &[String]) -> DeltaTableMetaData {
//     let table_schema = get_delta_schema();
//     DeltaTableMetaData::new(
//         None,
//         None,
//         None,
//         table_schema,
//         partition_cols.to_vec(),
//         HashMap::new(),
//     )
// }

// pub fn create_bare_table() -> DeltaTable {
//     let table_dir = tempfile::tempdir().unwrap();
//     let table_path = table_dir.path();
//     DeltaTableBuilder::from_uri(table_path.to_str().unwrap())
//         .build()
//         .unwrap()
// }

// pub async fn create_initialized_table(partition_cols: &[String]) -> DeltaTable {
//     let mut table = create_bare_table();
//     let table_schema = get_delta_schema();
//
//     let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
//     commit_info.insert(
//         "operation".to_string(),
//         serde_json::Value::String("CREATE TABLE".to_string()),
//     );
//     commit_info.insert(
//         "userName".to_string(),
//         serde_json::Value::String("test user".to_string()),
//     );
//
//     let protocol = Protocol {
//         min_reader_version: 1,
//         min_writer_version: 1,
//     };
//
//     let metadata = DeltaTableMetaData::new(
//         None,
//         None,
//         None,
//         table_schema,
//         partition_cols.to_vec(),
//         HashMap::new(),
//     );
//
//     table
//         .create(metadata, protocol, Some(commit_info), None)
//         .await
//         .unwrap();
//
//     table
// }
