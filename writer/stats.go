package writer

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/metadata"
	"github.com/apache/arrow/go/v9/parquet/schema"
	"github.com/thijskoot/delta-go/delta"
	"golang.org/x/exp/constraints"
	"k8s.io/utils/pointer"
)

type NullCounts map[string]delta.ColumnCountStat

type MinAndMaxValues struct {
	Min map[string]delta.ColumnValueStat
	Max map[string]delta.ColumnValueStat
}

func createAdd(partitionValues map[string]*string, nullCounts NullCounts, path string, size int64, fileMetadata *metadata.FileMetaData) (delta.Add, error) {
	// TODO: skipping a whole bunch of statistics stuff

	modTime := time.Now().UnixMilli()
	dataChange := true

	minmaxValues, err := minMaxValuesFromFileMetadata(partitionValues, *fileMetadata)
	if err != nil {
		return delta.Add{}, fmt.Errorf("unable to create add, got error while calculating minMaxValues: %w", err)
	}

	stats := delta.Stats{
		NumRecords: fileMetadata.NumRows,
		MinValues:  minmaxValues.Min,
		MaxValues:  minmaxValues.Max,
		NullCount:  nullCounts,
	}

	statsBytes, err := json.Marshal(&stats)
	if err != nil {
		return delta.Add{}, fmt.Errorf("unable to create add, error while serializing stats: %w", err)
	}
	statsString := string(statsBytes)

	add := delta.Add{
		Path:             path,
		Size:             size,
		PartitionValues:  partitionValues,
		ModificationTime: modTime,
		DataChange:       dataChange,
		Stats:            &statsString,
		Tags:             nil,
	}

	return add, nil
}

func applyNullCounts(arr *array.Struct, nullCounts NullCounts, nestLevel int) {
	fields := arr.DataType().(*arrow.StructType).Fields()
	for i, c := range arr.Data().Children() {
		key := fields[i].Name
		if c.DataType().ID() == arrow.STRUCT {
			colStruct, ok := nullCounts[key]
			if !ok {
				colStruct = delta.ColumnCountStat{
					Column: make(map[string]delta.ColumnCountStat),
				}
				nullCounts[key] = colStruct
			}
			applyNullCounts(array.NewStructData(c), colStruct.Column, nestLevel+1)
			continue
		}

		colStruct, ok := nullCounts[key]
		if !ok {
			colStruct = delta.ColumnCountStat{
				Value: pointer.Int64(0),
			}
			nullCounts[key] = colStruct
		}

		v := int64(c.NullN()) + *colStruct.Value
		colStruct.Value = &v
	}
}

func minMaxValuesFromFileMetadata(partitionValues map[string]*string, fileMeta metadata.FileMetaData) (MinAndMaxValues, error) {
	minValues := make(map[string]delta.ColumnValueStat)
	maxValues := make(map[string]delta.ColumnValueStat)

	colCount := fileMeta.Schema.NumColumns()
	// holds a slice of statistics for each column
	// index is column index, value is the slice of statistics
	statistics := make([][]metadata.TypedStatistics, colCount)

	for _, rg := range fileMeta.RowGroups {
		for i := 0; i < fileMeta.Schema.NumColumns(); i++ {
			cc := rg.Columns[i]

			ccstats := metadata.NewStatisticsFromEncoded(fileMeta.Schema.Column(i), nil, cc.MetaData.NumValues, cc.MetaData.Statistics)

			statistics[i] = append(statistics[i], ccstats)
		}
	}

	for i := 0; i < fileMeta.Schema.NumColumns(); i++ {
		col := fileMeta.Schema.Column(i)

		// If max rep level is > 0, this is an array element or a struct element of an array or something downstream of an array.
		// delta/databricks only computes null counts for arrays - not min max.
		// null counts are tracked at the record batch level, so skip any column with MaxRepLevel
		// > 0
		if col.MaxRepetitionLevel() > 0 {
			continue
		}

		colPath := col.ColumnPath()

		// Do not include partition columns in statistics
		if _, containsKey := partitionValues[colPath[0]]; containsKey {
			continue
		}

		err := applyMinMaxForColumn(statistics[i], col, colPath, minValues, maxValues)
		if err != nil {
			return MinAndMaxValues{}, fmt.Errorf("error creating min-max values: %w", err)
		}

	}
	return MinAndMaxValues{
		Min: minValues,
		Max: maxValues,
	}, nil
}

func applyMinMaxForColumn(
	statistics []metadata.TypedStatistics,
	columnDescr *schema.Column,
	colPathParts []string,
	minValues,
	maxValues map[string]delta.ColumnValueStat) error {
	if len(colPathParts) == 1 {
		min, max, err := minMaxFromParquetStats(statistics, columnDescr)
		if err == errNoStats {
			return nil
		}
		if err != nil {
			return err
		}

		minValues[columnDescr.Name()] = delta.ColumnValueStat{Value: min}
		maxValues[columnDescr.Name()] = delta.ColumnValueStat{Value: max}
		return nil
	}

	key := colPathParts[0]
	if _, ok := minValues[key]; !ok {
		minValues[key] = delta.ColumnValueStat{Column: make(map[string]delta.ColumnValueStat)}
	}

	if _, ok := maxValues[key]; !ok {
		minValues[key] = delta.ColumnValueStat{Column: make(map[string]delta.ColumnValueStat)}
	}

	childMin := minValues[key]
	childMax := maxValues[key]

	remainingParts := colPathParts[1:]
	err := applyMinMaxForColumn(statistics, columnDescr, remainingParts, childMin.Column, childMax.Column)
	if err != nil {
		return err
	}

	return nil
}

var errNoStats = errors.New("column has no stats")

func minMaxFromParquetStats(stats []metadata.TypedStatistics, columnDescr *schema.Column) (json.RawMessage, json.RawMessage, error) {
	statsWithMinMax := make([]metadata.TypedStatistics, 0)
	for _, s := range stats {
		if s.HasMinMax() {
			statsWithMinMax = append(statsWithMinMax, s)
		}
	}

	if len(statsWithMinMax) == 0 {
		return nil, nil, errNoStats
	}

	c := statsWithMinMax[0]
	var min, max interface{}
	switch c.Type() {
	case parquet.Types.Boolean:
		min, max = boolMinMax(stats)
	case parquet.Types.Int32:
		min, max = int32MinMax(stats)
	case parquet.Types.Int64:
		min, max = int64MinMax(stats)
	case parquet.Types.Float:
		min, max = float32MinMax(stats)
	case parquet.Types.Double:
		min, max = float64MinMax(stats)
	case parquet.Types.ByteArray:
		if columnDescr.ConvertedType() == schema.ConvertedTypes.UTF8 {
			min, max = stringMinMax(stats)
		}
	}

	minEnc, err := json.Marshal(min)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to encode min stats: %w", err)
	}
	maxEnc, err := json.Marshal(max)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to encode max stats: %w", err)
	}

	return minEnc, maxEnc, nil
}

func boolMinMax(stats []metadata.TypedStatistics) (bool, bool) {
	var min, max bool
	for i, stat := range stats {
		s, _ := stat.(*metadata.BooleanStatistics)
		if i == 0 {
			min = s.Min()
			max = s.Max()
		}

		// writing min max for bools is weird
		if min && !s.Min() {
			min = false
		}

		if !max && s.Max() {
			max = true
		}
	}

	return min, max
}

func int32MinMax(stats []metadata.TypedStatistics) (int32, int32) {
	var mins, maxes []int32
	for _, stat := range stats {
		s, _ := stat.(*metadata.Int32Statistics)
		mins = append(mins, s.Min())
		maxes = append(maxes, s.Max())
	}

	return sliceMin(mins), sliceMax(maxes)
}

func int64MinMax(stats []metadata.TypedStatistics) (int64, int64) {
	var mins, maxes []int64
	for _, stat := range stats {
		s, _ := stat.(*metadata.Int64Statistics)
		mins = append(mins, s.Min())
		maxes = append(maxes, s.Max())
	}

	return sliceMin(mins), sliceMax(maxes)
}

func float32MinMax(stats []metadata.TypedStatistics) (float32, float32) {
	var mins, maxes []float32
	for _, stat := range stats {
		s, _ := stat.(*metadata.Float32Statistics)
		mins = append(mins, s.Min())
		maxes = append(maxes, s.Max())
	}

	return sliceMin(mins), sliceMax(maxes)
}

func float64MinMax(stats []metadata.TypedStatistics) (float64, float64) {
	var mins, maxes []float64
	for _, stat := range stats {
		s, _ := stat.(*metadata.Float64Statistics)
		mins = append(mins, s.Min())
		maxes = append(maxes, s.Max())
	}

	return sliceMin(mins), sliceMax(maxes)
}

func stringMinMax(stats []metadata.TypedStatistics) (string, string) {
	var mins, maxes []string
	for _, stat := range stats {
		mins = append(mins, string(stat.EncodeMin()))
		maxes = append(maxes, string(stat.EncodeMax()))
	}

	return sliceMin(mins), sliceMax(maxes)
}

func sliceMin[T constraints.Ordered](values []T) T {
	var min T
	for i, v := range values {
		if i == 0 || v < min {
			min = v
		}
	}
	return min
}

func sliceMax[T constraints.Ordered](values []T) T {
	var max T
	for i, v := range values {
		if i == 0 || v > max {
			max = v
		}
	}
	return max
}

// fn apply_min_max_for_column(
//     statistics: &[&Statistics],
//     column_descr: Arc<ColumnDescriptor>,
//     column_path_parts: &[String],
//     min_values: &mut HashMap<String, ColumnValueStat>,
//     max_values: &mut HashMap<String, ColumnValueStat>,
// ) -> Result<(), DeltaWriterError> {
//     match (column_path_parts.len(), column_path_parts.first()) {
//         // Base case - we are at the leaf struct level in the path
//         (1, _) => {
//             let (min, max) = min_and_max_from_parquet_statistics(statistics, column_descr.clone())?;
//
//             if let Some(min) = min {
//                 let min = ColumnValueStat::Value(min);
//                 min_values.insert(column_descr.name().to_string(), min);
//             }
//
//             if let Some(max) = max {
//                 let max = ColumnValueStat::Value(max);
//                 max_values.insert(column_descr.name().to_string(), max);
//             }
//
//             Ok(())
//         }
//         // Recurse to load value at the appropriate level of HashMap
//         (_, Some(key)) => {
//             let child_min_values = min_values
//                 .entry(key.to_owned())
//                 .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
//             let child_max_values = max_values
//                 .entry(key.to_owned())
//                 .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
//
//             match (child_min_values, child_max_values) {
//                 (ColumnValueStat::Column(mins), ColumnValueStat::Column(maxes)) => {
//                     let remaining_parts: Vec<String> = column_path_parts
//                         .iter()
//                         .skip(1)
//                         .map(|s| s.to_string())
//                         .collect();
//
//                     apply_min_max_for_column(
//                         statistics,
//                         column_descr,
//                         remaining_parts.as_slice(),
//                         mins,
//                         maxes,
//                     )?;
//
//                     Ok(())
//                 }
//                 _ => {
//                     unreachable!();
//                 }
//             }
//         }
//         // column path parts will always have at least one element.
//         (_, None) => {
//             unreachable!();
//         }
//     }
// }

//
// func minMaxValues(partitionValues map[string]*string, fileMeta metadata.FileMetaData) (MinAndMaxValues, error) {
// 	minValues := make(map[string]delta.ColumnValueStat)
// 	maxValues := make(map[string]delta.ColumnValueStat)
//
// 	for _, rg := range fileMeta.RowGroups {
// 		metadata.NewRowGroupMetaDataBuilder
//
// 	}
//
// }

// let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
// let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;
//
// let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
// let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
//
// let row_group_metadata: Result<Vec<RowGroupMetaData>, ParquetError> = file_metadata
//     .row_groups
//     .iter()
//     .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
//     .collect();
// let row_group_metadata = row_group_metadata?;
//
// for i in 0..schema_descriptor.num_columns() {
//     let column_descr = schema_descriptor.column(i);
//
//     // If max rep level is > 0, this is an array element or a struct element of an array or something downstream of an array.
//     // delta/databricks only computes null counts for arrays - not min max.
//     // null counts are tracked at the record batch level, so skip any column with max_rep_level
//     // > 0
//     if column_descr.max_rep_level() > 0 {
//         continue;
//     }
//
//     let column_path = column_descr.path();
//     let column_path_parts = column_path.parts();
//
//     // Do not include partition columns in statistics
//     if partition_values.contains_key(&column_path_parts[0]) {
//         continue;
//     }
//
//     let statistics: Vec<&Statistics> = row_group_metadata
//         .iter()
//         .filter_map(|g| g.column(i).statistics())
//         .collect();
//
//     apply_min_max_for_column(
//         statistics.as_slice(),
//         column_descr.clone(),
//         column_path_parts,
//         &mut min_values,
//         &mut max_values,
//     )?;
// }
//
// Ok((min_values, max_values))
