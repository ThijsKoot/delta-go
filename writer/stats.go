package writer

import (
	"time"

	"github.com/thijskoot/delta-go/delta"
)

type NullCounts map[string]delta.ColumnCountStat
type MinAndMaxValues struct {
	Min map[string]delta.ColumnValueStat
	Max map[string]delta.ColumnValueStat
}

func CreateAdd(partitionValues map[string]*string, nullCounts NullCounts, path string, size int64) (delta.Add, error) {
	// TODO: skipping a whole bunch of statistics stuff

	modTime := time.Now().UnixMilli()
	dataChange := true

	add := delta.Add{
		Path:             &path,
		Size:             &size,
		PartitionValues:  &partitionValues,
		ModificationTime: &modTime,
		DataChange:       &dataChange,
		Stats:            nil,
		Tags:             nil,
	}

	return add, nil
}

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
