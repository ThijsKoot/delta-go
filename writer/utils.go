package writer

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

var (
	NULL_PARTITION_VALUE_DATA_PATH = "__HIVE_DEFAULT_PARTITION__"
)

// TODO: directly from delta-rs:
// parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that
// I have not been able to find documentation for yet.
func nextDataPath(partitionColumns []string, partitionValues map[string]*string, part *int32) (string, error) {
	var firstPart string
	if part != nil {
		firstPart = fmt.Sprintf("%05d", part)
	} else {
		firstPart = "00000"
	}

	uuidPart := uuid.New().String()
	lastPart := "c0000"

	fileName := fmt.Sprintf("part-%s-%s-%s.snappy.parquet", firstPart, uuidPart, lastPart)
	if len(partitionColumns) == 0 {
		return fileName, nil
	}

	partitionKey, err := getPartitionKey(partitionColumns, partitionValues)
	if err != nil {
		return "", fmt.Errorf("unable to create partition key: %w", err)
	}

	return fmt.Sprintf("%s/%s", partitionKey, fileName), nil
}

func getPartitionKey(partitionColumns []string, partitionValues map[string]*string) (string, error) {
	pathParts := make([]string, 0)

	for _, pc := range partitionColumns {
		var value string
		rawVal, ok := partitionValues[pc]
		if !ok {
			return "", &ErrMissingPartitionColumn{Column: pc}
		}

		if rawVal != nil {
			value = *rawVal
		} else {
			value = NULL_PARTITION_VALUE_DATA_PATH
		}

		part := fmt.Sprintf("%s=%s", pc, value)
		pathParts = append(pathParts, part)
	}

	return strings.Join(pathParts, "/"), nil
}
