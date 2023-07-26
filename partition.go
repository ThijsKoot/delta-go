package delta

import "fmt"

// A Enum used for selecting the partition value operation when filtering a DeltaTable partition.
type PartitionValue[T any] struct {
	// The partition value with the equal operator
	Equal *T
	// The partition value with the not equal operator
	NotEqual *T
	// The partition value with the greater than operator
	GreaterThan *T
	// The partition value with the greater than or equal operator
	GreaterThanOrEqual *T
	// The partition value with the less than operator
	LessThan *T
	// The partition value with the less than or equal operator
	LessThanOrEqual *T
	// The partition values with the in operator
	In *[]T
	// The partition values with the not in operator
	NotIn *[]T
}

// A Struct TablePartition used to represent a partition of a DeltaTable.
type TablePartition struct {
	// The key of the DeltaTable partition.
	Key string
	// The value of the DeltaTable partition.
	Value string
}

type PartitionFilter[T any] struct {
	Key   string
	Value PartitionValue[T]
}

func NewPartitionFilter[T any](key, operator string, value *T, values *[]T) (*PartitionFilter[T], error) {
	if key == "" {
		return nil, fmt.Errorf("invalid partition filter '%s %s %s'", key, operator, value)
	}

	switch ot := getOperatorType(operator); ot {
	case singleValueOperator:
		if value == nil {
			return nil, fmt.Errorf("no value passed to partition filter")
		}
	case multiValueOperator:
		if values == nil {
			return nil, fmt.Errorf("no value passed to partition filter")
		}
	case invalidOperator:
		return nil, fmt.Errorf("unknown operator for partition filter '%s'", operator)
	}

	// create the actual operator. no defensive programming required here, checks have been done
	pv := PartitionValue[T]{}
	switch operator {
	case "=":
		pv.Equal = value
	case "!=":
		pv.NotEqual = value
	case ">":
		pv.GreaterThan = value
	case ">=":
		pv.GreaterThanOrEqual = value
	case "<":
		pv.LessThan = value
	case "<=":
		pv.LessThanOrEqual = value
	case "in":
		pv.In = values
	case "not in":
		pv.NotIn = values
	}

	filter := PartitionFilter[T]{
		Key:   key,
		Value: pv,
	}

	return &filter, nil
}

type operatorType int

const (
	singleValueOperator operatorType = iota
	multiValueOperator
	invalidOperator
)

func getOperatorType(operator string) operatorType {
	switch operator {
	case "=", "!=", ">", ">=", "<", "<=":
		return singleValueOperator
	case "in", "not in":
		return multiValueOperator
	default:
		return invalidOperator
	}
}
