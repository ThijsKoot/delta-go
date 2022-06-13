package writer

import "fmt"

type ErrMissingPartitionColumn struct {
	Column string
}

func (e *ErrMissingPartitionColumn) Error() string {
	return fmt.Sprintf("missing partition value for column %s", e.Column)
}
