package delta

import (
	"fmt"

	"github.com/thijskoot/delta-go/types"
)

type ErrVersionAlreadyExists struct {
	Inner   error
	Version types.Version
}

func (e *ErrVersionAlreadyExists) Error() string {
	return fmt.Sprintf("table version %v already exists", e.Version)
}

func (e *ErrVersionAlreadyExists) Unwrap() error {
	return e.Inner
}
