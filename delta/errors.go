package delta

import "fmt"

type ErrVersionAlreadyExists struct {
	Inner   error
	Version DeltaDataTypeVersion
}

func (e *ErrVersionAlreadyExists) Error() string {
	return fmt.Sprintf("table version %v already exists", e.Version)
}

func (e *ErrVersionAlreadyExists) Unwrap() error {
	return e.Inner
}
