package storage

import "fmt"

type ErrNotFound struct {
	Inner error
}

func (e *ErrNotFound) Error() string {
	return "object not found"
}

func (e *ErrNotFound) Unwrap() error {
	return e.Inner
}

type ErrAlreadyExists struct {
	Inner error
	Path  string
}

func (e *ErrAlreadyExists) Error() string {
	return fmt.Sprintf("object already exists at path %s", e.Path)
}

func (e *ErrAlreadyExists) Unwrap() error {
	return e.Inner
}
