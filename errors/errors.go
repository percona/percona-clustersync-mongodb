package errors

import (
	"errors"
	"fmt"
)

var ErrUnsupported = errors.ErrUnsupported

type wrappedError struct {
	cause error
	msg   string
}

func (w *wrappedError) Error() string {
	return w.msg + ": " + w.cause.Error()
}

func (w *wrappedError) Unwrap() error {
	return w.cause
}

// New calls [errors.New].
//
//go:inline
func New(text string) error {
	return errors.New(text) //nolint:err113
}

// Errorf calls [fmt.Errorf].
//
//go:inline
func Errorf(format string, vals ...any) error {
	return fmt.Errorf(format, vals...) //nolint:err113
}

func Wrap(cause error, text string) error {
	if cause == nil {
		return nil
	}

	if text == "" {
		return cause
	}

	return &wrappedError{cause: cause, msg: text}
}

func Wrapf(cause error, format string, vals ...any) error {
	if cause == nil {
		return nil
	}

	msg := fmt.Sprintf(format, vals...)
	if msg == "" {
		return cause
	}

	return &wrappedError{cause: cause, msg: msg}
}

// Unwrap calls [errors.Unwrap].
//
//go:inline
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Join calls [errors.Join].
//
//go:inline
func Join(errs ...error) error {
	return errors.Join(errs...)
}

// Is calls [errors.Is].
//
//go:inline
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As calls [errors.As].
//
//go:inline
func As(err error, target any) bool {
	return errors.As(err, target)
}
