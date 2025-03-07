package errors

import (
	"errors"
	"fmt"
)

// ErrUnsupported indicates an unsupported operation.
var ErrUnsupported = errors.ErrUnsupported

// wrappedError wraps an error with an additional message.
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

// New creates a new error with the given text.
//
// Calls [errors.New].
//
//go:inline
func New(text string) error {
	return errors.New(text) //nolint:err113
}

// Errorf formats an error according to a format specifier.
//
// Calls [fmt.Errorf].
//
//go:inline
func Errorf(format string, vals ...any) error {
	return fmt.Errorf(format, vals...) //nolint:err113
}

// Wrap adds a message to an existing error.
func Wrap(cause error, text string) error {
	if cause == nil {
		return nil
	}

	if text == "" {
		return cause
	}

	return &wrappedError{cause: cause, msg: text}
}

// Wrapf adds a formatted message to an existing error.
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

// Unwrap returns the cause of the error.
//
// Calls [errors.Unwrap].
//
//go:inline
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Join combines multiple errors into one.
//
// Calls [errors.Join].
//
//go:inline
func Join(errs ...error) error {
	return errors.Join(errs...)
}

// Is checks if an error matches a target error.
//
// Calls [errors.Is].
//
//go:inline
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As checks if an error can be cast to a target type.
//
// Calls [errors.As].
//
//go:inline
func As(err error, target any) bool {
	return errors.As(err, target)
}
