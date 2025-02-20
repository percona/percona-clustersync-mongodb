package errors

import (
	"errors"
	"fmt"
)

type wrappedError struct {
	cause error
	msg   string
}

func (w *wrappedError) Error() string { return w.msg + ": " + w.cause.Error() }
func (w *wrappedError) Unwrap() error { return w.cause }

// New calls [errors.New].
func New(text string) error {
	return errors.New(text) //nolint:err113
}

// Errorf calls [fmt.Errorf].
func Errorf(format string, vals ...any) error {
	return fmt.Errorf(format, vals...) //nolint:err113
}

func Wrap(cause error, text string) error {
	if cause == nil {
		return nil
	}

	return &wrappedError{cause: cause, msg: text}
}

func Wrapf(cause error, format string, vals ...any) error {
	if cause == nil {
		return nil
	}

	return &wrappedError{cause: cause, msg: fmt.Sprintf(format, vals...)}
}

// Unwrap calls [errors.Unwrap].
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Join calls [errors.Join].
func Join(errs ...error) error {
	return errors.Join(errs...)
}

// Is calls [errors.Is].
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As calls [errors.As].
func As(err error, target any) bool {
	return errors.As(err, target)
}
