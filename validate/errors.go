package validate

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

// ValidationError represents a single validation error.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	var sb strings.Builder
	for i, err := range e {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(err.Error())
	}

	return sb.String()
}

// TranslateErrors converts validator.ValidationErrors to user-friendly messages.
func TranslateErrors(err error) error {
	if err == nil {
		return nil
	}

	validationErrors, ok := err.(validator.ValidationErrors) //nolint:errorlint
	if !ok {
		return err
	}

	var errs ValidationErrors
	for _, e := range validationErrors {
		errs = append(errs, ValidationError{
			Field:   e.Field(),
			Message: translateFieldError(e),
		})
	}

	return errs
}

func translateFieldError(e validator.FieldError) string {
	switch e.Tag() {
	case "required":
		return "is required"
	case "gte":
		return fmt.Sprintf("must be at least %s", e.Param())
	case "lte":
		return fmt.Sprintf("must be at most %s", e.Param())
	case "min":
		return fmt.Sprintf("must be at least %s", e.Param())
	case "max":
		return fmt.Sprintf("must be at most %s", e.Param())
	case "bytesize":
		return "must be a valid byte size (e.g., '100MB', '1GiB')"
	case "bytesizemin":
		return fmt.Sprintf("must be at least %s", e.Param())
	case "bytesizemax":
		return fmt.Sprintf("must be at most %s", e.Param())
	default:
		return fmt.Sprintf("failed %s validation", e.Tag())
	}
}
