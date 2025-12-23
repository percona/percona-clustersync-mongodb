package validate

import (
	"reflect"

	"github.com/dustin/go-humanize"
	"github.com/go-playground/validator/v10"
)

// validateByteSize checks if a string can be parsed as a byte size.
func validateByteSize(fl validator.FieldLevel) bool {
	s := getStringValue(fl.Field())
	if s == "" || s == "0" {
		return true // empty/zero = use default
	}

	_, err := humanize.ParseBytes(s)

	return err == nil
}

// validateByteSizeMin validates minimum byte size.
// Tag usage: bytesizemin=16MB
func validateByteSizeMin(fl validator.FieldLevel) bool {
	s := getStringValue(fl.Field())
	if s == "" || s == "0" {
		return true // empty/zero bypasses validation (use default)
	}

	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return false
	}

	minBytes, err := humanize.ParseBytes(fl.Param())
	if err != nil {
		return false
	}

	return bytes >= minBytes
}

// validateByteSizeMax validates maximum byte size.
// Tag usage: bytesizemax=64GiB
func validateByteSizeMax(fl validator.FieldLevel) bool {
	s := getStringValue(fl.Field())
	if s == "" || s == "0" {
		return true
	}

	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return false
	}

	maxBytes, err := humanize.ParseBytes(fl.Param())
	if err != nil {
		return false
	}

	return bytes <= maxBytes
}

func getStringValue(field reflect.Value) string {
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return ""
		}

		return field.Elem().String()
	}

	return field.String()
}
