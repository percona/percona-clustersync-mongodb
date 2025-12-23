// Package validate provides request validation using go-playground/validator.
package validate

import (
	"reflect"
	"strings"
	"sync"

	"github.com/go-playground/validator/v10"
)

var (
	instance *validator.Validate //nolint:gochecknoglobals
	once     sync.Once           //nolint:gochecknoglobals
)

// Validator returns the singleton validator instance.
func Validator() *validator.Validate {
	once.Do(func() {
		instance = validator.New(validator.WithRequiredStructEnabled())
		registerCustomValidators(instance)
		registerTagNameFunc(instance)
	})

	return instance
}

func registerCustomValidators(v *validator.Validate) {
	_ = v.RegisterValidation("bytesize", validateByteSize)
	_ = v.RegisterValidation("bytesizemin", validateByteSizeMin)
	_ = v.RegisterValidation("bytesizemax", validateByteSizeMax)
}

// registerTagNameFunc uses JSON tag names in error messages.
func registerTagNameFunc(v *validator.Validate) {
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return fld.Name
		}

		return name
	})
}

// Struct validates a struct using the singleton validator.
func Struct(s any) error {
	return TranslateErrors(Validator().Struct(s))
}
