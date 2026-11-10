package testutils

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// SetNonZero sets v, a settable field of a generated query row, to a value
// other than its zero, so a test can tell whether code copied that field.
func SetNonZero(t *testing.T, v reflect.Value) {
	t.Helper()

	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		SetNonZero(t, v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		SetNonZero(t, v.Index(0))
	case reflect.Array:
		SetNonZero(t, v.Index(0))
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Unix(1, 0)))
		}
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("set")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	}

	require.False(t, v.IsZero(), "SetNonZero does not handle %s", v.Type())
}
