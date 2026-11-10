package utils

import "reflect"

// IsNil reports whether v is nil or holds a nil pointer, map, slice, func, channel or interface.
func IsNil(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return r.IsNil()
	}

	return false
}
