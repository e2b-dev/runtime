package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNil(t *testing.T) {
	t.Parallel()

	var nilPointer *int
	var nilMap map[string]int
	var nilFunc func()
	var nilChan chan int
	var nilSlice []int
	var nilError error
	value := 1
	for name, tc := range map[string]struct {
		in   any
		want bool
	}{
		"nil":           {nil, true},
		"nil pointer":   {nilPointer, true},
		"nil map":       {nilMap, true},
		"nil func":      {nilFunc, true},
		"nil chan":      {nilChan, true},
		"nil slice":     {nilSlice, true},
		"nil interface": {nilError, true},
		"pointer":       {&value, false},
		"value":         {value, false},
		"empty map":     {map[string]int{}, false},
		"empty slice":   {[]int{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, IsNil(tc.in))
		})
	}
}
