package krv

import (
	"slices"
)

func errRepeat(n int, err error) []error {
	return slices.Repeat([]error{err}, n)
}
