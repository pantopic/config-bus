package krv

import (
	"github.com/pantopic/krv/internal"
)

func txnIntCompare(cond internal.Compare_CompareResult, a, b int64) bool {
	switch cond {
	case internal.Compare_EQUAL:
		return a == b
	case internal.Compare_GREATER:
		return a > b
	case internal.Compare_LESS:
		return a < b
	case internal.Compare_NOT_EQUAL:
		return a != b
	}
	return false
}

func withGlobal(flag *bool, val bool, fn func()) {
	prev := *flag
	*flag = val
	fn()
	*flag = prev
}

func withGlobalInt(flag *int, val int, fn func()) {
	prev := *flag
	*flag = val
	fn()
	*flag = prev
}
