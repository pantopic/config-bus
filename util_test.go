package pcb

func withGlobal(flag *bool, val bool, fn func()) {
	prev := *flag
	*flag = val
	if globalSet != nil {
		var gval = uint64(0)
		if val {
			gval = 1
		}
		globalSet(flagName[flag], gval)
		defer globalDel(flagName[flag])
	}
	fn()
	*flag = prev
}

func withGlobalInt(flag *int, val int, fn func()) {
	prev := *flag
	*flag = val
	if globalSet != nil {
		globalSet(flagNameInt[flag], uint64(val))
		defer globalDel(flagNameInt[flag])
	}
	fn()
	*flag = prev
}
