package pcb

func withGlobal(flag *bool, val bool, fn func()) {
	prev := *flag
	*flag = val
	if globalOverride != nil {
		var gval = uint64(0)
		if val {
			gval = 1
		}
		globalOverride(flagName[flag], gval)
		defer func() {
			var gval = uint64(0)
			if prev {
				gval = 1
			}
			globalOverride(flagName[flag], uint64(gval))
		}()
	}
	fn()
	*flag = prev
}

func withGlobalInt(flag *int, val int, fn func()) {
	prev := *flag
	*flag = val
	if globalOverride != nil {
		globalOverride(flagNameInt[flag], uint64(val))
		defer globalOverride(flagNameInt[flag], uint64(prev))
	}
	fn()
	*flag = prev
}
