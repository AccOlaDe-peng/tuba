package launcher

// ProcessIdentity returns the platform process identity used by the launcher
// state file (boot ID + start time on Linux, process creation token on
// Windows). It is exported so sibling supervisors (e.g. the managed component
// supervisor's instance lock) share the exact same identity semantics instead
// of growing a second implementation.
func ProcessIdentity(pid int) (string, error) {
	return processIdentity(pid)
}

// ProcessInstanceAlive reports whether pid is alive and still belongs to the
// process that recorded expectedIdentity. PID-only state and identity
// mismatches fail closed, matching the launcher supervisor's stale-state
// rules.
func ProcessInstanceAlive(pid int, expectedIdentity string) bool {
	return processInstanceAlive(pid, expectedIdentity)
}
