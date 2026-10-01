package launcher

import "fmt"

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

// QueryService returns the live status of one service in a manifest, failing
// closed when the supervisor is not alive: status rows written by a dead
// supervisor must never be presented as live evidence (same rule as status).
func QueryService(manifestPath, name string) (ServiceStatus, error) {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return ServiceStatus{}, err
	}
	if !manifest.hasService(name) {
		return ServiceStatus{}, fmt.Errorf("unknown service %q", name)
	}
	state, ok := supervisorRunning(manifest)
	if !ok {
		return ServiceStatus{}, fmt.Errorf("no live supervisor for manifest %s", manifest.path)
	}
	return state.Services[name], nil
}

// ServiceAlive reports whether the named service is supervised in state
// "running" and its recorded PID is an actually live process.
func ServiceAlive(manifestPath, name string) (bool, error) {
	status, err := QueryService(manifestPath, name)
	if err != nil {
		return false, err
	}
	return status.State == "running" && status.PID > 0 && processAlive(status.PID), nil
}
