// Package component implements the managed component supervisor core required
// by COL-09: signed component upgrade packages, state-format compatibility
// checks, guarded rollback, and a host-wide single-instance lock.
//
// Process supervision itself (crash backoff, graceful stop, process identity
// of the supervisor, per-service desired state) is deliberately NOT
// reimplemented here: managed components run under internal/launcher, and the
// upgrader operates on the on-disk version layout while the caller stops and
// restarts the component through the launcher (single-service granularity).
// Semantics shared with the launcher — fail-closed identity checks, positive
// evidence before acting — are reused from that package.
package component
