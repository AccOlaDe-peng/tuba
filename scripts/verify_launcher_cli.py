#!/usr/bin/env python3
"""Exercise the packaged Launcher CLI with an isolated signal-aware helper."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]
HELPER_SOURCE = r'''package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func appendLine(path, value string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil { os.Exit(81) }
	defer file.Close()
	if _, err := fmt.Fprintln(file, value); err != nil { os.Exit(82) }
}

func main() {
	if len(os.Args) != 3 { os.Exit(83) }
	appendLine(os.Args[1], "ready")
	fmt.Println("launcher helper ready")
	stops := make(chan os.Signal, 1)
	signal.Notify(stops, os.Interrupt, syscall.SIGTERM)
	<-stops
	appendLine(os.Args[2], "stopped")
	fmt.Println("launcher helper stopped")
}
'''


def invoke(binary: pathlib.Path, *args: str, timeout: float = 15) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(binary), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Launcher command failed ({args[0]}): exit={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return result


def line_count(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def wait_until(predicate, message: str, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(message)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="tuba-launcher-cli-") as temp:
        work = pathlib.Path(temp)
        suffix = ".exe" if os.name == "nt" else ""
        launcher = work / f"tuba-launcher{suffix}"
        helper_source = work / "helper.go"
        helper = work / f"helper{suffix}"
        helper_source.write_text(HELPER_SOURCE, encoding="utf-8")
        subprocess.run(["go", "build", "-trimpath", "-o", str(launcher), "./cmd/tuba-launcher"], cwd=ROOT, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(helper), str(helper_source)], cwd=ROOT, check=True)

        state_dir = work / "state"
        log_dir = work / "logs"
        ready_path = work / "ready.count"
        stopped_path = work / "stopped.count"
        manifest_path = work / "tuba-services.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "state_dir": str(state_dir),
                    "log_dir": str(log_dir),
                    "services": [
                        {
                            "name": "lifecycle-helper",
                            "command": str(helper),
                            "args": [str(ready_path), str(stopped_path)],
                            "restart_min": "100ms",
                            "restart_max": "1s",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        started = False
        try:
            validated = invoke(launcher, "validate", "--manifest", str(manifest_path))
            if "manifest valid services=1" not in validated.stdout:
                raise AssertionError(f"Unexpected validate result: {validated.stdout}")

            invalid_manifest_path = work / "missing-environment.json"
            invalid_manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "state_dir": str(work / "invalid-state"),
                        "log_dir": str(work / "invalid-logs"),
                        "services": [
                            {
                                "name": "invalid-helper",
                                "command": str(helper),
                                "environment": {"TEST_API_KEY": "${TUBA_VALIDATE_MISSING_KEY_20260928}"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            rejected = subprocess.run(
                [str(launcher), "validate", "--manifest", str(invalid_manifest_path)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if rejected.returncode == 0 or "TUBA_VALIDATE_MISSING_KEY_20260928" not in rejected.stderr:
                raise AssertionError(
                    "Launcher validate accepted a missing secret reference or returned an unexpected diagnostic: "
                    f"exit={rejected.returncode}, stderr={rejected.stderr}"
                )

            invoke(launcher, "start", "--manifest", str(manifest_path))
            started = True
            wait_until(lambda: line_count(ready_path) >= 1, "helper did not start")
            status = invoke(launcher, "status", "--manifest", str(manifest_path))
            if "lifecycle-helper" not in status.stdout or "running" not in status.stdout:
                raise AssertionError(f"Launcher status did not show the managed helper: {status.stdout}")
            logs = invoke(launcher, "logs", "--service", "lifecycle-helper", "--manifest", str(manifest_path))
            if "launcher helper ready" not in logs.stdout:
                raise AssertionError(f"Launcher logs did not contain helper output: {logs.stdout}")

            invoke(launcher, "restart", "--manifest", str(manifest_path), timeout=30)
            wait_until(
                lambda: line_count(ready_path) >= 2 and line_count(stopped_path) >= 1,
                "restart did not gracefully stop and relaunch the helper",
            )
            invoke(launcher, "stop", "--manifest", str(manifest_path), "--timeout", "10s", timeout=15)
            started = False
            wait_until(lambda: line_count(stopped_path) >= 2, "stop did not run helper cleanup")
            status = invoke(launcher, "status", "--manifest", str(manifest_path))
            if "TUBA launcher is stopped" not in status.stdout or "stale state" in status.stdout:
                raise AssertionError(f"Stopped Launcher still appears active: {status.stdout}")
            print("Launcher CLI lifecycle passed: validate, start, status, logs, restart, graceful stop, and helper cleanup.")
        finally:
            if started:
                subprocess.run(
                    [str(launcher), "stop", "--manifest", str(manifest_path), "--timeout", "10s"],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )


if __name__ == "__main__":
    main()

