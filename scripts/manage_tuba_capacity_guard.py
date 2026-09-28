#!/usr/bin/env python3
"""Start, stop and inspect the TUBA capacity guard without a service manager."""
import argparse, json, os, signal, subprocess, sys, time

ROOT = "/opt/tuba/collector-live/capacity-guard"
PID = os.path.join(ROOT, "run", "pid")
LOG = os.path.join(ROOT, "logs", "capacity-guard.log")
GUARD = os.path.join(ROOT, "tuba_capacity_guard.py")

def live(pid):
    try:
        executable = os.path.basename(os.path.realpath("/proc/%d/exe" % pid))
        return "python" in executable and GUARD.encode() in open("/proc/%d/cmdline" % pid, "rb").read()
    except OSError:
        return False

def current():
    try: return int(open(PID).read().strip())
    except (OSError, ValueError): return 0

def start():
    pid = current()
    if pid and live(pid): raise RuntimeError("capacity guard already running")
    os.makedirs(os.path.dirname(PID), mode=0o750, exist_ok=True); os.makedirs(os.path.dirname(LOG), mode=0o750, exist_ok=True)
    output = open(LOG, "ab", buffering=0)
    process = subprocess.Popen([sys.executable, GUARD, "--delete"], stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    output.close(); open(PID, "w").write(str(process.pid) + "\n"); os.chmod(PID, 0o600); time.sleep(2)
    if process.poll() is not None: raise RuntimeError("capacity guard exited; inspect log")
    print("capacity guard started pid=%d" % process.pid)

def stop():
    pid = current()
    if not pid or not live(pid): print("capacity guard stopped"); return
    os.killpg(pid, signal.SIGTERM)
    for _ in range(50):
        if not live(pid): break
        time.sleep(.2)
    if live(pid): raise RuntimeError("capacity guard did not stop")
    try: os.remove(PID)
    except OSError: pass
    print("capacity guard stopped")

def main():
    p=argparse.ArgumentParser(); p.add_argument("action", choices=("start","stop","status")); a=p.parse_args()
    try:
        if a.action == "start": start()
        elif a.action == "stop": stop()
        else:
            pid=current(); print("capacity guard pid=%d running"%pid if pid and live(pid) else "capacity guard stopped")
    except Exception as e: print("error: %s"%e,file=sys.stderr); return 1
    return 0
if __name__ == "__main__": sys.exit(main())
