#!/usr/bin/env bash
#
# Samples the running server's thread count and memory usage every second.
# This is THE tool for seeing the scale problem: /proc/<pid>/status is the
# same place `top`, `htop`, and `ps` all get their numbers from -- we're
# just reading it directly so you see exactly what's being measured.
#
#   Threads:   from /proc/<pid>/status -- one line per OS thread the kernel
#              is scheduling for this process. This is the number that
#              explodes under thread-per-connection and stays flat under epoll.
#   VmRSS:     "Resident Set Size" -- actual physical RAM this process is
#              using right now (not virtual/reserved, actual resident memory).
#
# Usage:
#   ./monitor_server.sh <pid> [interval_seconds]
#
# Find the pid with:  pgrep -f RedisServer
#
set -euo pipefail

PID="${1:?Usage: monitor_server.sh <pid> [interval_seconds]}"
INTERVAL="${2:-1}"

if [ ! -d "/proc/$PID" ]; then
    echo "No such process: $PID"
    exit 1
fi

echo "timestamp,threads,vmrss_kb"
while [ -d "/proc/$PID" ]; do
    THREADS=$(grep -oP '(?<=Threads:)\s*\K\d+' "/proc/$PID/status" 2>/dev/null || echo "0")
    VMRSS=$(grep -oP '(?<=VmRSS:)\s*\K\d+' "/proc/$PID/status" 2>/dev/null || echo "0")
    echo "$(date +%H:%M:%S),$THREADS,$VMRSS"
    sleep "$INTERVAL"
done

echo "Process $PID exited."
