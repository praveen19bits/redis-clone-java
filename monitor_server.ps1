<#
.SYNOPSIS
    Samples a process's OS thread count and resident memory every N seconds.
    Windows equivalent of monitor_server.sh (which reads Linux's /proc/<pid>/status).

.DESCRIPTION
    Threads   -> $proc.Threads.Count, the number of OS threads Windows is
                 scheduling for this process. This is the number that
                 explodes under --mode=thread and stays flat under --mode=epoll.
    WorkingSet -> $proc.WorkingSet64, physical RAM actually resident right now.
                 This is Windows' equivalent of Linux's VmRSS.

.PARAMETER ProcessId
    The PID of the running java RedisServer process. Find it with:
        Get-Process java
    or, if you have multiple java processes and need to tell them apart:
        Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Select-Object ProcessId, CommandLine

.PARAMETER IntervalSeconds
    How often to sample, in seconds. Defaults to 1.

.EXAMPLE
    .\monitor_server.ps1 -ProcessId 12345 -IntervalSeconds 2

.EXAMPLE
    .\monitor_server.ps1 -ProcessId 12345 -IntervalSeconds 2 | Tee-Object -FilePath monitor-thread-mode.csv
#>

param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId,

    [int]$IntervalSeconds = 1
)

$proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
if (-not $proc) {
    Write-Error "No process found with PID $ProcessId"
    exit 1
}

Write-Output "timestamp,threads,workingset_kb"

while ($true) {
    $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $proc) {
        Write-Output "Process $ProcessId exited."
        break
    }

    $timestamp = Get-Date -Format "HH:mm:ss"
    $threads = $proc.Threads.Count
    $workingSetKb = [math]::Round($proc.WorkingSet64 / 1KB)

    Write-Output "$timestamp,$threads,$workingSetKb"
    Start-Sleep -Seconds $IntervalSeconds
}
