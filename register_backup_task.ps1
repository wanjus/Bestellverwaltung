# PowerShell script to register a daily Scheduled Task that runs the daily_backup.py
# Usage (run in an elevated PowerShell if needed):
#   .\register_backup_task.ps1 -PythonExe "C:\Path\to\python.exe" -Time "02:00"
param(
    [string]$PythonExe = "$env:USERPROFILE\\AppData\\Local\\Programs\\Python\\Python39\\python.exe",
    [string]$ScriptPath = "C:\tools\ki\coffeeproject\Bestellverwaltung\\daily_backup.py",
    [string]$Time = "02:00",
    [string]$TaskName = "BestellverwaltungBackup"
)

# Ensure script path is absolute
$ScriptPath = Resolve-Path $ScriptPath

$action = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$ScriptPath`" --keep 14"
$trigger = New-ScheduledTaskTrigger -Daily -At $Time
$principal = New-ScheduledTaskPrincipal -UserId "NT AUTHORITY\SYSTEM" -LogonType ServiceAccount -RunLevel Highest

try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Force
    Write-Host "Scheduled Task '$TaskName' erstellt: jeden Tag um $Time"
} catch {
    Write-Error "Fehler beim Erstellen des Scheduled Task: $_"
}
