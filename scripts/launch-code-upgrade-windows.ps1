param([Parameter(Mandatory=$true)][string]$Updater,[Parameter(Mandatory=$true)][string]$Candidate,[Parameter(Mandatory=$true)][string]$Sha256,[Parameter(Mandatory=$true)][string]$Version,[Parameter(Mandatory=$true)][string]$Result)
$ErrorActionPreference='Stop';if ((Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLowerInvariant() -ne $Sha256) { throw 'candidate checksum mismatch' }
$name="Remote Hosts Code Upgrade $($Sha256.Substring(0,12))";$arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File',('"'+$Updater+'"'),'-Candidate',('"'+$Candidate+'"'),'-Sha256',$Sha256,'-Version',$Version,'-Result',('"'+$Result+'"')) -join ' '
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arguments;$trigger=New-ScheduledTaskTrigger -Once -At ((Get-Date).AddSeconds(3));$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 15) -StartWhenAvailable
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings -Force|Out-Null;Start-ScheduledTask -TaskName $name
@{state='started';task=$name;result=$Result}|ConvertTo-Json -Compress
