param(
  [Parameter(Mandatory=$true)][string]$Candidate,
  [Parameter(Mandatory=$true)][string]$Sha256,
  [Parameter(Mandatory=$true)][string]$Version,
  [Parameter(Mandatory=$true)][string]$Result,
  [string]$BinaryPath="",
  [string]$ConfigPath="",
  [string]$TaskName="Remote Hosts Code Agent",
  [int]$IdleTimeout=180
)
$ErrorActionPreference='Stop'
if ($Sha256 -notmatch '^[0-9a-f]{64}$' -or $Version -notmatch '^\d+\.\d+\.\d+$') { throw 'invalid version/checksum' }
$record=[ordered]@{state='preflight';version=$Version;candidate_sha256=$Sha256;service_changed=$false}
$lease=[guid]::NewGuid().ToString('N').PadRight(64,'0').Substring(0,64)
$config=$null;$maintenance=$false;$backup=$null
$agentTask=Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$agentAction=@($agentTask.Actions)[0]
if ([string]::IsNullOrWhiteSpace($BinaryPath)) { $BinaryPath=[string]$agentAction.Execute }
if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
  $match=[regex]::Match([string]$agentAction.Arguments,'--config\s+(?:"([^"]+)"|(\S+))')
  if ($match.Success) {
    if ($match.Groups[1].Success) { $ConfigPath=$match.Groups[1].Value } else { $ConfigPath=$match.Groups[2].Value }
  } else {
    $preferred=Join-Path $env:LOCALAPPDATA 'RemoteHostsCode\agent.json'
    $legacy=Join-Path $env:USERPROFILE '.local\share\remote-hosts-code\agent.json'
    if (Test-Path -LiteralPath $preferred) { $ConfigPath=$preferred } elseif (Test-Path -LiteralPath $legacy) { $ConfigPath=$legacy } else { $ConfigPath=$preferred }
  }
}
$record.binary_path=$BinaryPath;$record.config_path=$ConfigPath
function Save-Receipt { param($Value) $parent=Split-Path -Parent $Result;New-Item -ItemType Directory -Force $parent|Out-Null;$Value|ConvertTo-Json -Depth 8|Set-Content -Encoding UTF8 $Result }
function Maintenance-Call { param([string]$Action) $body=@{action=$Action;lease_id=$lease;ttl_seconds=300}|ConvertTo-Json -Compress;$headers=@{Authorization=('Bearer '+$config.device_token)};Invoke-RestMethod -Method Post -Uri ($config.gateway_url+'/device/maintenance') -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 10|Out-Null }
function Gateway-Ready { param([string]$Expected,[long]$Baseline) $deadline=(Get-Date).AddSeconds(240);$samples=0;$session=$null;$last=$Baseline;$headers=@{Authorization=('Bearer '+$config.device_token);'Cache-Control'='no-cache'};while ((Get-Date) -lt $deadline) { try {$v=Invoke-RestMethod -Method Get -Uri ($config.gateway_url+'/device/readiness') -Headers $headers -TimeoutSec 10;if ($v.agent_version -eq $Expected -and $v.ready -eq $true -and [long]$v.last_seen -gt $last) {if ($session -ne $v.session) {$session=$v.session;$samples=1} else {$samples++};$last=[long]$v.last_seen;if ($samples -ge 3) {return @{gateway_verified=$true;session=$session;samples=$samples;last_seen=$last}}}} catch {};Start-Sleep -Seconds 2};throw 'gateway readiness did not converge' }
try {
  if ((Get-FileHash -Algorithm SHA256 $Candidate).Hash.ToLowerInvariant() -ne $Sha256) { throw 'candidate checksum mismatch' }
  if ((& $Candidate --version).Trim() -ne "remote-hosts-code $Version") { throw 'candidate version mismatch' }
  $config=Get-Content -Raw -LiteralPath $ConfigPath|ConvertFrom-Json
  $headers=@{Authorization=('Bearer '+$config.device_token);'Cache-Control'='no-cache'};$before=Invoke-RestMethod -Method Get -Uri ($config.gateway_url+'/device/readiness') -Headers $headers -TimeoutSec 10
  Maintenance-Call 'acquire';$maintenance=$true
  $deadline=(Get-Date).AddSeconds($IdleTimeout)
  do {
    $agents=@(Get-Process remote-hosts-code -ErrorAction SilentlyContinue)
    $agentIds=@($agents|ForEach-Object {$_.Id})
    $children=@(Get-CimInstance Win32_Process|Where-Object {$agentIds -contains $_.ParentProcessId -and $_.Name -notin @('conhost.exe','OpenConsole.exe')})
    $record.active_children=$children.Count
    if ($children.Count -eq 0) {break}
    if ((Get-Date) -ge $deadline) {throw 'agent still has active child work; upgrade not applied'}
    Start-Sleep -Seconds 2
  } while ($true)
  $backupRoot=Join-Path $env:LOCALAPPDATA ("remote-hosts-code\releases\before-$Version-"+(Get-Date -Format yyyyMMddTHHmmss)+'-'+[guid]::NewGuid().ToString('N').Substring(0,8));New-Item -ItemType Directory -Force $backupRoot|Out-Null;$backup=Join-Path $backupRoot 'remote-hosts-code.exe';Copy-Item -LiteralPath $BinaryPath -Destination $backup
  $record.backup=$backup;$record.previous_sha256=(Get-FileHash -Algorithm SHA256 $backup).Hash.ToLowerInvariant();$record.state='installing'
  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue;Start-Sleep -Seconds 2
  $temp="$BinaryPath.new";Copy-Item -LiteralPath $Candidate -Destination $temp -Force;Move-Item -LiteralPath $temp -Destination $BinaryPath -Force;$record.service_changed=$true
  if ((Get-FileHash -Algorithm SHA256 $BinaryPath).Hash.ToLowerInvariant() -ne $Sha256) {throw 'installed checksum mismatch'}
  Start-ScheduledTask -TaskName $TaskName
  $ready=Gateway-Ready $Version ([long]$before.last_seen);$record.state='upgraded';$record.installed_sha256=$Sha256;$record.gateway_verified=$true;$record.session=$ready.session;$record.samples=$ready.samples
} catch {
  $record.state='failed';$record.error=$_.Exception.Message
  if ($record.service_changed -and $backup) { try { Copy-Item -LiteralPath $backup -Destination $BinaryPath -Force;Start-ScheduledTask -TaskName $TaskName;$record.rollback='restored_previous_binary' } catch { $record.rollback='failed' } }
}
if ($maintenance) { try { Maintenance-Call 'release';$record.maintenance_released=$true } catch {$record.maintenance_released=$false} }
Save-Receipt $record;$record|ConvertTo-Json -Compress
if ($record.state -ne 'upgraded') { exit 1 }
