[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet(
        'Help', 'Install', 'Stage', 'Update', 'Start', 'Stop', 'Restart', 'Status',
        'Logs', 'Ui', 'Skills', 'Doctor', 'PrintConfig', 'Uninstall', 'RunApi',
        'RunConnector'
    )]
    [string]$Action = 'Help',

    [string]$Root,

    [switch]$Force,

    [switch]$PurgeData
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Script:SourceScriptPath = $PSCommandPath
$Script:PackageRoot = Split-Path -Parent $Script:SourceScriptPath
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = Join-Path $env:LOCALAPPDATA 'RemoteHosts'
}
$Script:Root = [IO.Path]::GetFullPath($Root)
$Script:ConfigDir = Join-Path $Script:Root 'config'
$Script:ConfigPath = Join-Path $Script:ConfigDir 'service.json'
$Script:DataDir = Join-Path $Script:Root 'data'
$Script:LogDir = Join-Path $Script:Root 'logs'
$Script:ReleaseDir = Join-Path $Script:Root 'releases'
$Script:BinDir = Join-Path $Script:Root 'bin'
$Script:ManagerPath = Join-Path $Script:BinDir 'remote-hosts-service.ps1'
$Script:ApiTaskName = 'Remote Hosts API'
$Script:ConnectorTaskName = 'Remote Hosts Connector'

function Show-Usage {
    @"
Remote Hosts Windows service manager

Usage:
  .\remote-hosts-service.ps1 <Action> [-Root <path>] [-Force] [-PurgeData]

Actions:
  Install       Stage this package, register per-user tasks, and start services
  Stage         Install a versioned release without restarting active services
  Update        Stage and restart after active conversations have drained
  Start         Start the API and connector scheduled tasks
  Stop          Stop both scheduled tasks
  Restart       Restart after active conversations have drained
  Status        Show task state, release, paths, and API health
  Logs          Follow API and connector logs
  Ui            Refresh only the hot-swappable admin page
  Skills        Install the Agent Skill into Codex and Antigravity
  Doctor        Run the installed binary's local diagnostics
  PrintConfig   Print service paths and the MCP command
  Uninstall     Remove scheduled tasks; add -PurgeData to delete local state

The tasks run as the current interactive user at logon so the connector can use
that user's SSH Agent, keys, configuration, and encrypted Remote Hosts vault.
"@
}

function Ensure-Directories {
    @(
        $Script:Root,
        $Script:ConfigDir,
        $Script:DataDir,
        $Script:LogDir,
        $Script:ReleaseDir,
        $Script:BinDir,
        (Join-Path $Script:DataDir 'artifacts'),
        (Join-Path $Script:DataDir 'ui')
    ) | ForEach-Object {
        [IO.Directory]::CreateDirectory($_) | Out-Null
    }
}

function ConvertTo-SqliteUrl([string]$Path) {
    $normalized = [IO.Path]::GetFullPath($Path).Replace('\', '/')
    return "sqlite://$normalized"
}

function ConvertTo-Hashtable($Value) {
    $result = @{}
    if ($null -eq $Value) {
        return $result
    }
    foreach ($property in $Value.PSObject.Properties) {
        $result[$property.Name] = $property.Value
    }
    return $result
}

function New-DefaultConfig {
    $databasePath = Join-Path $Script:DataDir 'remote-hosts.sqlite'
    return @{
        ConfigVersion = 1
        Root = $Script:Root
        DatabaseUrl = ConvertTo-SqliteUrl $databasePath
        Bind = '127.0.0.1:8787'
        ArtifactRoot = Join-Path $Script:DataDir 'artifacts'
        ConnectorId = [guid]::NewGuid().ToString().ToLowerInvariant()
        ConnectorName = 'local-windows-connector'
        EnvironmentId = [guid]::NewGuid().ToString().ToLowerInvariant()
        EnvironmentName = 'local-windows'
        CurrentNetwork = 'local'
        HostKeyPolicy = 'add'
        ConnectTimeoutSeconds = 10
        SshBackend = 'russh'
        PtyBackendMode = 'auto'
        VaultMasterPasswordFile = Join-Path $Script:ConfigDir 'vault-master-password'
        AdminHtmlPath = Join-Path (Join-Path $Script:DataDir 'ui') 'admin.html'
        KnownHostsPath = ''
        RusshInactivityTimeoutSeconds = 30
        MaxConcurrentOperations = 16
        RustLog = 'remote_hosts=info,remote_hosts_cli=info'
        BinaryPath = ''
        LauncherPath = Join-Path $Script:BinDir 'remote-hosts-launcher.exe'
        CurrentBinaryPointer = Join-Path $Script:ConfigDir 'current-binary.txt'
        ReleaseId = ''
        AdminHtmlSource = ''
        SkillSource = ''
    }
}

function Read-Config {
    if (-not (Test-Path -LiteralPath $Script:ConfigPath)) {
        throw "Remote Hosts is not configured at $Script:ConfigPath; run Install first"
    }
    $raw = Get-Content -LiteralPath $Script:ConfigPath -Raw | ConvertFrom-Json
    return ConvertTo-Hashtable $raw
}

function Write-Config([hashtable]$Config) {
    Ensure-Directories
    $temporary = "$($Script:ConfigPath).tmp.$PID"
    $Config | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $Script:ConfigPath -Force
}

function Write-BinaryPointer([hashtable]$Config) {
    $path = [string]$Config['CurrentBinaryPointer']
    $temporary = "$path.tmp.$PID"
    [string]$Config['BinaryPath'] | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $path -Force
}

function Merge-ConfigDefaults([hashtable]$Config) {
    $defaults = New-DefaultConfig
    foreach ($key in $defaults.Keys) {
        if (-not $Config.ContainsKey($key)) {
            $Config[$key] = $defaults[$key]
        }
    }
    $Config['ConfigVersion'] = 1
    $Config['Root'] = $Script:Root
    return $Config
}

function Ensure-Vault([hashtable]$Config) {
    $path = [string]$Config['VaultMasterPasswordFile']
    [IO.Directory]::CreateDirectory((Split-Path -Parent $path)) | Out-Null
    if (-not (Test-Path -LiteralPath $path) -or (Get-Item -LiteralPath $path).Length -eq 0) {
        $bytes = New-Object byte[] 32
        $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
        try {
            $generator.GetBytes($bytes)
        }
        finally {
            $generator.Dispose()
        }
        [Convert]::ToBase64String($bytes) | Set-Content -LiteralPath $path -Encoding ASCII
        $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        & icacls.exe $path '/inheritance:r' "/grant:r" "${identity}:(F)" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "failed to restrict vault key permissions at $path"
        }
    }
}

function Get-PackageReleaseId {
    $manifestPath = Join-Path $Script:PackageRoot 'release.json'
    if (Test-Path -LiteralPath $manifestPath) {
        $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
        $version = [string]$manifest.version
        $commit = [string]$manifest.commit
        if (-not [string]::IsNullOrWhiteSpace($version) -and -not [string]::IsNullOrWhiteSpace($commit)) {
            return "$version-$commit"
        }
    }
    $sourceBinary = Join-Path $Script:PackageRoot 'remote-hosts.exe'
    $digest = (Get-FileHash -LiteralPath $sourceBinary -Algorithm SHA256).Hash.ToLowerInvariant()
    return "local-$($digest.Substring(0, 12))"
}

function Copy-Directory([string]$Source, [string]$Destination) {
    if (-not (Test-Path -LiteralPath $Source -PathType Container)) {
        throw "required directory is missing: $Source"
    }
    if (Test-Path -LiteralPath $Destination) {
        Remove-Item -LiteralPath $Destination -Recurse -Force
    }
    [IO.Directory]::CreateDirectory((Split-Path -Parent $Destination)) | Out-Null
    Copy-Item -LiteralPath $Source -Destination $Destination -Recurse -Force
}

function Install-AdminUi([hashtable]$Config) {
    $source = [string]$Config['AdminHtmlSource']
    $destination = [string]$Config['AdminHtmlPath']
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "admin UI source is missing: $source"
    }
    [IO.Directory]::CreateDirectory((Split-Path -Parent $destination)) | Out-Null
    $temporary = "$destination.tmp.$PID"
    Copy-Item -LiteralPath $source -Destination $temporary -Force
    Move-Item -LiteralPath $temporary -Destination $destination -Force
}

function Install-Skills([hashtable]$Config) {
    $source = [string]$Config['SkillSource']
    if (-not (Test-Path -LiteralPath (Join-Path $source 'SKILL.md') -PathType Leaf)) {
        throw "Remote Hosts Agent Skill is missing: $source"
    }
    $codex = Join-Path $env:USERPROFILE '.codex\skills\remote-hosts-agent'
    $antigravity = Join-Path $env:USERPROFILE '.gemini\config\skills\remote-hosts-agent'
    Copy-Directory $source $codex
    Copy-Directory $source $antigravity
}

function Invoke-Installed([hashtable]$Config, [string[]]$Arguments) {
    $binary = [string]$Config['BinaryPath']
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
        throw "installed Remote Hosts binary is missing: $binary"
    }
    & $binary @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "remote-hosts exited with status $LASTEXITCODE"
    }
}

function Initialize-Database([hashtable]$Config) {
    Invoke-Installed $Config @('migrate', '--database-url', [string]$Config['DatabaseUrl'])
    Invoke-Installed $Config @(
        'bootstrap-connector',
        '--database-url', [string]$Config['DatabaseUrl'],
        '--connector-id', [string]$Config['ConnectorId'],
        '--connector-name', [string]$Config['ConnectorName'],
        '--environment-id', [string]$Config['EnvironmentId'],
        '--environment-name', [string]$Config['EnvironmentName'],
        '--environment-kind', 'home-lan',
        '--trust-level', 'owned',
        '--environment-description', 'Local Windows Task Scheduler environment',
        '--environment-notes', 'Created by remote-hosts-service.ps1',
        '--current-network', [string]$Config['CurrentNetwork']
    )
}

function Stage-Release {
    Ensure-Directories
    $sourceBinary = Join-Path $Script:PackageRoot 'remote-hosts.exe'
    $sourceLauncher = Join-Path $Script:PackageRoot 'remote-hosts-launcher.exe'
    $sourceUi = Join-Path $Script:PackageRoot 'admin.html'
    $sourceSkill = Join-Path $Script:PackageRoot 'skills\remote-hosts-agent'
    foreach ($required in @($sourceBinary, $sourceLauncher, $sourceUi, (Join-Path $sourceSkill 'SKILL.md'))) {
        if (-not (Test-Path -LiteralPath $required)) {
            throw "Windows package is incomplete; missing $required"
        }
    }

    $releaseId = Get-PackageReleaseId
    $releasePath = Join-Path $Script:ReleaseDir $releaseId
    [IO.Directory]::CreateDirectory($releasePath) | Out-Null
    $releaseBinary = Join-Path $releasePath 'remote-hosts.exe'
    if (Test-Path -LiteralPath $releaseBinary) {
        $sourceHash = (Get-FileHash -LiteralPath $sourceBinary -Algorithm SHA256).Hash
        $installedHash = (Get-FileHash -LiteralPath $releaseBinary -Algorithm SHA256).Hash
        if ($sourceHash -ne $installedHash) {
            throw "release id collision at $releasePath"
        }
    }
    else {
        Copy-Item -LiteralPath $sourceBinary -Destination $releaseBinary -Force
    }
    Copy-Item -LiteralPath $sourceUi -Destination (Join-Path $releasePath 'admin.html') -Force
    Copy-Directory $sourceSkill (Join-Path $releasePath 'skills\remote-hosts-agent')
    Copy-Item -LiteralPath $Script:SourceScriptPath -Destination $Script:ManagerPath -Force
    $stableLauncher = Join-Path $Script:BinDir 'remote-hosts-launcher.exe'
    if (-not (Test-Path -LiteralPath $stableLauncher -PathType Leaf)) {
        Copy-Item -LiteralPath $sourceLauncher -Destination $stableLauncher -Force
    }

    if (Test-Path -LiteralPath $Script:ConfigPath) {
        $config = Merge-ConfigDefaults (Read-Config)
    }
    else {
        $config = New-DefaultConfig
    }
    $config['BinaryPath'] = $releaseBinary
    $config['ReleaseId'] = $releaseId
    $config['AdminHtmlSource'] = Join-Path $releasePath 'admin.html'
    $config['SkillSource'] = Join-Path $releasePath 'skills\remote-hosts-agent'
    Ensure-Vault $config
    Write-Config $config
    Write-BinaryPointer $config
    Initialize-Database $config | Out-Null
    Install-AdminUi $config
    Install-Skills $config
    return $config
}

function New-ServiceTask([string]$TaskName, [string]$RunAction) {
    $powerShell = Join-Path $PSHOME 'powershell.exe'
    $arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$($Script:ManagerPath)`" -Action $RunAction -Root `"$($Script:Root)`""
    $taskAction = New-ScheduledTaskAction -Execute $powerShell -Argument $arguments -WorkingDirectory $Script:Root
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $identity
    $principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -RestartCount 999 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero)
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $taskAction `
        -Trigger $trigger `
        -Principal $principal `
        -Settings $settings `
        -Description 'Remote Hosts per-user background service' `
        -Force | Out-Null
}

function Register-ServiceTasks {
    if (-not (Test-Path -LiteralPath $Script:ManagerPath -PathType Leaf)) {
        throw "installed service manager is missing: $Script:ManagerPath"
    }
    New-ServiceTask $Script:ApiTaskName 'RunApi'
    New-ServiceTask $Script:ConnectorTaskName 'RunConnector'
}

function Start-ServiceTasks {
    foreach ($taskName in @($Script:ApiTaskName, $Script:ConnectorTaskName)) {
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        if ($null -eq $task) {
            throw "scheduled task is not installed: $taskName"
        }
        Start-ScheduledTask -TaskName $taskName
    }
}

function Stop-ServiceTasks {
    foreach ($taskName in @($Script:ConnectorTaskName, $Script:ApiTaskName)) {
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        if ($null -ne $task -and $task.State -ne 'Ready') {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        }
    }
}

function Assert-RestartSafe([hashtable]$Config) {
    if ($Force) {
        return
    }
    Invoke-Installed $Config @(
        'restart-readiness',
        '--database-url', [string]$Config['DatabaseUrl']
    )
}

function Restart-ServiceTasks([hashtable]$Config) {
    Assert-RestartSafe $Config
    Stop-ServiceTasks
    Start-Sleep -Milliseconds 500
    Start-ServiceTasks
}

function Run-ApiService {
    $config = Read-Config
    $env:RUST_LOG = [string]$config['RustLog']
    $env:REMOTE_HOSTS_ADMIN_HTML_PATH = [string]$config['AdminHtmlPath']
    $arguments = @(
        'serve',
        '--database-url', [string]$config['DatabaseUrl'],
        '--bind', [string]$config['Bind'],
        '--vault-master-password-file', [string]$config['VaultMasterPasswordFile']
    )
    $stdout = Join-Path $Script:LogDir 'api.out.log'
    $stderr = Join-Path $Script:LogDir 'api.err.log'
    & ([string]$config['BinaryPath']) @arguments 1>> $stdout 2>> $stderr
    exit $LASTEXITCODE
}

function Run-ConnectorService {
    $config = Read-Config
    $env:RUST_LOG = [string]$config['RustLog']
    $arguments = @(
        'worker-daemon',
        '--database-url', [string]$config['DatabaseUrl'],
        '--connector-id', [string]$config['ConnectorId'],
        '--current-network', [string]$config['CurrentNetwork'],
        '--host-key-policy', [string]$config['HostKeyPolicy'],
        '--connect-timeout-seconds', [string]$config['ConnectTimeoutSeconds'],
        '--ssh-backend', 'russh',
        '--pty-backend-mode', 'auto',
        '--vault-master-password-file', [string]$config['VaultMasterPasswordFile'],
        '--russh-inactivity-timeout-seconds', [string]$config['RusshInactivityTimeoutSeconds'],
        '--max-concurrent-operations', [string]$config['MaxConcurrentOperations'],
        '--artifact-root', [string]$config['ArtifactRoot']
    )
    if (-not [string]::IsNullOrWhiteSpace([string]$config['KnownHostsPath'])) {
        $arguments += @('--known-hosts-path', [string]$config['KnownHostsPath'])
    }
    $stdout = Join-Path $Script:LogDir 'connector.out.log'
    $stderr = Join-Path $Script:LogDir 'connector.err.log'
    & ([string]$config['BinaryPath']) @arguments 1>> $stdout 2>> $stderr
    exit $LASTEXITCODE
}

function Show-Status {
    $config = Read-Config
    foreach ($taskName in @($Script:ApiTaskName, $Script:ConnectorTaskName)) {
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        if ($null -eq $task) {
            Write-Output "$taskName`: not installed"
        }
        else {
            $info = Get-ScheduledTaskInfo -TaskName $taskName
            Write-Output "$taskName`: $($task.State), last_result=$($info.LastTaskResult), last_run=$($info.LastRunTime)"
        }
    }
    try {
        Invoke-RestMethod -Uri "http://$($config['Bind'])/v1/command-profiles" -TimeoutSec 3 | Out-Null
        Write-Output 'API health: reachable'
    }
    catch {
        Write-Output "API health: unavailable ($($_.Exception.Message))"
    }
    Write-Output "Release: $($config['ReleaseId'])"
    Write-Output "Binary:  $($config['BinaryPath'])"
    Write-Output "Data:    $Script:DataDir"
    Write-Output "Logs:    $Script:LogDir"
}

function Show-Logs {
    Ensure-Directories
    $paths = @(
        (Join-Path $Script:LogDir 'api.out.log'),
        (Join-Path $Script:LogDir 'api.err.log'),
        (Join-Path $Script:LogDir 'connector.out.log'),
        (Join-Path $Script:LogDir 'connector.err.log')
    )
    foreach ($path in $paths) {
        if (-not (Test-Path -LiteralPath $path)) {
            New-Item -ItemType File -Path $path | Out-Null
        }
    }
    Get-Content -LiteralPath $paths -Tail 80 -Wait
}

function Show-Config {
    $config = Read-Config
    Write-Output "Root:       $Script:Root"
    Write-Output "Config:     $Script:ConfigPath"
    Write-Output "Binary:     $($config['BinaryPath'])"
    Write-Output "Launcher:   $($config['LauncherPath'])"
    Write-Output "Database:   $($config['DatabaseUrl'])"
    Write-Output "Artifacts:  $($config['ArtifactRoot'])"
    Write-Output "Logs:       $Script:LogDir"
    Write-Output "Admin UI:   http://$($config['Bind'])/admin"
    Write-Output 'MCP command:'
    Write-Output "  `"$($config['LauncherPath'])`" -- mcp-stdio --database-url `"$($config['DatabaseUrl'])`" --tool-profile agent --vault-master-password-file `"$($config['VaultMasterPasswordFile'])`" --artifact-root `"$($config['ArtifactRoot'])`""
}

function Uninstall-Services {
    if (Test-Path -LiteralPath $Script:ConfigPath) {
        Assert-RestartSafe (Read-Config)
    }
    Stop-ServiceTasks
    foreach ($taskName in @($Script:ConnectorTaskName, $Script:ApiTaskName)) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    }
    if ($PurgeData) {
        Remove-Item -LiteralPath $Script:Root -Recurse -Force
        Write-Output "Removed Remote Hosts tasks and data at $Script:Root"
    }
    else {
        Write-Output "Removed Remote Hosts tasks; local data remains at $Script:Root"
    }
}

try {
    switch ($Action) {
        'Help' { Show-Usage }
        'Install' {
            Stage-Release | Out-Null
            Register-ServiceTasks
            Start-ServiceTasks
            Show-Config
        }
        'Stage' {
            Stage-Release | Out-Null
            Show-Config
        }
        'Update' {
            $config = Stage-Release
            Register-ServiceTasks
            Restart-ServiceTasks $config
            Show-Status
        }
        'Start' { Start-ServiceTasks }
        'Stop' { Stop-ServiceTasks }
        'Restart' { Restart-ServiceTasks (Read-Config) }
        'Status' { Show-Status }
        'Logs' { Show-Logs }
        'Ui' {
            Install-AdminUi (Read-Config)
            Write-Output 'Admin UI refreshed; reload /admin in the browser'
        }
        'Skills' {
            Install-Skills (Read-Config)
            Write-Output 'Remote Hosts Agent Skill installed for Codex and Antigravity'
        }
        'Doctor' { Invoke-Installed (Read-Config) @('doctor') }
        'PrintConfig' { Show-Config }
        'Uninstall' { Uninstall-Services }
        'RunApi' { Run-ApiService }
        'RunConnector' { Run-ConnectorService }
    }
}
catch {
    Write-Error $_
    exit 1
}
