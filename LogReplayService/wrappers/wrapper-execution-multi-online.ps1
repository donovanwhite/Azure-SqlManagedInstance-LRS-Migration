[CmdletBinding()]
param(
    [string]$TenantId,
    [string]$SubscriptionId,
    [bool]$AutoReauthenticate = $true,
    [switch]$IncludeDiffs,
    [string]$ResourceGroupName = 'rg_sql_dev_zan',
    [string]$ManagedInstanceName = 'dev-sql-mi-001',
    [string]$StorageAccountName = 'adlssqlbackups',
    [string]$BackupRootPath = 'C:\SqlBackups',
    [ValidateSet('EntraAzCli', 'EntraDevice', 'Sas')]
    [string]$StorageAuthMode = 'EntraAzCli',
    [string]$StorageContainerSasToken,
    [string[]]$SelectedInstanceNames,
    [string[]]$SelectedDatabaseNames,
    [int]$TransferPollSeconds = 300,
    [int]$StatusIntervalMinutes = 15,
    [int]$InitialUploadTimeoutMinutes = 30,
    [int]$CutoverCandidateCount = 3,
    [Nullable[datetime]]$ScheduledCutoverLocalTime,
    [string]$TransferStatePath,
    [string]$TransferOutputPath,
    [string]$TransferErrorPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$eventSourceScriptName = Split-Path -Leaf $PSCommandPath

function Get-ContainerNameFromInstanceFolder {
    param([string]$Value)

    $normalized = $Value.ToLowerInvariant()
    $normalized = $normalized -replace '[^a-z0-9-]', '-'
    $normalized = $normalized -replace '-+', '-'
    $normalized = $normalized.Trim('-')

    if ($normalized.Length -gt 63) {
        $normalized = $normalized.Substring(0, 63).Trim('-')
    }

    return $normalized
}

function Get-InstanceFolders {
    param([string]$RootPath)

    return Get-ChildItem -LiteralPath $RootPath -Directory | Where-Object {
        Get-ChildItem -LiteralPath $_.FullName -Directory -ErrorAction SilentlyContinue | Select-Object -First 1
    }
}

function Get-DatabaseNames {
    param([string]$InstancePath)

    return Get-ChildItem -LiteralPath $InstancePath -Directory | Select-Object -ExpandProperty Name
}

function Select-InstanceFolders {
    param(
        [System.IO.DirectoryInfo[]]$InstanceFolderList,
        [string[]]$RequestedNames
    )

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return ,@($InstanceFolderList)
    }

    $requestedLookup = $RequestedNames | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $selectedFolders = @($InstanceFolderList | Where-Object { $_.Name -in $requestedLookup })
    $missingNames = $requestedLookup | Where-Object { $_ -notin ($selectedFolders | Select-Object -ExpandProperty Name) }
    if ($missingNames) {
        throw "Requested instance folder(s) were not found under '$BackupRootPath': $($missingNames -join ', ')"
    }

    return ,@($selectedFolders)
}

function Select-DatabaseNames {
    param(
        [string[]]$DatabaseNameList,
        [string[]]$RequestedNames
    )

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return ,@($DatabaseNameList)
    }

    $requestedLookup = $RequestedNames | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    return ,@($DatabaseNameList | Where-Object { $_ -in $requestedLookup })
}

function Get-BackupFolderPath {
    param(
        [string]$DatabaseRootPath,
        [string[]]$CandidateNames
    )

    foreach ($candidateName in $CandidateNames) {
        $candidatePath = Join-Path -Path $DatabaseRootPath -ChildPath $candidateName
        if (Test-Path -LiteralPath $candidatePath) {
            return $candidatePath
        }
    }

    return $null
}

function Get-OrderedBackupFiles {
    param(
        [string]$FolderPath,
        [string[]]$Extensions
    )

    if (-not $FolderPath -or -not (Test-Path -LiteralPath $FolderPath)) {
        return @()
    }

    $files = Get-ChildItem -LiteralPath $FolderPath -File -ErrorAction SilentlyContinue | Where-Object {
        $Extensions -contains $_.Extension.ToLowerInvariant()
    } | Sort-Object -Property Name

    $files = @($files)

    if (-not $files) {
        return @()
    }

    return ,@($files)
}

function Get-LatestFullBackupFile {
    param([string]$DatabaseRootPath)

    $fullFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('FULL', 'Full', 'full')
    $fullFiles = Get-OrderedBackupFiles -FolderPath $fullFolderPath -Extensions @('.bak')
    if (-not $fullFiles -or $fullFiles.Count -eq 0) {
        throw "No full backup file was found under '$DatabaseRootPath'."
    }

    return $fullFiles[$fullFiles.Count - 1]
}

function Get-ApplicableDiffBackupFile {
    param([string]$DatabaseRootPath, [datetime]$FullBackupTime)

    $diffFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('DIFF', 'Diff', 'diff')
    $diffFiles = Get-OrderedBackupFiles -FolderPath $diffFolderPath -Extensions @('.bak', '.diff', '.dif')
    if (-not $diffFiles -or $diffFiles.Count -eq 0) {
        return $null
    }

    $applicableDiffs = @($diffFiles | Where-Object { $_.LastWriteTime -ge $FullBackupTime })
    if (-not $applicableDiffs -or $applicableDiffs.Count -eq 0) {
        return $null
    }

    return $applicableDiffs[$applicableDiffs.Count - 1]
}

function Get-ApplicableLogFiles {
    param([string]$DatabaseRootPath, [datetime]$AnchorTime, [datetime]$LastUploadedUtc)

    $logFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('LOG', 'Log', 'log', 'TRAN', 'Tran', 'tran', 'TLOG', 'tlog', 'Logs', 'logs')
    $logFiles = Get-OrderedBackupFiles -FolderPath $logFolderPath -Extensions @('.trn')
    if (-not $logFiles -or $logFiles.Count -eq 0) {
        return @()
    }

    $applicableLogs = @($logFiles | Where-Object { $_.LastWriteTime -ge $AnchorTime.AddSeconds(-1) })
    if ($LastUploadedUtc) {
        $applicableLogs = @($applicableLogs | Where-Object { $_.LastWriteTime -le $LastUploadedUtc.ToLocalTime().AddSeconds(1) })
    }

    if (-not $applicableLogs) {
        return @()
    }

    return ,@($applicableLogs)
}

function Get-StateValue {
    param(
        [object]$StateObject,
        [string]$PropertyName,
        [string]$Key
    )

    if (-not $StateObject) {
        return $null
    }

    $property = $StateObject.PSObject.Properties[$PropertyName]
    if (-not $property) {
        return $null
    }

    $valueObject = $property.Value
    if (-not $valueObject) {
        return $null
    }

    if ($valueObject -is [hashtable]) {
        if ($valueObject.ContainsKey($Key)) {
            return $valueObject[$Key]
        }

        return $null
    }

    $match = $valueObject.PSObject.Properties | Where-Object { $_.Name -eq $Key } | Select-Object -First 1
    if ($match) {
        return $match.Value
    }

    return $null
}

function Load-TransferState {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    } catch {
        Write-Warning "Unable to read transfer state '$Path'. $($_.Exception.Message)"
        return $null
    }
}

function Get-TransferLastSyncUtc {
    param([object]$State, [string]$StateKey)

    $rawValue = Get-StateValue -StateObject $State -PropertyName 'LastSyncUtc' -Key $StateKey
    if (-not $rawValue) {
        return $null
    }

    if ($rawValue -is [datetime]) {
        return ([datetime]$rawValue).ToUniversalTime()
    }

    $rawText = [string]$rawValue
    $parsedValue = [datetime]::MinValue
    if ([datetime]::TryParse($rawText, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind, [ref]$parsedValue)) {
        return $parsedValue.ToUniversalTime()
    }

    if ([datetime]::TryParse($rawText, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal, [ref]$parsedValue)) {
        return $parsedValue.ToUniversalTime()
    }

    return [datetime]::Parse($rawText).ToUniversalTime()
}

function Get-TransferShellPath {
    $pwshCommand = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($pwshCommand) {
        return $pwshCommand.Source
    }

    $windowsPowerShell = Get-Command powershell.exe -ErrorAction SilentlyContinue
    if ($windowsPowerShell) {
        return $windowsPowerShell.Source
    }

    throw 'Unable to locate pwsh or powershell.exe for the background transfer process.'
}

function Resolve-AzCopyExecutablePath {
    $azCopyCommand = Get-Command azcopy -ErrorAction SilentlyContinue
    if ($azCopyCommand -and $azCopyCommand.Source -and (Test-Path -LiteralPath $azCopyCommand.Source)) {
        return $azCopyCommand.Source
    }

    $candidateRoots = @(
        (Join-Path -Path $env:LOCALAPPDATA -ChildPath 'Microsoft\WinGet\Packages'),
        (Join-Path -Path $env:ProgramFiles -ChildPath 'WindowsApps')
    )

    foreach ($root in $candidateRoots) {
        if (-not $root -or -not (Test-Path -LiteralPath $root)) {
            continue
        }

        $candidate = Get-ChildItem -Path $root -Filter 'azcopy.exe' -File -Recurse -ErrorAction SilentlyContinue |
            Sort-Object -Property FullName -Descending |
            Select-Object -First 1

        if ($candidate) {
            return $candidate.FullName
        }
    }

    throw 'AzCopy is not available in PATH. Install AzCopy before running the online wrapper.'
}

function Ensure-AzCopyDeviceLogin {
    param([string]$Tenant)

    $azCopyPath = Resolve-AzCopyExecutablePath
    $loginArgs = @('login')
    if ($Tenant) {
        $loginArgs += "--tenant-id=$Tenant"
    }

    Write-Host 'Online transfer auth preflight: completing AzCopy device login in the foreground so the background transfer can reuse it.'
    & $azCopyPath @loginArgs | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'AzCopy device login failed during online transfer auth preflight.'
    }
}

function Get-FirstObjectPropertyValue {
    param(
        [object]$InputObject,
        [string[]]$PropertyNames
    )

    if (-not $InputObject) {
        return $null
    }

    foreach ($propertyName in $PropertyNames) {
        if ($InputObject.PSObject.Properties.Match($propertyName).Count -gt 0) {
            $value = $InputObject.$propertyName
            if ($null -ne $value -and [string]::IsNullOrWhiteSpace([string]$value) -eq $false) {
                return $value
            }
        }
    }

    return $null
}

function Get-AzCliLogReplayStatus {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$DatabaseName
    )

    $azCommand = Get-Command az -ErrorAction SilentlyContinue
    if (-not $azCommand) {
        return $null
    }

    $output = & $azCommand.Source sql midb log-replay show -g $ResourceGroup --mi $ManagedInstance -n $DatabaseName --output json 2>&1
    $outputText = $output | Out-String
    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    try {
        $payload = $outputText | ConvertFrom-Json -ErrorAction Stop
    } catch {
        return $null
    }

    return [pscustomobject]@{
        Status = [string](Get-FirstObjectPropertyValue -InputObject $payload -PropertyNames @('status', 'state', 'provisioningState'))
        Progress = [string](Get-FirstObjectPropertyValue -InputObject $payload -PropertyNames @('percentComplete', 'progress', 'restoreProgress', 'pendingFilesCount', 'restoredFilesCount'))
        LastRestored = [string](Get-FirstObjectPropertyValue -InputObject $payload -PropertyNames @('lastRestoredBackupFileName', 'lastRestoredFileName', 'lastBackupApplied', 'lastBackupName'))
    }
}

function Invoke-AzPowerShellLogReplayStart {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$DatabaseName,
        [string]$StorageUri,
        [string]$StorageIdentity,
        [string]$StorageSasToken
    )

    $startParameters = @{
        ResourceGroupName = $ResourceGroup
        InstanceName = $ManagedInstance
        Name = $DatabaseName
        StorageContainerUri = $StorageUri
        StorageContainerIdentity = $StorageIdentity
        ErrorAction = 'Stop'
    }

    if ($StorageSasToken) {
        $startParameters['StorageContainerSasToken'] = $StorageSasToken
    }

    $result = Start-AzSqlInstanceDatabaseLogReplay @startParameters
    return ($result | Out-String).Trim()
}

function Invoke-AzCliLogReplayComplete {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$DatabaseName,
        [string]$LastBackupName
    )

    $azCommand = Get-Command az -ErrorAction SilentlyContinue
    if (-not $azCommand) {
        throw 'Azure CLI (az) is required for online LRS cutover operations.'
    }

    $arguments = @(
        'sql', 'midb', 'log-replay', 'complete',
        '-g', $ResourceGroup,
        '--mi', $ManagedInstance,
        '-n', $DatabaseName,
        '--last-backup-name', $LastBackupName,
        '--output', 'json'
    )

    $output = & $azCommand.Source @arguments 2>&1
    $outputText = $output | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw $outputText.Trim()
    }

    return $outputText
}

function Format-RemainingDurationText {
    param([timespan]$Duration)

    if ($Duration.TotalSeconds -le 0) {
        return 'due now'
    }

    if ($Duration.TotalDays -ge 1) {
        return ('{0}d {1}h' -f [int]$Duration.TotalDays, $Duration.Hours)
    }

    if ($Duration.TotalHours -ge 1) {
        return ('{0}h {1}m' -f [int]$Duration.TotalHours, $Duration.Minutes)
    }

    if ($Duration.TotalMinutes -ge 1) {
        return ('{0}m {1}s' -f [int]$Duration.TotalMinutes, $Duration.Seconds)
    }

    return ('{0}s' -f [int][Math]::Max(0, [Math]::Round($Duration.TotalSeconds)))
}

function Write-PhaseStatusBanner {
    param(
        [string]$Phase,
        [string]$Message,
        [Nullable[datetime]]$NextUpdateLocalTime,
        [string]$FallbackUpdateText
    )

    Write-Host ''
    Write-Host ("=== Current Phase: {0} ===" -f $Phase) -ForegroundColor Cyan
    Write-Host $Message

    if ($NextUpdateLocalTime) {
        Write-Host ("Next expected console update: {0}" -f ([datetime]$NextUpdateLocalTime).ToString('yyyy-MM-dd HH:mm:ss'))
        return
    }

    if ($FallbackUpdateText) {
        Write-Host ("Next expected console update: {0}" -f $FallbackUpdateText)
    }
}

function Read-GroupCutoverScheduleUtc {
    while ($true) {
        Write-Host ''
        Write-Host 'Select a group cutover schedule for all databases on this instance:'
        Write-Host '[1] In 1 hour'
        Write-Host '[2] In 2 hours'
        Write-Host '[3] In 1 day'
        Write-Host '[4] In 2 days'
        Write-Host '[5] Specify an exact local date/time'
        Write-Host '[6] Do not schedule yet; decide later from the monitor'

        $choice = Read-Host -Prompt 'Cutover schedule option'
        $now = Get-Date
        switch ($choice) {
            '1' { return $now.AddHours(1).ToUniversalTime() }
            '2' { return $now.AddHours(2).ToUniversalTime() }
            '3' { return $now.AddDays(1).ToUniversalTime() }
            '4' { return $now.AddDays(2).ToUniversalTime() }
            '5' {
                $exactText = Read-Host -Prompt 'Enter local cutover date/time (example: 2026-04-10 21:30)'
                $exactValue = $null
                if (-not [datetime]::TryParse($exactText, [ref]$exactValue)) {
                    Write-Warning 'Unable to parse the supplied date/time.'
                    continue
                }

                if ($exactValue -le $now) {
                    Write-Warning 'Cutover time must be in the future.'
                    continue
                }

                return $exactValue.ToUniversalTime()
            }
            '6' { return $null }
            default { Write-Warning 'Invalid cutover schedule selection.' }
        }
    }
}

function Resolve-RequestedCutoverScheduleUtc {
    param([Nullable[datetime]]$RequestedCutoverLocalTime)

    if ($null -eq $RequestedCutoverLocalTime) {
        return $null
    }

    $resolvedLocalTime = [datetime]$RequestedCutoverLocalTime
    if ($resolvedLocalTime -le (Get-Date)) {
        throw "ScheduledCutoverLocalTime '$($resolvedLocalTime.ToString('yyyy-MM-dd HH:mm:ss'))' is in the past. Cutover would execute immediately on the next monitor poll using only the backups already transferred, causing silent data loss for any source activity after the last uploaded log. Provide a future local date/time, or omit -ScheduledCutoverLocalTime to schedule interactively."
    }

    return $resolvedLocalTime.ToUniversalTime()
}

function Test-CutoverScheduleReady {
    param(
        [object[]]$Items,
        [string]$StatePath,
        [datetime]$TargetCutoverUtc
    )

    $state = Load-TransferState -Path $StatePath
    foreach ($item in $Items) {
        $lastSyncUtc = Get-TransferLastSyncUtc -State $state -StateKey $item.StateKey
        if (-not $lastSyncUtc -or $lastSyncUtc -lt $TargetCutoverUtc) {
            return $false
        }
    }

    return $true
}

function Stop-TransferProcessIfRunning {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Action,
        [string]$Message,
        [string]$Level = 'Warning'
    )

    if (-not $Process) {
        return $false
    }

    try {
        if ($Process.HasExited) {
            return $false
        }

        Stop-Process -Id $Process.Id -Force
        Write-Host $Message
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level $Level -Phase 'Transfer' -Action $Action -Message $Message -Data @{ processId = $Process.Id }
        return $true
    } catch {
        Write-Warning "Failed to stop transfer process $($Process.Id). $($_.Exception.Message)"
        return $false
    }
}

function Remove-RestoringManagedDatabaseIfPresent {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$InstanceName,
        [string]$DatabaseName
    )

    $managedDatabase = $null
    try {
        $managedDatabase = Get-AzSqlInstanceDatabase -ResourceGroupName $ResourceGroup -InstanceName $ManagedInstance -Name $DatabaseName -ErrorAction Stop
    } catch {
        return $false
    }

    if (-not $managedDatabase) {
        return $false
    }

    $databaseStatus = [string]$managedDatabase.Status
    if ($databaseStatus -ne 'Restoring') {
        return $false
    }

    Remove-AzSqlInstanceDatabase -ResourceGroupName $ResourceGroup -InstanceName $ManagedInstance -Name $DatabaseName -Force -ErrorAction Stop | Out-Null
    Write-Host "Removed partially restored managed database '$DatabaseName'."
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Warning' -Phase 'LRS' -Action 'PartialDatabaseRemoved' -InstanceName $InstanceName -DatabaseName $DatabaseName -Message 'Removed partially restored managed database during online failure cleanup.' -Data @{ status = $databaseStatus }
    return $true
}

function Invoke-OnlineStartupFailureCleanup {
    param(
        [System.Diagnostics.Process]$TransferProcess,
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [object[]]$Items
    )

    [void](Stop-TransferProcessIfRunning -Process $TransferProcess -Action 'BackgroundProcessStoppedOnStartFailure' -Message 'Stopped background transfer process because online LRS startup failed for the selected database group.')

    foreach ($item in $Items) {
        try {
            [void](Remove-RestoringManagedDatabaseIfPresent -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -InstanceName $item.InstanceName -DatabaseName $item.DatabaseName)
        } catch {
            Write-Warning "Failed to remove partially restored managed database '$($item.DatabaseName)'. $($_.Exception.Message)"
            Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Warning' -Phase 'LRS' -Action 'PartialDatabaseRemovalFailed' -InstanceName $item.InstanceName -DatabaseName $item.DatabaseName -Message $_.Exception.Message
        }
    }
}

function Test-CriticalOnlineCutoverFailure {
    param([string]$Message)

    if (-not $Message) {
        return $false
    }

    $criticalPatterns = @(
        'Cannot convert the ".*" value of type ".*" to type ".*"',
        'The property ''.+'' cannot be found on this object',
        'Cannot index into a null array',
        'Index was outside the bounds of the array',
        'Object reference not set to an instance of an object',
        'Method invocation failed because',
        'You cannot call a method on a null-valued expression',
        # ARM-side failures during cutover/complete that are unsafe to retry automatically.
        # InternalServerError on completeRestore can leave the restoring DB dropped; a follow-up
        # retry then hits ResourceNotFound and masks the real problem. Stop and surface to operator.
        '\(InternalServerError\)',
        'Code:\s*InternalServerError',
        '\(ResourceNotFound\)',
        'Code:\s*ResourceNotFound',
        'ARMResourceNotFoundFix',
        '\(ParentResourceNotFound\)',
        'Code:\s*ParentResourceNotFound'
    )

    foreach ($pattern in $criticalPatterns) {
        if ($Message -match $pattern) {
            return $true
        }
    }

    return $false
}

function Stop-OnlineProcessesForCriticalFailure {
    param(
        [System.Diagnostics.Process]$TransferProcess,
        [string]$FailurePhase,
        [string]$FailureMessage
    )

    [void](Stop-TransferProcessIfRunning -Process $TransferProcess -Action 'BackgroundProcessStoppedOnCriticalFailure' -Message 'Stopped background transfer process because a critical online wrapper failure occurred.')
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase $FailurePhase -Action 'CriticalFailureDetected' -Message $FailureMessage
}

function Test-AzureClaimsChallenge {
    param([string]$Text)

    if (-not $Text) {
        return $false
    }

    return $Text -match '(?i)claims challenge|Status_InteractionRequired|Response_Status\.Status_InteractionRequired|interaction required|acrs'
}

function Invoke-AzCliDeviceCodeLogin {
    param(
        [string]$Tenant,
        [string]$Subscription
    )

    $azCommand = Get-Command az -ErrorAction SilentlyContinue
    if (-not $azCommand) {
        throw 'Azure CLI (az) is required for automatic reauthentication.'
    }

    Write-Host 'Azure CLI session requires interactive reauthentication. Starting device code sign-in...'
    & $azCommand.Source logout 2>$null | Out-Null

    $loginArgs = @('login', '--use-device-code')
    if ($Tenant) {
        $loginArgs += @('--tenant', $Tenant)
    }

    $loginOutput = & $azCommand.Source @loginArgs 2>&1
    $loginText = $loginOutput | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI device code login failed. $loginText"
    }

    if ($Subscription) {
        $setOutput = & $azCommand.Source account set --subscription $Subscription 2>&1
        $setText = $setOutput | Out-String
        if ($LASTEXITCODE -ne 0) {
            throw "Azure CLI login succeeded, but failed to select subscription '$Subscription'. $setText"
        }
    }
}

function Import-LatestAzAccountsModule {
    $minimumVersion = [version]'5.3.3'
    $attemptedInstall = $false

    while ($true) {
        $module = Get-Module -ListAvailable -Name 'Az.Accounts' | Sort-Object Version -Descending | Select-Object -First 1

        if ($module -and $module.Version -ge $minimumVersion) {
            $loadedModule = Get-Module -Name 'Az.Accounts' | Sort-Object Version -Descending | Select-Object -First 1
            if (-not $loadedModule -or $loadedModule.Version -ne $module.Version) {
                Import-Module -Name 'Az.Accounts' -RequiredVersion $module.Version -Force -ErrorAction Stop
            }

            return
        }

        if ($attemptedInstall) {
            throw 'Az.Accounts module is required for Azure PowerShell authentication. Automatic installation did not succeed.'
        }

        Write-Host 'Az.Accounts module was not found or is below the required version. Attempting installation for the current user...'
        Install-Module -Name 'Az.Accounts' -MinimumVersion $minimumVersion.ToString() -Scope CurrentUser -Repository PSGallery -Force -AllowClobber -ErrorAction Stop
        $attemptedInstall = $true
    }
}

function Connect-AzPowerShellInteractive {
    param(
        [string]$Tenant,
        [string]$Subscription,
        [bool]$UseDeviceAuthentication
    )

    Import-LatestAzAccountsModule

    $connectArgs = @{}
    if ($Tenant) {
        $connectArgs['Tenant'] = $Tenant
    }

    if ($Subscription) {
        $connectArgs['Subscription'] = $Subscription
    }

    if ($UseDeviceAuthentication) {
        $connectArgs['UseDeviceAuthentication'] = $true
    }

    Connect-AzAccount @connectArgs | Out-Null
}

function ConvertTo-PlainTextAccessToken {
    param([object]$TokenValue)

    if ($null -eq $TokenValue) {
        return $null
    }

    if ($TokenValue -is [string]) {
        return $TokenValue
    }

    if ($TokenValue -is [securestring]) {
        $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($TokenValue)
        try {
            return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
        } finally {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
        }
    }

    return [string]$TokenValue
}

function ConvertFrom-Base64UrlString {
    param([string]$Value)

    if (-not $Value) {
        return $null
    }

    $normalized = $Value.Replace('-', '+').Replace('_', '/')
    switch ($normalized.Length % 4) {
        2 { $normalized += '==' }
        3 { $normalized += '=' }
    }

    return [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($normalized))
}

function Get-AzAccessTokenPayload {
    Import-LatestAzAccountsModule

    if (-not (Get-Command Get-AzAccessToken -ErrorAction SilentlyContinue)) {
        return $null
    }

    $tokenResponse = Get-AzAccessToken -ResourceUrl 'https://management.azure.com/' -ErrorAction Stop
    $accessToken = ConvertTo-PlainTextAccessToken -TokenValue $tokenResponse.Token
    if (-not $accessToken) {
        return $null
    }

    $tokenParts = $accessToken -split '\.'
    if ($tokenParts.Count -lt 2) {
        return $null
    }

    $payloadJson = ConvertFrom-Base64UrlString -Value $tokenParts[1]
    if (-not $payloadJson) {
        return $null
    }

    return $payloadJson | ConvertFrom-Json -ErrorAction Stop
}

function Test-AzAccessTokenHasMfaClaim {
    param([object]$TokenPayload)

    if (-not $TokenPayload) {
        return $false
    }

    $amrValues = @($TokenPayload.amr | ForEach-Object { [string]$_ })
    if ($amrValues -contains 'mfa') {
        return $true
    }

    $acrsValues = @($TokenPayload.acrs | ForEach-Object { [string]$_ })
    if ($acrsValues.Count -gt 0) {
        return $true
    }

    $acrValue = [string]$TokenPayload.acr
    if ($acrValue -and $acrValue -notin @('0', '1')) {
        return $true
    }

    return $false
}

function Assert-AzureControlPlaneMfaReady {
    param(
        [string]$Tenant,
        [string]$Subscription,
        [bool]$AllowAutoReauthenticate = $true,
        [string]$OperationLabel = 'Azure control-plane'
    )

    $tokenPayload = $null
    try {
        $tokenPayload = Get-AzAccessTokenPayload
    } catch {
        Write-Warning "$OperationLabel preflight could not inspect the current ARM token. $($_.Exception.Message)"
        return
    }

    if (Test-AzAccessTokenHasMfaClaim -TokenPayload $tokenPayload) {
        Write-Host "$OperationLabel preflight: current Az PowerShell token includes MFA claims."
        return
    }

    if (-not $AllowAutoReauthenticate) {
        throw "$OperationLabel preflight detected an Az PowerShell token without MFA claims. Run Connect-AzAccount -UseDeviceAuthentication in this terminal, then rerun the wrapper."
    }

    Write-Host "$OperationLabel preflight: current Az PowerShell token does not show MFA claims. Starting device authentication before transfer..."
    Connect-AzPowerShellInteractive -Tenant $Tenant -Subscription $Subscription -UseDeviceAuthentication $true

    $refreshedPayload = $null
    try {
        $refreshedPayload = Get-AzAccessTokenPayload
    } catch {
        throw "$OperationLabel preflight could not inspect the refreshed ARM token after device authentication. $($_.Exception.Message)"
    }

    if (Test-AzAccessTokenHasMfaClaim -TokenPayload $refreshedPayload) {
        Write-Host "$OperationLabel preflight: refreshed Az PowerShell token includes MFA claims."
        return
    }

    throw "$OperationLabel preflight completed device authentication, but the current ARM token still does not include MFA claims. Run Connect-AzAccount -UseDeviceAuthentication in this terminal, complete MFA, then rerun the wrapper."
}

function Set-AzureExecutionContext {
    param(
        [string]$Tenant,
        [string]$Subscription,
        [bool]$AllowAutoReauthenticate = $true
    )

    $effectiveTenant = $Tenant
    $effectiveSubscription = $Subscription

    if (Get-Command az -ErrorAction SilentlyContinue) {
        $azCommand = (Get-Command az -ErrorAction SilentlyContinue).Source
        $accountJson = & $azCommand account show --output json 2>&1
        $accountText = $accountJson | Out-String
        if ($LASTEXITCODE -ne 0) {
            if ($AllowAutoReauthenticate -and (Test-AzureClaimsChallenge -Text $accountText)) {
                Invoke-AzCliDeviceCodeLogin -Tenant $effectiveTenant -Subscription $effectiveSubscription
                $accountJson = & $azCommand account show --output json 2>&1
                $accountText = $accountJson | Out-String
            }

            if ($LASTEXITCODE -ne 0) {
                throw "Unable to resolve Azure CLI account context. $accountText"
            }
        }

        if ($accountJson) {
            $account = $accountJson | ConvertFrom-Json
            if (-not $effectiveSubscription -and $account.id) {
                $effectiveSubscription = [string]$account.id
            }

            if (-not $effectiveTenant -and $account.tenantId) {
                $effectiveTenant = [string]$account.tenantId
            }
        }

        if ($effectiveSubscription) {
            $setOutput = & $azCommand account set --subscription $effectiveSubscription 2>&1
            $setText = $setOutput | Out-String
            if ($LASTEXITCODE -ne 0) {
                if ($AllowAutoReauthenticate -and (Test-AzureClaimsChallenge -Text $setText)) {
                    Invoke-AzCliDeviceCodeLogin -Tenant $effectiveTenant -Subscription $effectiveSubscription
                    $setOutput = & $azCommand account set --subscription $effectiveSubscription 2>&1
                    $setText = $setOutput | Out-String
                }

                if ($LASTEXITCODE -ne 0) {
                    throw "Failed to set Azure CLI subscription '$effectiveSubscription'. $setText"
                }
            }
        }
    }

    Import-LatestAzAccountsModule

    if (Get-Command Get-AzContext -ErrorAction SilentlyContinue) {
        $context = Get-AzContext -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        if (-not $context) {
            try {
                Connect-AzPowerShellInteractive -Tenant $effectiveTenant -Subscription $effectiveSubscription -UseDeviceAuthentication $false
            } catch {
                if (-not $AllowAutoReauthenticate) {
                    throw
                }

                Write-Host 'Az PowerShell context requires interactive authentication. Starting device authentication...'
                Connect-AzPowerShellInteractive -Tenant $effectiveTenant -Subscription $effectiveSubscription -UseDeviceAuthentication $true
            }
        } elseif ($effectiveSubscription) {
            $setContextArgs = @{ Subscription = $effectiveSubscription }
            if ($effectiveTenant) {
                $setContextArgs['Tenant'] = $effectiveTenant
            }

            try {
                Set-AzContext @setContextArgs | Out-Null
            } catch {
                if (-not $AllowAutoReauthenticate) {
                    throw
                }

                Write-Host 'Az PowerShell context could not be refreshed silently. Starting device authentication...'
                Connect-AzPowerShellInteractive -Tenant $effectiveTenant -Subscription $effectiveSubscription -UseDeviceAuthentication $true
            }
        }
    }

    return [pscustomobject]@{
        TenantId = $effectiveTenant
        SubscriptionId = $effectiveSubscription
    }
}

function New-MigrationItem {
    param(
        [System.IO.DirectoryInfo]$InstanceFolder,
        [string]$DatabaseName,
        [string]$AccountName
    )

    $databaseRootPath = Join-Path -Path $InstanceFolder.FullName -ChildPath $DatabaseName
    $containerName = Get-ContainerNameFromInstanceFolder -Value $InstanceFolder.Name
    $storageUri = "https://$AccountName.blob.core.windows.net/$containerName/$DatabaseName/"

    return [pscustomobject]@{
        InstanceName      = $InstanceFolder.Name
        InstancePath      = $InstanceFolder.FullName
        DatabaseName      = $DatabaseName
        DatabaseRootPath  = $databaseRootPath
        ContainerName     = $containerName
        StorageUri        = $storageUri
        StateKey          = "$($InstanceFolder.Name)::$DatabaseName"
    }
}

function Get-MigrationItems {
    param(
        [System.IO.DirectoryInfo[]]$InstanceFolderList,
        [string]$AccountName,
        [string[]]$RequestedDatabaseNames
    )

    $items = @()
    foreach ($instanceFolder in $InstanceFolderList) {
        $instanceDatabaseNames = Select-DatabaseNames -DatabaseNameList (Get-DatabaseNames -InstancePath $instanceFolder.FullName) -RequestedNames $RequestedDatabaseNames
        foreach ($databaseName in $instanceDatabaseNames) {
            $items += New-MigrationItem -InstanceFolder $instanceFolder -DatabaseName $databaseName -AccountName $AccountName
        }
    }

    return ,@($items)
}

function Remove-IfExists {
    param([string]$Path)

    if ($Path -and (Test-Path -LiteralPath $Path)) {
        Remove-Item -LiteralPath $Path -Force
    }
}

function Get-SafePathComponent {
    param([string]$Value)

    $safeValue = [string]$Value
    $safeValue = $safeValue -replace '[^A-Za-z0-9._-]', '-'
    $safeValue = $safeValue.Trim('-')
    if (-not $safeValue) {
        return 'item'
    }

    return $safeValue
}

function ConvertTo-SerializedArgument {
    param([string[]]$Values)

    return (($Values | ForEach-Object { [string]$_ } | Where-Object { $_ }) -join '|')
}

function Start-TransferProcess {
    param(
        [string]$ScriptPath,
        [string]$SourceRootPath,
        [string[]]$InstanceFilter,
        [string]$AccountName,
        [string]$Tenant,
        [string]$AuthMode,
        [string]$StorageSasToken,
        [bool]$AssumeDeviceLoginReady,
        [int]$PollSeconds,
        [string]$StatePath,
        [string]$StdOutPath,
        [string]$StdErrPath,
        [string[]]$DbNames,
        [string]$EventLogPath,
        [string]$ReportContextPath,
        [string]$RunId,
        [switch]$IncludeDiffs
    )

    Remove-IfExists -Path $StatePath
    Remove-IfExists -Path $StdOutPath
    Remove-IfExists -Path $StdErrPath

    $argumentList = @(
        '-NoProfile',
        '-File', $ScriptPath,
        '-Mode', 'Online',
        '-SourcePathBase', $SourceRootPath,
        '-StorageAccountName', $AccountName,
        '-StorageAuthMode', $AuthMode,
        '-IntervalSeconds', $PollSeconds.ToString(),
        '-StatePath', $StatePath,
        '-EventLogPath', $EventLogPath,
        '-ReportContextPath', $ReportContextPath,
        '-RunId', $RunId
    )

    if ($IncludeDiffs) {
        $argumentList += '-IncludeDiffs'
    }

    if ($InstanceFilter -and $InstanceFilter.Count -gt 0) {
        $argumentList += '-InstanceNamesList'
        $argumentList += ConvertTo-SerializedArgument -Values $InstanceFilter
    }

    if ($Tenant) {
        $argumentList += '-TenantId'
        $argumentList += $Tenant
    }

    if ($StorageSasToken) {
        $argumentList += '-StorageContainerSasToken'
        $argumentList += $StorageSasToken
    }

    if ($AssumeDeviceLoginReady) {
        $argumentList += '-AssumeDeviceLoginReady'
    }

    if ($DbNames -and $DbNames.Count -gt 0) {
        $argumentList += '-DatabaseNamesList'
        $argumentList += ConvertTo-SerializedArgument -Values $DbNames
    }

    $shellPath = Get-TransferShellPath
    $startProcessArgs = @{
        FilePath = $shellPath
        ArgumentList = $argumentList
        PassThru = $true
        RedirectStandardOutput = $StdOutPath
        RedirectStandardError = $StdErrPath
    }

    if ($IsWindows) {
        $startProcessArgs['WindowStyle'] = 'Hidden'
    }

    return Start-Process @startProcessArgs
}

function Start-OnlineLrsWorkerProcess {
    param(
        [string]$ScriptPath,
        [string]$Tenant,
        [string]$Subscription,
        [pscustomobject]$Item,
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$StorageIdentity,
        [string]$StorageSasToken,
        [string]$ResultPath,
        [string]$StdOutPath,
        [string]$StdErrPath
    )

    Remove-IfExists -Path $ResultPath
    Remove-IfExists -Path $StdOutPath
    Remove-IfExists -Path $StdErrPath

    $argumentList = @(
        '-NoProfile',
        '-File', $ScriptPath,
        '-ResourceGroupName', $ResourceGroup,
        '-ManagedInstanceName', $ManagedInstance,
        '-DatabaseName', $Item.DatabaseName,
        '-StorageContainerUri', $Item.StorageUri,
        '-StorageContainerIdentity', $StorageIdentity,
        '-ResultPath', $ResultPath
    )

    if ($Tenant) {
        $argumentList += '-TenantId'
        $argumentList += $Tenant
    }

    if ($Subscription) {
        $argumentList += '-SubscriptionId'
        $argumentList += $Subscription
    }

    if ($StorageSasToken) {
        $argumentList += '-StorageContainerSasToken'
        $argumentList += $StorageSasToken
    }

    $startProcessArgs = @{
        FilePath = (Get-TransferShellPath)
        ArgumentList = $argumentList
        PassThru = $true
        RedirectStandardOutput = $StdOutPath
        RedirectStandardError = $StdErrPath
    }

    if ($IsWindows) {
        $startProcessArgs['WindowStyle'] = 'Hidden'
    }

    $process = Start-Process @startProcessArgs
    return [pscustomobject]@{
        Item = $Item
        Process = $process
        ResultPath = $ResultPath
        StdOutPath = $StdOutPath
        StdErrPath = $StdErrPath
    }
}

function Wait-ForOnlineLrsWorkerProcesses {
    param(
        [object[]]$WorkerList,
        [int]$TimeoutMinutes = 60,
        [int]$StatusIntervalMinutes = 1,
        [Nullable[datetime]]$ScheduledCutoverUtc,
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [object[]]$Items,
        [string]$StatePath,
        [System.Diagnostics.Process]$TransferProcess
    )

    if (-not $WorkerList -or $WorkerList.Count -eq 0) {
        return @()
    }

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $heartbeatIntervalSeconds = [Math]::Max(5, $StatusIntervalMinutes * 60)
    $nextHeartbeatAt = Get-Date
    $results = @()
    $completedWorkerIds = @{}
    while ((Get-Date) -lt $deadline) {
        $activeWorkers = @()
        foreach ($worker in $WorkerList) {
            $worker.Process.Refresh()
            if (-not $worker.Process.HasExited) {
                if ($ResourceGroup -and $ManagedInstance) {
                    $existingState = Get-ExistingOnlineRestoreState -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -DatabaseName $worker.Item.DatabaseName
                    if ($existingState.Found) {
                        try {
                            $worker.Process.Kill($true)
                        } catch {
                        }

                        $completedWorkerIds[$worker.Process.Id] = $true
                        $results += [pscustomobject]@{
                            Item = $worker.Item
                            Success = $true
                            Message = "Existing online restore detected with status $($existingState.Status) while the startup worker was still running."
                        }
                        continue
                    }
                }

                $activeWorkers += $worker
            }
        }

        if ($activeWorkers.Count -eq 0) {
            break
        }

        if ((Get-Date) -ge $nextHeartbeatAt) {
            $activeDatabases = @($activeWorkers | ForEach-Object { $_.Item.DatabaseName }) -join ', '
            if ($ScheduledCutoverUtc) {
                $cutoverText = Format-RemainingDurationText -Duration (([datetime]$ScheduledCutoverUtc).ToLocalTime() - (Get-Date))
                Write-Host ("Waiting for online LRS startup workers: {0} active for {1}. Waiting for restore startup to accept the next transaction log. Scheduled cutover {2}." -f $activeWorkers.Count, $activeDatabases, $cutoverText)
            } else {
                Write-Host ("Waiting for online LRS startup workers: {0} active for {1}. Waiting for restore startup to accept the next transaction log." -f $activeWorkers.Count, $activeDatabases)
            }

            if ($ResourceGroup -and $ManagedInstance -and $Items -and $StatePath -and $TransferProcess) {
                Show-MigrationSnapshot -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -Items $Items -StatePath $StatePath -TransferProcess $TransferProcess -ScheduledCutoverUtc $ScheduledCutoverUtc
            }

            $nextHeartbeatAt = (Get-Date).AddSeconds($heartbeatIntervalSeconds)
        }

        Start-Sleep -Seconds 2
    }

    foreach ($worker in $WorkerList) {
        if ($completedWorkerIds.ContainsKey($worker.Process.Id)) {
            continue
        }

        $worker.Process.Refresh()

        if (-not $worker.Process.HasExited) {
            try {
                $worker.Process.Kill($true)
            } catch {
            }

            $results += [pscustomobject]@{
                Item = $worker.Item
                Success = $false
                Message = "Timed out waiting for LRS start worker to finish for $($worker.Item.DatabaseName)."
            }
            continue
        }

        $workerPayload = $null
        if (Test-Path -LiteralPath $worker.ResultPath) {
            try {
                $workerPayload = Get-Content -LiteralPath $worker.ResultPath -Raw | ConvertFrom-Json -ErrorAction Stop
            } catch {
            }
        }

        if ($workerPayload -and $worker.Process.ExitCode -eq 0 -and $workerPayload.success) {
            $results += [pscustomobject]@{
                Item = $worker.Item
                Success = $true
                Message = [string]$workerPayload.message
            }
            continue
        }

        $failureMessage = if ($workerPayload -and $workerPayload.message) {
            [string]$workerPayload.message
        } elseif (Test-Path -LiteralPath $worker.StdErrPath) {
            ((Get-Content -LiteralPath $worker.StdErrPath -Tail 20) -join [Environment]::NewLine)
        } else {
            "LRS start worker failed for $($worker.Item.DatabaseName) with exit code $($worker.Process.ExitCode)."
        }

        $results += [pscustomobject]@{
            Item = $worker.Item
            Success = $false
            Message = $failureMessage
        }
    }

    return @($results)
}

function Test-OnlineLrsWorkerResultObject {
    param([object]$InputObject)

    if ($null -eq $InputObject) {
        return $false
    }

    $requiredProperties = @('Item', 'Success', 'Message')
    foreach ($propertyName in $requiredProperties) {
        if ($InputObject.PSObject.Properties.Match($propertyName).Count -eq 0) {
            return $false
        }
    }

    return $true
}

function Wait-ForInitialUploadCompletion {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$StatePath,
        [string[]]$ExpectedStateKeys,
        [int]$TimeoutMinutes,
        [string]$StdErrPath
    )

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $lastReportedReadyCount = -1
    $lastWaitReminderAt = $null
    while ((Get-Date) -lt $deadline) {
        $state = Load-TransferState -Path $StatePath
        $availableKeys = @()
        foreach ($stateKey in $ExpectedStateKeys) {
            if (Get-TransferLastSyncUtc -State $state -StateKey $stateKey) {
                $availableKeys += $stateKey
            }
        }

        if ($availableKeys.Count -eq $ExpectedStateKeys.Count) {
            Write-Host "Initial upload completed for $($ExpectedStateKeys.Count) database(s)."
            return
        }

        if ($Process.HasExited) {
            $errorTail = ''
            if (Test-Path -LiteralPath $StdErrPath) {
                $errorTail = (Get-Content -LiteralPath $StdErrPath -Tail 20) -join [Environment]::NewLine
            }

            throw "Background transfer process exited before initial upload completed. $errorTail"
        }

        $readyCount = $availableKeys.Count
        if (($readyCount -ne $lastReportedReadyCount) -or ($null -eq $lastWaitReminderAt) -or ((Get-Date) -ge $lastWaitReminderAt.AddSeconds(30))) {
            Write-Host "Waiting for initial upload completion: $readyCount/$($ExpectedStateKeys.Count) database(s) ready."
            $lastReportedReadyCount = $readyCount
            $lastWaitReminderAt = Get-Date
        }

        Start-Sleep -Seconds 5
    }

    throw "Timed out after $TimeoutMinutes minute(s) waiting for the initial upload to complete."
}

function Get-LrsStatusText {
    param([object]$LrsObject)

    if (-not $LrsObject) {
        return 'Unknown'
    }

    if ($LrsObject.PSObject.Properties.Match('Status').Count -gt 0) {
        return [string]$LrsObject.Status
    }

    return 'Unknown'
}

function Get-ExistingOnlineRestoreState {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [string]$DatabaseName
    )

    $existingLrs = $null
    try {
        $existingLrs = Get-AzSqlInstanceDatabaseLogReplay -ResourceGroupName $ResourceGroup -InstanceName $ManagedInstance -Name $DatabaseName -ErrorAction Stop
    } catch {
    }

    if ($existingLrs) {
        return [pscustomobject]@{
            Found = $true
            Kind = 'Lrs'
            Status = Get-LrsStatusText -LrsObject $existingLrs
        }
    }

    $managedDatabase = $null
    try {
        $managedDatabase = Get-AzSqlInstanceDatabase -ResourceGroupName $ResourceGroup -InstanceName $ManagedInstance -Name $DatabaseName -ErrorAction Stop
    } catch {
    }

    if ($managedDatabase -and ([string]$managedDatabase.Status) -eq 'Restoring') {
        return [pscustomobject]@{
            Found = $true
            Kind = 'ManagedDatabase'
            Status = [string]$managedDatabase.Status
        }
    }

    return [pscustomobject]@{
        Found = $false
        Kind = $null
        Status = $null
    }
}

function Start-OnlineLrsForItem {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [pscustomobject]$Item,
        [string]$StorageIdentity,
        [string]$StorageSasToken,
        [string]$WorkerScriptPath,
        [string]$TenantId,
        [string]$SubscriptionId,
        [string]$WorkerResultDir,
        [string]$WorkerLogsDir
    )

    $existingState = Get-ExistingOnlineRestoreState -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -DatabaseName $Item.DatabaseName
    if ($existingState.Found) {
        Write-Host "Online restore already exists for $($Item.DatabaseName) with status $($existingState.Status)."
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'LRS' -Action 'ReuseExisting' -InstanceName $Item.InstanceName -DatabaseName $Item.DatabaseName -Message 'Existing online restore session detected.' -Data @{ status = $existingState.Status; kind = $existingState.Kind }
        return [pscustomobject]@{
            Mode = 'Reused'
            Item = $Item
        }
    }

    Write-Host "Starting online LRS for $($Item.DatabaseName) from $($Item.StorageUri)"
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'LRS' -Action 'StartRequested' -InstanceName $Item.InstanceName -DatabaseName $Item.DatabaseName -Message 'Starting online LRS for database.' -Data @{ storageUri = $Item.StorageUri }
    if (-not $script:monitorCommandsHintShown) {
        Write-MonitorCommandsHint
        $script:monitorCommandsHintShown = $true
    }

    $safeDatabaseName = Get-SafePathComponent -Value $Item.DatabaseName
    $resultPath = Join-Path -Path $WorkerResultDir -ChildPath ("$safeDatabaseName.result.json")
    $stdoutPath = Join-Path -Path $WorkerLogsDir -ChildPath ("$safeDatabaseName.stdout.log")
    $stderrPath = Join-Path -Path $WorkerLogsDir -ChildPath ("$safeDatabaseName.stderr.log")
    $worker = Start-OnlineLrsWorkerProcess -ScriptPath $WorkerScriptPath -Tenant $TenantId -Subscription $SubscriptionId -Item $Item -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -StorageIdentity $StorageIdentity -StorageSasToken $StorageSasToken -ResultPath $resultPath -StdOutPath $stdoutPath -StdErrPath $stderrPath
    return [pscustomobject]@{
        Mode = 'Worker'
        Item = $Item
        Worker = $worker
    }
}

function Get-UploadedApplicableLogsForItem {
    param(
        [pscustomobject]$Item,
        [object]$State
    )

    $latestFull = Get-LatestFullBackupFile -DatabaseRootPath $Item.DatabaseRootPath
    $selectedDiff = Get-ApplicableDiffBackupFile -DatabaseRootPath $Item.DatabaseRootPath -FullBackupTime $latestFull.LastWriteTime
    $anchorTime = if ($selectedDiff) { $selectedDiff.LastWriteTime } else { $latestFull.LastWriteTime }
    $lastSyncUtc = Get-TransferLastSyncUtc -State $State -StateKey $Item.StateKey

    $applicableLogs = Get-ApplicableLogFiles -DatabaseRootPath $Item.DatabaseRootPath -AnchorTime $anchorTime -LastUploadedUtc $lastSyncUtc
    return @($applicableLogs | ForEach-Object { $_ })
}

function Show-MigrationSnapshot {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [object[]]$Items,
        [string]$StatePath,
        [System.Diagnostics.Process]$TransferProcess,
        [Nullable[datetime]]$ScheduledCutoverUtc
    )

    $state = Load-TransferState -Path $StatePath
    $rows = foreach ($item in $Items) {
        $lrs = $null
        $statusText = 'NotStarted'
        $lastRestored = $null
        $pendingFiles = $null
        $lastSyncUtc = Get-TransferLastSyncUtc -State $state -StateKey $item.StateKey
        $lastSyncLocalText = if ($lastSyncUtc) { ([datetime]$lastSyncUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') } else { 'NotUploadedYet' }
        $cutoverLagText = ''

        if ($ScheduledCutoverUtc) {
            if ($lastSyncUtc) {
                $lagDuration = ([datetime]$ScheduledCutoverUtc) - ([datetime]$lastSyncUtc)
                if ($lagDuration.TotalSeconds -gt 0) {
                    $cutoverLagText = ('BehindBy {0}' -f (Format-RemainingDurationText -Duration $lagDuration))
                } else {
                    $cutoverLagText = 'ReadyForCutover'
                }
            } else {
                $cutoverLagText = 'WaitingForFirstUpload'
            }
        }

        try {
            $cliStatus = Get-AzCliLogReplayStatus -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -DatabaseName $item.DatabaseName
            $lrs = Get-AzSqlInstanceDatabaseLogReplay -ResourceGroupName $ResourceGroup -InstanceName $ManagedInstance -Name $item.DatabaseName -ErrorAction Stop
            $statusText = if ($cliStatus -and $cliStatus.Status) { $cliStatus.Status } else { Get-LrsStatusText -LrsObject $lrs }

            foreach ($candidateProperty in @('LastRestoredBackupFileName', 'LastRestoredFileName', 'LastBackupApplied')) {
                if ($lrs.PSObject.Properties.Match($candidateProperty).Count -gt 0 -and $lrs.$candidateProperty) {
                    $lastRestored = $lrs.$candidateProperty
                    break
                }
            }

            foreach ($candidateProperty in @('PendingFilesCount', 'RestoredFilesCount', 'PercentComplete')) {
                if ($lrs.PSObject.Properties.Match($candidateProperty).Count -gt 0 -and $null -ne $lrs.$candidateProperty) {
                    $pendingFiles = $lrs.$candidateProperty
                    break
                }
            }

            if ($cliStatus) {
                if ($cliStatus.LastRestored) {
                    $lastRestored = $cliStatus.LastRestored
                }

                if ($cliStatus.Progress) {
                    $pendingFiles = $cliStatus.Progress
                }
            }
        } catch {
            $statusText = 'Unavailable'
        }

        $uploadedLogs = @(Get-UploadedApplicableLogsForItem -Item $item -State $state)
        $lastUploaded = if ($uploadedLogs.Count -gt 0) { $uploadedLogs[$uploadedLogs.Count - 1].Name } else { $null }

        [pscustomobject]@{
            Database          = $item.DatabaseName
            Status            = $statusText
            RestoreProgress   = $pendingFiles
            UploadedThrough   = $lastSyncLocalText
            CutoverReadiness  = $cutoverLagText
            LastUploadedLog   = $lastUploaded
            LastRestoredLog   = $lastRestored
        }
    }

    Write-Host ''
    Write-Host ("=== LRS Progress Snapshot @ {0} ===" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
    Write-Host ("Transfer process: {0}" -f ($(if ($TransferProcess.HasExited) { "Exited ($($TransferProcess.ExitCode))" } else { "Running (PID $($TransferProcess.Id))" })))
    if ($ScheduledCutoverUtc) {
        $scheduledLocal = ([datetime]$ScheduledCutoverUtc).ToLocalTime()
        $remainingText = Format-RemainingDurationText -Duration ($scheduledLocal - (Get-Date))
        Write-Host ("Scheduled group cutover: {0} ({1})" -f $scheduledLocal.ToString('yyyy-MM-dd HH:mm:ss'), $remainingText)
        $slowestUpload = $rows | Where-Object { $_.UploadedThrough -ne 'NotUploadedYet' } | Sort-Object UploadedThrough | Select-Object -First 1
        if ($slowestUpload) {
            Write-Host ("Current uploaded-through watermark: {0}" -f $slowestUpload.UploadedThrough)
        } else {
            Write-Host 'Current uploaded-through watermark: no uploaded logs recorded yet'
        }
    } else {
        Write-Host 'Scheduled group cutover: not set'
    }
    $rows | Format-Table -AutoSize | Out-Host
    Write-MonitorCommandsHint
    Write-Host ''
    return
}

function Write-MonitorCommandsHint {
    Write-Host 'Commands: S = status now, C = immediate cutover, T = schedule/reschedule cutover, Q = quit monitor' -ForegroundColor Green
}

function Read-OperatorKey {
    try {
        if ([Console]::KeyAvailable) {
            return [Console]::ReadKey($true).Key
        }
    } catch {
        return $null
    }

    return $null
}

function Show-TransferLogTail {
    param([string]$Path)

    if (Test-Path -LiteralPath $Path) {
        Write-Host 'Background transfer error log (tail):'
        Get-Content -LiteralPath $Path -Tail 20 | Out-Host
    }
}

function Invoke-OnlineCutover {
    param(
        [string]$ResourceGroup,
        [string]$ManagedInstance,
        [object[]]$Items,
        [string]$StatePath,
        [int]$CandidateCount,
        [Nullable[datetime]]$TargetCutoffUtc,
        [switch]$AutoSelectLatest
    )

    $state = Load-TransferState -Path $StatePath
    $candidateSets = @{}
    $candidateCutoffTimes = @()

    foreach ($item in $Items) {
        $uploadedLogs = @(Get-UploadedApplicableLogsForItem -Item $item -State $state)
        if ($uploadedLogs.Count -eq 0) {
            throw "No uploaded transaction log files are available for cutover on $($item.DatabaseName)."
        }

        $candidateSets[$item.StateKey] = $uploadedLogs
        foreach ($uploadedLog in $uploadedLogs) {
            $candidateCutoffTimes += [datetime]$uploadedLog.LastWriteTime.ToUniversalTime()
        }
    }

    if ($candidateCutoffTimes.Count -eq 0) {
        throw 'No common cutover candidates are available across the selected databases.'
    }

    $options = @()
    if ($TargetCutoffUtc -and -not $AutoSelectLatest) {
        $resolvedTargetCutoffUtc = [datetime]$TargetCutoffUtc
        $selection = [ordered]@{}
        foreach ($item in $Items) {
            $selectedLog = $candidateSets[$item.StateKey] | Where-Object { $_.LastWriteTime.ToUniversalTime() -le $resolvedTargetCutoffUtc } | Select-Object -Last 1
            if (-not $selectedLog) {
                throw "No common cutover candidate is available for $($item.DatabaseName) at or before the scheduled cutover time $($resolvedTargetCutoffUtc.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))."
            }

            $selection[$item.StateKey] = [pscustomobject]@{
                DisplayName = "$($item.InstanceName)\$($item.DatabaseName)"
                DatabaseName = $item.DatabaseName
                LastBackupName = $selectedLog.Name
            }
        }

        $options += [pscustomobject]@{
            Index      = 1
            Label      = "Scheduled cutoff at $($resolvedTargetCutoffUtc.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))"
            LastBackup = $selection
        }
    } else {
        $seenSignatures = @{}
        $orderedCutoffs = $candidateCutoffTimes | Sort-Object -Descending -Unique
        foreach ($cutoffTimeUtc in $orderedCutoffs) {
            $selection = [ordered]@{}
            $validSelection = $true
            foreach ($item in $Items) {
                $selectedLog = $candidateSets[$item.StateKey] | Where-Object { $_.LastWriteTime.ToUniversalTime() -le $cutoffTimeUtc } | Select-Object -Last 1
                if (-not $selectedLog) {
                    $validSelection = $false
                    break
                }

                $selection[$item.StateKey] = [pscustomobject]@{
                    DisplayName = "$($item.InstanceName)\$($item.DatabaseName)"
                    DatabaseName = $item.DatabaseName
                    LastBackupName = $selectedLog.Name
                }
            }

            if (-not $validSelection) {
                continue
            }

            $signature = ($selection.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value.LastBackupName)" }) -join ';'
            if ($seenSignatures.ContainsKey($signature)) {
                continue
            }

            $seenSignatures[$signature] = $true

            $options += [pscustomobject]@{
                Index      = $options.Count + 1
                Label      = "Common cutoff at $($cutoffTimeUtc.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))"
                LastBackup = $selection
            }

            if ($options.Count -ge $CandidateCount) {
                break
            }
        }
    }

    if ($options.Count -eq 0) {
        throw 'No common cutover candidates are available across the selected databases.'
    }

    if ($TargetCutoffUtc -and -not $AutoSelectLatest) {
        $selectedOption = $options[0]
    } elseif ($AutoSelectLatest) {
        $selectedOption = $options[0]
    } else {
        Write-Host ''
        Write-Host 'Available cutover candidates:'
        foreach ($option in $options) {
            Write-Host ("[{0}] {1}" -f $option.Index, $option.Label)
            foreach ($entry in $option.LastBackup.GetEnumerator()) {
                Write-Host ("  {0} -> {1}" -f $entry.Value.DisplayName, $entry.Value.LastBackupName)
            }
        }

        $choice = Read-Host -Prompt 'Select the cutover option number, or press Enter to cancel'
        if (-not $choice) {
            Write-Host 'Cutover cancelled.'
            return $false
        }

        $selectedIndex = 0
        if (-not [int]::TryParse($choice, [ref]$selectedIndex)) {
            Write-Warning 'Invalid cutover selection.'
            return $false
        }

        $selectedOption = $options | Where-Object { $_.Index -eq $selectedIndex } | Select-Object -First 1
        if (-not $selectedOption) {
            Write-Warning 'Invalid cutover selection.'
            return $false
        }

        $confirmation = Read-Host -Prompt ("Type CUTOVER to apply option {0}" -f $selectedOption.Index)
        if ($confirmation -cne 'CUTOVER') {
            Write-Host 'Cutover cancelled.'
            return $false
        }
    }

    foreach ($item in $Items) {
        $lastBackupName = $selectedOption.LastBackup[$item.StateKey].LastBackupName
        Write-Host "Completing LRS cutover for $($item.DatabaseName) with $lastBackupName"
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'CutoverRequested' -InstanceName $item.InstanceName -DatabaseName $item.DatabaseName -Message 'Submitting online cutover request.' -Data @{ lastBackupName = $lastBackupName }
        [void](Invoke-AzCliLogReplayComplete -ResourceGroup $ResourceGroup -ManagedInstance $ManagedInstance -DatabaseName $item.DatabaseName -LastBackupName $lastBackupName)
    }

    Write-Host 'Cutover submitted for all databases.'
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Success' -Phase 'Cutover' -Action 'CutoverSubmitted' -Message 'Online cutover submitted for all selected databases.' -Data @{ option = $selectedOption.Label }
    return $true
}

$exampleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoDir = Split-Path -Parent $exampleDir
$pwshDir = Join-Path -Path $repoDir -ChildPath 'pwsh'
$logsDir = Join-Path -Path $repoDir -ChildPath 'logs'
$stateDir = Join-Path -Path $repoDir -ChildPath 'state'
$transferScript = Join-Path -Path $pwshDir -ChildPath 'lrs-backup-transfer.ps1'
$lrsStartWorkerScript = Join-Path -Path $pwshDir -ChildPath 'lrs-online-start-worker.ps1'
$reportHelper = Join-Path -Path $pwshDir -ChildPath 'migration-report.ps1'
. $reportHelper

$runId = [guid]::NewGuid().ToString()
$reportStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$reportDir = Join-Path -Path $repoDir -ChildPath ("reports\migration-$reportStamp")
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null

$wrapperEventLogPath = Join-Path -Path $reportDir -ChildPath 'wrapper-events.jsonl'
$transferEventLogPath = Join-Path -Path $reportDir -ChildPath 'transfer-events.jsonl'
$guidedEventLogPath = Join-Path -Path $reportDir -ChildPath 'guided-events.jsonl'
$migrationReportJsonPath = Join-Path -Path $reportDir -ChildPath 'migration_report.json'
$migrationReportHtmlPath = Join-Path -Path $reportDir -ChildPath 'migration_report.html'
$reportContextPath = Join-Path -Path $reportDir -ChildPath 'transfer-report-context.json'
$databaseNames = @()

if (-not $TransferStatePath) {
    $TransferStatePath = Join-Path -Path $stateDir -ChildPath ("lrs-backup-transfer-$reportStamp.state.json")
}

if (-not $TransferOutputPath) {
    $TransferOutputPath = Join-Path -Path $logsDir -ChildPath ("lrs-backup-transfer-$reportStamp.stdout.log")
}

if (-not $TransferErrorPath) {
    $TransferErrorPath = Join-Path -Path $logsDir -ChildPath ("lrs-backup-transfer-$reportStamp.stderr.log")
}

$lrsStartWorkerResultDir = Join-Path -Path $stateDir -ChildPath ("lrs-online-start-$reportStamp")
$lrsStartWorkerLogsDir = Join-Path -Path $logsDir -ChildPath ("lrs-online-start-$reportStamp")
New-Item -ItemType Directory -Path $lrsStartWorkerResultDir -Force | Out-Null
New-Item -ItemType Directory -Path $lrsStartWorkerLogsDir -Force | Out-Null

function Get-MigrationReportMetadata {
    param([string[]]$DatabaseNameList)

    return [ordered]@{
        mode = 'Online'
        runId = $runId
        tenantId = $TenantId
        subscriptionId = $SubscriptionId
        resourceGroupName = $ResourceGroupName
        managedInstanceName = $ManagedInstanceName
        storageAccountName = $StorageAccountName
        backupRootPath = $BackupRootPath
        selectedInstanceNames = $SelectedInstanceNames
        selectedDatabaseNames = $SelectedDatabaseNames
        resolvedDatabaseNames = $DatabaseNameList
        transferStatePath = $TransferStatePath
        transferOutputPath = $TransferOutputPath
        transferErrorPath = $TransferErrorPath
        reportDirectory = $reportDir
    }
}

function Write-TransferReportContext {
    param([string[]]$DatabaseNameList)

    $context = [ordered]@{
        jsonPath = $migrationReportJsonPath
        htmlPath = $migrationReportHtmlPath
        eventLogPaths = @($wrapperEventLogPath, $transferEventLogPath, $guidedEventLogPath)
        metadata = Get-MigrationReportMetadata -DatabaseNameList $DatabaseNameList
    }

    $context | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $reportContextPath
}

function Update-MigrationReportArtifacts {
    param([string[]]$DatabaseNameList)

    Write-TransferReportContext -DatabaseNameList $DatabaseNameList
    $events = Read-MigrationEventLog -Paths @($wrapperEventLogPath, $transferEventLogPath, $guidedEventLogPath)
    Export-MigrationArtifacts -JsonPath $migrationReportJsonPath -HtmlPath $migrationReportHtmlPath -Events $events -Metadata (Get-MigrationReportMetadata -DatabaseNameList $DatabaseNameList)
}

try {
    if ($StorageAuthMode -eq 'Sas' -and -not $StorageContainerSasToken) {
        throw 'StorageContainerSasToken is required when StorageAuthMode is Sas.'
    }

    $effectiveTransferAuthMode = $StorageAuthMode
    $assumeTransferDeviceLoginReady = $false
    if ($StorageAuthMode -eq 'EntraAzCli' -or $StorageAuthMode -eq 'EntraDevice') {
        Ensure-AzCopyDeviceLogin -Tenant $TenantId
        $effectiveTransferAuthMode = 'EntraDevice'
        $assumeTransferDeviceLoginReady = $true
    }

    $storageContainerIdentity = 'ManagedIdentity'

    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Wrapper' -Action 'Start' -Message 'Online migration wrapper started.' -Data @{
        backupRootPath = $BackupRootPath
        selectedInstanceNames = $SelectedInstanceNames
        selectedDatabaseNames = $SelectedDatabaseNames
        storageAuthMode = $StorageAuthMode
        effectiveTransferAuthMode = $effectiveTransferAuthMode
        storageContainerIdentity = $storageContainerIdentity
    }
    Update-MigrationReportArtifacts -DatabaseNameList @()

    $resolvedAzureContext = Set-AzureExecutionContext -Tenant $TenantId -Subscription $SubscriptionId -AllowAutoReauthenticate $AutoReauthenticate
    $TenantId = $resolvedAzureContext.TenantId
    $SubscriptionId = $resolvedAzureContext.SubscriptionId
    Assert-AzureControlPlaneMfaReady -Tenant $TenantId -Subscription $SubscriptionId -AllowAutoReauthenticate $AutoReauthenticate -OperationLabel 'Online LRS start'

    $instanceFolders = Get-InstanceFolders -RootPath $BackupRootPath
    $instanceFolders = Select-InstanceFolders -InstanceFolderList $instanceFolders -RequestedNames $SelectedInstanceNames
    if (-not $instanceFolders -or $instanceFolders.Count -eq 0) {
        throw "No instance folders were found under '$BackupRootPath'."
    }

    if ($instanceFolders.Count -ne 1) {
        $instanceNames = $instanceFolders | Select-Object -ExpandProperty Name
        throw "Online mode supports exactly one source instance at a time. Multi-instance and same-named database support are intentionally limited to offline mode. Resolved instances: $($instanceNames -join ', ')"
    }

    $migrationItems = Get-MigrationItems -InstanceFolderList $instanceFolders -AccountName $StorageAccountName -RequestedDatabaseNames $SelectedDatabaseNames
    if (-not $migrationItems -or $migrationItems.Count -eq 0) {
        throw 'No databases were discovered for online migration.'
    }

    if ($SelectedDatabaseNames -and $SelectedDatabaseNames.Count -gt 0) {
        $resolvedDatabaseNames = $migrationItems | Select-Object -ExpandProperty DatabaseName -Unique
        $missingDatabaseNames = $SelectedDatabaseNames | ForEach-Object { $_.Trim() } | Where-Object { $_ -and $_ -notin $resolvedDatabaseNames }
        if ($missingDatabaseNames) {
            throw "Requested database folder(s) were not found under the selected instance folder(s): $($missingDatabaseNames -join ', ')"
        }
    }

    $databaseNames = @($migrationItems | Select-Object -ExpandProperty DatabaseName -Unique)
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Discovery' -Action 'TopologyResolved' -Message 'Resolved backup topology for online migration.' -Data @{
        instanceNames = @($instanceFolders | Select-Object -ExpandProperty Name)
        databaseNames = $databaseNames
    }
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames

    foreach ($item in $migrationItems) {
        [void](Get-LatestFullBackupFile -DatabaseRootPath $item.DatabaseRootPath)
    }

    Write-Host '=== Multi-DB Online: Start background upload and polling ==='
    $transferProcess = Start-TransferProcess -ScriptPath $transferScript -SourceRootPath $BackupRootPath -InstanceFilter $SelectedInstanceNames -AccountName $StorageAccountName -Tenant $TenantId -AuthMode $effectiveTransferAuthMode -StorageSasToken $StorageContainerSasToken -AssumeDeviceLoginReady:$assumeTransferDeviceLoginReady -PollSeconds $TransferPollSeconds -StatePath $TransferStatePath -StdOutPath $TransferOutputPath -StdErrPath $TransferErrorPath -DbNames $databaseNames -EventLogPath $transferEventLogPath -ReportContextPath $reportContextPath -RunId $runId -IncludeDiffs:$IncludeDiffs
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Transfer' -Action 'BackgroundProcessStarted' -Message 'Started background transfer process.' -Data @{ processId = $transferProcess.Id; statePath = $TransferStatePath }
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
    Write-PhaseStatusBanner -Phase 'InitialUpload' -Message 'Background upload is running and the wrapper is waiting for the first full/log set to land in storage.' -NextUpdateLocalTime $null -FallbackUpdateText 'on progress change or within 30 seconds'

    $script:monitorCommandsHintShown = $false
    $cutoverSubmitted = $false
    $stopMonitoring = $false
    $transferExitReported = $false
    $scheduledCutoverUtc = $null
    $requestedCutoverUtcHint = Resolve-RequestedCutoverScheduleUtc -RequestedCutoverLocalTime $ScheduledCutoverLocalTime
    $nextScheduledCutoverRetryAt = $null
    $allowTransferToContinue = $false

try {
    Wait-ForInitialUploadCompletion -Process $transferProcess -StatePath $TransferStatePath -ExpectedStateKeys ($migrationItems | Select-Object -ExpandProperty StateKey) -TimeoutMinutes $InitialUploadTimeoutMinutes -StdErrPath $TransferErrorPath
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Success' -Phase 'Transfer' -Action 'InitialUploadCompleted' -Message 'Initial upload completed for all selected databases.'
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames

    $lrsStartFailures = @()
    $lrsReadyItems = @()
    $lrsStartWorkers = @()
    foreach ($item in $migrationItems) {
        try {
            $startHandle = Start-OnlineLrsForItem -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Item $item -StorageIdentity $storageContainerIdentity -StorageSasToken $StorageContainerSasToken -WorkerScriptPath $lrsStartWorkerScript -TenantId $TenantId -SubscriptionId $SubscriptionId -WorkerResultDir $lrsStartWorkerResultDir -WorkerLogsDir $lrsStartWorkerLogsDir
            if ($startHandle.Mode -eq 'Reused') {
                $lrsReadyItems += $item
            } elseif ($startHandle.Mode -eq 'Worker') {
                $lrsStartWorkers += $startHandle.Worker
            }
        } catch {
            $failureMessage = $_.Exception.Message
            Write-Warning "Failed to start online LRS for $($item.DatabaseName). $failureMessage"
            Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase 'LRS' -Action 'StartFailed' -InstanceName $item.InstanceName -DatabaseName $item.DatabaseName -Message $failureMessage -Data @{ storageUri = $item.StorageUri }
            $lrsStartFailures += [pscustomobject]@{
                DatabaseName = $item.DatabaseName
                Message = $failureMessage
            }
        }
    }

    if ($lrsStartWorkers.Count -gt 0) {
        $lrsStartupMessage = if ($requestedCutoverUtcHint) {
            "Waiting for restore startup to accept the next transaction log before scheduled cutover at $(([datetime]$requestedCutoverUtcHint).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))."
        } else {
            'Waiting for restore startup to accept the next transaction log or for existing restore state to be detected.'
        }
        Write-PhaseStatusBanner -Phase 'LrsStartup' -Message $lrsStartupMessage -NextUpdateLocalTime (Get-Date).AddMinutes($StatusIntervalMinutes) -FallbackUpdateText $null
    }

    $workerResults = @(Wait-ForOnlineLrsWorkerProcesses -WorkerList $lrsStartWorkers -StatusIntervalMinutes $StatusIntervalMinutes -ScheduledCutoverUtc $requestedCutoverUtcHint -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems -StatePath $TransferStatePath -TransferProcess $transferProcess)
    $invalidWorkerResults = @($workerResults | Where-Object { -not (Test-OnlineLrsWorkerResultObject -InputObject $_) })
    if ($invalidWorkerResults.Count -gt 0) {
        $invalidTypes = @($invalidWorkerResults | ForEach-Object { if ($null -eq $_) { 'null' } else { $_.GetType().FullName } }) -join ', '
        throw "Unexpected startup worker result payload encountered. Types: $invalidTypes"
    }

    foreach ($workerResult in $workerResults) {
        if ($workerResult.Success) {
            $lrsReadyItems += $workerResult.Item
            Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Success' -Phase 'LRS' -Action 'StartSubmitted' -InstanceName $workerResult.Item.InstanceName -DatabaseName $workerResult.Item.DatabaseName -Message 'Online LRS start request submitted.' -Data @{ storageUri = $workerResult.Item.StorageUri }
            continue
        }

        if ($workerResult.Message -like 'Timed out waiting for LRS start worker to finish*') {
            try {
                $existingState = Get-ExistingOnlineRestoreState -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -DatabaseName $workerResult.Item.DatabaseName
                if ($existingState.Found) {
                    Write-Host "LRS start worker timed out for $($workerResult.Item.DatabaseName), but the restore already exists with status $($existingState.Status). Continuing monitoring."
                    $lrsReadyItems += $workerResult.Item
                    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'LRS' -Action 'ReuseExistingAfterWorkerTimeout' -InstanceName $workerResult.Item.InstanceName -DatabaseName $workerResult.Item.DatabaseName -Message 'Worker timed out, but an existing online restore session was detected.' -Data @{ status = $existingState.Status; kind = $existingState.Kind; storageUri = $workerResult.Item.StorageUri }
                    continue
                }

                Write-Host "LRS start worker timed out for $($workerResult.Item.DatabaseName). Retrying directly in the current session..."
                $inlineOutput = Invoke-AzPowerShellLogReplayStart -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -DatabaseName $workerResult.Item.DatabaseName -StorageUri $workerResult.Item.StorageUri -StorageIdentity $storageContainerIdentity -StorageSasToken $StorageContainerSasToken
                $lrsReadyItems += $workerResult.Item
                Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Success' -Phase 'LRS' -Action 'StartSubmittedInlineFallback' -InstanceName $workerResult.Item.InstanceName -DatabaseName $workerResult.Item.DatabaseName -Message 'Online LRS start request submitted after worker timeout by retrying in the current session.' -Data @{ storageUri = $workerResult.Item.StorageUri; output = $inlineOutput }
                continue
            } catch {
                $existingState = Get-ExistingOnlineRestoreState -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -DatabaseName $workerResult.Item.DatabaseName
                if ($existingState.Found) {
                    Write-Host "Inline retry reported a collision for $($workerResult.Item.DatabaseName), but the restore already exists with status $($existingState.Status). Continuing monitoring."
                    $lrsReadyItems += $workerResult.Item
                    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'LRS' -Action 'ReuseExistingAfterInlineRetry' -InstanceName $workerResult.Item.InstanceName -DatabaseName $workerResult.Item.DatabaseName -Message 'Inline retry collided with an already-created online restore session. Continuing monitoring.' -Data @{ status = $existingState.Status; kind = $existingState.Kind; storageUri = $workerResult.Item.StorageUri }
                    continue
                }

                $workerResult.Message = "Worker timed out and inline retry failed. $($_.Exception.Message)"
            }
        }

        Write-Warning "Failed to start online LRS for $($workerResult.Item.DatabaseName). $($workerResult.Message)"
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase 'LRS' -Action 'StartFailed' -InstanceName $workerResult.Item.InstanceName -DatabaseName $workerResult.Item.DatabaseName -Message $workerResult.Message -Data @{ storageUri = $workerResult.Item.StorageUri }
        $lrsStartFailures += [pscustomobject]@{
            DatabaseName = $workerResult.Item.DatabaseName
            Message = $workerResult.Message
        }
    }

    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames

    if ($lrsStartFailures.Count -gt 0) {
        $failureSummary = ($lrsStartFailures | ForEach-Object { "$($_.DatabaseName): $($_.Message)" }) -join '; '
        Invoke-OnlineStartupFailureCleanup -TransferProcess $transferProcess -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems
        $transferExitReported = $true
        throw "Online mode requires all selected databases for the instance to enter LRS before monitoring and cutover scheduling continue. $failureSummary"
    }

    $scheduledCutoverUtc = Resolve-RequestedCutoverScheduleUtc -RequestedCutoverLocalTime $ScheduledCutoverLocalTime
    if ($null -eq $scheduledCutoverUtc) {
        $scheduledCutoverUtc = Read-GroupCutoverScheduleUtc
    }

    if ($null -ne $scheduledCutoverUtc) {
        $scheduleSource = if ($null -ne $ScheduledCutoverLocalTime) { 'parameter' } else { 'prompt' }
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'ScheduleSelected' -Message 'Scheduled group cutover selected.' -Data @{ cutoverUtc = ([datetime]$scheduledCutoverUtc).ToString('o'); cutoverLocal = ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'); source = $scheduleSource }
        Write-Host ("Scheduled group cutover for {0}" -f ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))
    } else {
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'ScheduleDeferred' -Message 'Group cutover schedule deferred by operator.'
    }
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames

    $monitoringBannerMessage = if ($scheduledCutoverUtc) {
        "Monitoring restore/upload progress until the scheduled cutover window is ready."
    } else {
        "Monitoring restore/upload progress and waiting for operator action."
    }
    Write-PhaseStatusBanner -Phase 'Monitoring' -Message $monitoringBannerMessage -NextUpdateLocalTime (Get-Date) -FallbackUpdateText $null

    $nextStatusAt = Get-Date
    $cutoverCatchupReminderShown = $false
    while (-not $cutoverSubmitted -and -not $stopMonitoring) {
        if ((Get-Date) -ge $nextStatusAt) {
            Show-MigrationSnapshot -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems -StatePath $TransferStatePath -TransferProcess $transferProcess -ScheduledCutoverUtc $scheduledCutoverUtc
            Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Monitoring' -Action 'SnapshotRendered' -Message 'Rendered migration status snapshot.'
            Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
            $nextStatusAt = (Get-Date).AddMinutes($StatusIntervalMinutes)
        }

        if ($scheduledCutoverUtc -and ((Get-Date).ToUniversalTime() -ge ([datetime]$scheduledCutoverUtc))) {
            if (Test-CutoverScheduleReady -Items $migrationItems -StatePath $TransferStatePath -TargetCutoverUtc ([datetime]$scheduledCutoverUtc)) {
                $cutoverCatchupReminderShown = $false
                if (-not $nextScheduledCutoverRetryAt -or (Get-Date) -ge $nextScheduledCutoverRetryAt) {
                    try {
                        Write-Host ("Scheduled cutover time reached for {0}. Submitting group cutover now." -f ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))
                        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'ScheduledCutoverTriggered' -Message 'Scheduled group cutover conditions were satisfied. Starting automatic cutover.' -Data @{ cutoverUtc = ([datetime]$scheduledCutoverUtc).ToString('o'); cutoverLocal = ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
                        $cutoverSubmitted = Invoke-OnlineCutover -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems -StatePath $TransferStatePath -CandidateCount $CutoverCandidateCount -TargetCutoffUtc ([datetime]$scheduledCutoverUtc) -AutoSelectLatest
                        $nextScheduledCutoverRetryAt = $null
                    } catch {
                        if (Test-CriticalOnlineCutoverFailure -Message $_.Exception.Message) {
                            Stop-OnlineProcessesForCriticalFailure -TransferProcess $transferProcess -FailurePhase 'Cutover' -FailureMessage $_.Exception.Message
                            throw "Critical scheduled cutover failure - automatic retry suppressed because the failure may have left the restoring database in an inconsistent state (for example InternalServerError on completeRestore can drop the placeholder DB, causing follow-up calls to return ResourceNotFound). Investigate the activity log, then re-run online LRS from a fresh FULL backup if needed. If the underlying error is InternalServerError and storage auth is ManagedIdentity, this matches a known SQL engine defect with long AAD tokens (fix pending in an upcoming CU); rerun with -StorageContainerSasToken (SharedAccessSignature) as a workaround. Underlying error: $($_.Exception.Message)"
                        }

                        $nextScheduledCutoverRetryAt = (Get-Date).AddSeconds([Math]::Max(15, $TransferPollSeconds))
                        Write-Warning "Scheduled cutover attempt failed. $($_.Exception.Message)"
                        Write-Host ("Automatic cutover will retry after {0}. Background transfer remains active." -f $nextScheduledCutoverRetryAt.ToString('yyyy-MM-dd HH:mm:ss'))
                        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase 'Cutover' -Action 'ScheduledCutoverFailed' -Message $_.Exception.Message -Data @{ cutoverUtc = ([datetime]$scheduledCutoverUtc).ToString('o'); retryAfterLocal = $nextScheduledCutoverRetryAt.ToString('yyyy-MM-dd HH:mm:ss') }
                    }
                    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
                }
                continue
            }

            if (-not $cutoverCatchupReminderShown) {
                Write-Host ("Scheduled cutover time has passed, but uploaded logs have not yet caught up for all databases. Waiting for transfer state to reach {0}." -f ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))
                $cutoverCatchupReminderShown = $true
            }
        } else {
            $cutoverCatchupReminderShown = $false
        }

        if ($transferProcess.HasExited -and -not $transferExitReported) {
            Write-Warning "Background transfer process exited with code $($transferProcess.ExitCode)."
            Show-TransferLogTail -Path $TransferErrorPath
            Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Warning' -Phase 'Transfer' -Action 'BackgroundProcessExited' -Message "Background transfer process exited with code $($transferProcess.ExitCode)." -Data @{ errorLogPath = $TransferErrorPath }
            Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
            $transferExitReported = $true
        }

        $operatorKey = Read-OperatorKey
        switch ($operatorKey) {
            'S' {
                Show-MigrationSnapshot -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems -StatePath $TransferStatePath -TransferProcess $transferProcess -ScheduledCutoverUtc $scheduledCutoverUtc
                Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Monitoring' -Action 'StatusRequested' -Message 'Operator requested immediate status snapshot.'
                Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
                $nextStatusAt = (Get-Date).AddMinutes($StatusIntervalMinutes)
            }
            'C' {
                try {
                    $cutoverSubmitted = Invoke-OnlineCutover -ResourceGroup $ResourceGroupName -ManagedInstance $ManagedInstanceName -Items $migrationItems -StatePath $TransferStatePath -CandidateCount $CutoverCandidateCount -TargetCutoffUtc $null
                } catch {
                    if (Test-CriticalOnlineCutoverFailure -Message $_.Exception.Message) {
                        Stop-OnlineProcessesForCriticalFailure -TransferProcess $transferProcess -FailurePhase 'Cutover' -FailureMessage $_.Exception.Message
                        throw "Critical manual cutover failure - automatic retry suppressed because the failure may have left the restoring database in an inconsistent state (for example InternalServerError on completeRestore can drop the placeholder DB, causing follow-up calls to return ResourceNotFound). Investigate the activity log, then re-run online LRS from a fresh FULL backup if needed. If the underlying error is InternalServerError and storage auth is ManagedIdentity, this matches a known SQL engine defect with long AAD tokens (fix pending in an upcoming CU); rerun with -StorageContainerSasToken (SharedAccessSignature) as a workaround. Underlying error: $($_.Exception.Message)"
                    }

                    Write-Warning "Manual cutover attempt failed. $($_.Exception.Message)"
                    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase 'Cutover' -Action 'ManualCutoverFailed' -Message $_.Exception.Message
                }
                Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
            }
            'T' {
                $scheduledCutoverUtc = Read-GroupCutoverScheduleUtc
                $cutoverCatchupReminderShown = $false
                if ($scheduledCutoverUtc) {
                    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'ScheduleSelected' -Message 'Scheduled group cutover selected.' -Data @{ cutoverUtc = ([datetime]$scheduledCutoverUtc).ToString('o'); cutoverLocal = ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
                    Write-Host ("Scheduled group cutover for {0}" -f ([datetime]$scheduledCutoverUtc).ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss'))
                } else {
                    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Cutover' -Action 'ScheduleDeferred' -Message 'Group cutover schedule deferred by operator.'
                    Write-Host 'Group cutover schedule cleared.'
                }
                Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
            }
            'Q' {
                Write-Host 'Exiting the wrapper without cutover. Background upload process will continue running.'
                Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Warning' -Phase 'Monitoring' -Action 'QuitRequested' -Message 'Operator stopped monitoring without cutover.'
                Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
                $allowTransferToContinue = $true
                $stopMonitoring = $true
            }
        }

        if ($cutoverSubmitted -or $stopMonitoring) {
            break
        }

        Start-Sleep -Seconds 1
    }
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Success' -Phase 'Wrapper' -Action 'Completed' -Message 'Online migration wrapper completed.' -Data @{ cutoverSubmitted = $cutoverSubmitted; stopMonitoring = $stopMonitoring }
} finally {
    if ($cutoverSubmitted -and $transferProcess -and -not $transferProcess.HasExited) {
        Stop-Process -Id $transferProcess.Id -Force
        Write-Host 'Stopped background transfer process after cutover submission.'
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Info' -Phase 'Transfer' -Action 'BackgroundProcessStopped' -Message 'Stopped background transfer process after cutover submission.' -Data @{ processId = $transferProcess.Id }
    } elseif (-not $allowTransferToContinue -and $transferProcess -and -not $transferProcess.HasExited) {
        Stop-Process -Id $transferProcess.Id -Force
        Write-Host 'Stopped background transfer process because the online wrapper exited without a monitored continuation path.'
        Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Warning' -Phase 'Transfer' -Action 'BackgroundProcessStoppedOnExit' -Message 'Stopped background transfer process because the online wrapper exited without cutover or operator continuation.' -Data @{ processId = $transferProcess.Id }
    }
}
} catch {
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Online' -Level 'Error' -Phase 'Wrapper' -Action 'Failed' -Message $_.Exception.Message
    throw
} finally {
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
    Write-Host "Migration report: $migrationReportHtmlPath"
}
