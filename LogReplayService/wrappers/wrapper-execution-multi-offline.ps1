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
    [string[]]$SelectedDatabaseNames
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$eventSourceScriptName = Split-Path -Leaf $PSCommandPath

function Write-PhaseStatusBanner {
    param(
        [string]$Phase,
        [string]$Message,
        [string]$NextUpdateText
    )

    Write-Host ''
    Write-Host ("=== Current Phase: {0} ===" -f $Phase) -ForegroundColor Green
    if ($Message) {
        Write-Host $Message
    }

    if ($NextUpdateText) {
        Write-Host ("Next expected console update: {0}" -f $NextUpdateText)
    }
}

function Show-OfflineSelectionSummary {
    param(
        [System.IO.DirectoryInfo[]]$InstanceFolders,
        [string[]]$DatabaseNames
    )

    $rows = foreach ($instanceFolder in $InstanceFolders) {
        $instanceDatabases = @(Select-DatabaseNames -DatabaseNameList (Get-DatabaseNames -InstancePath $instanceFolder.FullName) -RequestedNames $SelectedDatabaseNames -InstanceName $instanceFolder.Name)
        [pscustomobject]@{
            Instance = $instanceFolder.Name
            Databases = $instanceDatabases.Count
            DatabaseList = ($instanceDatabases -join ', ')
        }
    }

    Write-Host ''
    Write-Host ("Resolved selection: {0} instance(s), {1} database(s)" -f $InstanceFolders.Count, $DatabaseNames.Count)
    $rows | Format-Table -AutoSize | Out-Host
}

function Write-OfflinePhaseCompletion {
    param(
        [string]$Phase,
        [datetime]$StartedAt,
        [string]$Message
    )

    $durationText = Format-Duration -Duration ((Get-Date) - $StartedAt)
    Write-Host ("{0} complete in {1}. {2}" -f $Phase, $durationText, $Message)
}

function Show-InstanceRestorePlan {
    param(
        [string]$InstanceName,
        [string[]]$DatabaseNames,
        [string]$StorageUriTemplate,
        [string]$GuidedLogPath
    )

    Write-Host ''
    Write-Host ("Starting guided restore for instance {0}" -f $InstanceName)
    Write-Host ("Databases: {0}" -f ($DatabaseNames -join ', '))
    Write-Host ("Storage template: {0}" -f $StorageUriTemplate)
    Write-Host ("Restore log: {0}" -f $GuidedLogPath)
}

function Format-Duration {
    param([timespan]$Duration)

    if ($Duration.TotalSeconds -lt 60) {
        return ("{0}s" -f [Math]::Max(0, [int][Math]::Round($Duration.TotalSeconds)))
    }

    if ($Duration.TotalMinutes -lt 60) {
        return ("{0}m {1}s" -f [int][Math]::Floor($Duration.TotalMinutes), $Duration.Seconds)
    }

    return ("{0}h {1}m {2}s" -f [int][Math]::Floor($Duration.TotalHours), $Duration.Minutes, $Duration.Seconds)
}

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
        return @($InstanceFolderList)
    }

    $requestedLookup = $RequestedNames | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $selectedFolders = $InstanceFolderList | Where-Object { $_.Name -in $requestedLookup }
    $missingNames = $requestedLookup | Where-Object { $_ -notin ($selectedFolders | Select-Object -ExpandProperty Name) }
    if ($missingNames) {
        throw "Requested instance folder(s) were not found under '$BackupRootPath': $($missingNames -join ', ')"
    }

    return @($selectedFolders)
}

function Select-DatabaseNames {
    param(
        [string[]]$DatabaseNameList,
        [string[]]$RequestedNames,
        [string]$InstanceName
    )

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return @($DatabaseNameList)
    }

    $selectionRules = @(Get-DatabaseSelectionRules -RequestedNames $RequestedNames)
    $globalLookup = @($selectionRules | Where-Object { -not $_.InstanceName } | Select-Object -ExpandProperty DatabaseName -Unique)
    $instanceLookup = @($selectionRules | Where-Object { $_.InstanceName -eq $InstanceName } | Select-Object -ExpandProperty DatabaseName -Unique)
    $effectiveLookup = @($globalLookup + $instanceLookup | Select-Object -Unique)

    if ($effectiveLookup.Count -eq 0) {
        return @()
    }

    return @($DatabaseNameList | Where-Object { $_ -in $effectiveLookup })
}

function Get-DatabaseSelectionRules {
    param([string[]]$RequestedNames)

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return @()
    }

    $rules = @()
    foreach ($requestedName in $RequestedNames) {
        $trimmedName = [string]$requestedName
        $trimmedName = $trimmedName.Trim()
        if (-not $trimmedName) {
            continue
        }

        $instanceName = $null
        $databaseName = $trimmedName
        if ($trimmedName -match '^(?<instance>.+?)(?:\\|::)(?<database>.+)$') {
            $instanceName = [string]$Matches.instance
            $databaseName = [string]$Matches.database
        }

        $rules += [pscustomobject]@{
            RawName = $trimmedName
            InstanceName = $instanceName
            DatabaseName = $databaseName
        }
    }

    return @($rules)
}

function Get-SelectedDatabaseEntries {
    param(
        [System.IO.DirectoryInfo[]]$InstanceFolderList,
        [string[]]$RequestedDatabaseNames
    )

    $entries = @()
    foreach ($instanceFolder in $InstanceFolderList) {
        $instanceDatabases = @(Select-DatabaseNames -DatabaseNameList (Get-DatabaseNames -InstancePath $instanceFolder.FullName) -RequestedNames $RequestedDatabaseNames -InstanceName $instanceFolder.Name)
        foreach ($databaseName in $instanceDatabases) {
            $entries += [pscustomobject]@{
                InstanceName = $instanceFolder.Name
                DatabaseName = $databaseName
            }
        }
    }

    return @($entries)
}

function Assert-NoCrossInstanceDatabaseNameConflicts {
    param([object[]]$DatabaseEntries)

    $conflicts = @($DatabaseEntries | Group-Object -Property DatabaseName | Where-Object { $_.Count -gt 1 })
    if ($conflicts.Count -eq 0) {
        return
    }

    $conflictText = @($conflicts | ForEach-Object {
        "{0} ({1})" -f $_.Name, (($_.Group | Select-Object -ExpandProperty InstanceName | Sort-Object -Unique) -join ', ')
    }) -join '; '

    throw "cannot restore databases with same name: $conflictText"
}

function Assert-RequestedDatabasesResolved {
    param(
        [object[]]$SelectionRules,
        [object[]]$DatabaseEntries
    )

    if (-not $SelectionRules -or $SelectionRules.Count -eq 0) {
        return
    }

    $missingSelections = @()
    foreach ($selectionRule in $SelectionRules) {
        if ($selectionRule.InstanceName) {
            $match = $DatabaseEntries | Where-Object { $_.InstanceName -eq $selectionRule.InstanceName -and $_.DatabaseName -eq $selectionRule.DatabaseName } | Select-Object -First 1
        } else {
            $match = $DatabaseEntries | Where-Object { $_.DatabaseName -eq $selectionRule.DatabaseName } | Select-Object -First 1
        }

        if (-not $match) {
            $missingSelections += $selectionRule.RawName
        }
    }

    if ($missingSelections.Count -gt 0) {
        throw "Requested database folder(s) were not found under the selected instance folder(s): $($missingSelections -join ', ')"
    }
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

    if (-not $files) {
        return @()
    }

    return @($files)
}

function Get-LastBackupFileNameForDatabase {
    param([string]$DatabaseRootPath)

    $fullFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('FULL', 'Full', 'full')
    $diffFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('DIFF', 'Diff', 'diff')
    $logFolderPath = Get-BackupFolderPath -DatabaseRootPath $DatabaseRootPath -CandidateNames @('LOG', 'Log', 'log', 'TRAN', 'Tran', 'tran', 'TLOG', 'tlog', 'Logs', 'logs')

    $candidates = @()
    $candidates += Get-OrderedBackupFiles -FolderPath $fullFolderPath -Extensions @('.bak')
    $candidates += Get-OrderedBackupFiles -FolderPath $diffFolderPath -Extensions @('.bak', '.diff', '.dif')
    $candidates += Get-OrderedBackupFiles -FolderPath $logFolderPath -Extensions @('.trn')

    if (-not $candidates -or $candidates.Count -eq 0) {
        throw "No backup files were found under '$DatabaseRootPath'."
    }

    # TODO: Replace lexicographic last-backup selection with restore-chain-aware ordering.
    return ($candidates | Sort-Object -Property Name | Select-Object -Last 1).Name
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

$exampleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoDir = Split-Path -Parent $exampleDir
$pwshDir = Join-Path -Path $repoDir -ChildPath 'pwsh'
$logsDir = Join-Path -Path $repoDir -ChildPath 'logs'
$stateDir = Join-Path -Path $repoDir -ChildPath 'state'
$transferScript = Join-Path -Path $pwshDir -ChildPath 'lrs-backup-transfer.ps1'
$guidedScript = Join-Path -Path $pwshDir -ChildPath 'lrs-guided.ps1'
$reportHelper = Join-Path -Path $pwshDir -ChildPath 'migration-report.ps1'
. $reportHelper

$runId = [guid]::NewGuid().ToString()
$reportStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$reportDir = Join-Path -Path $repoDir -ChildPath ("reports\migration-$reportStamp")
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
$transferStatePath = Join-Path -Path $stateDir -ChildPath ("lrs-backup-transfer-$reportStamp.state.json")

$wrapperEventLogPath = Join-Path -Path $reportDir -ChildPath 'wrapper-events.jsonl'
$transferEventLogPath = Join-Path -Path $reportDir -ChildPath 'transfer-events.jsonl'
$guidedEventLogPath = Join-Path -Path $reportDir -ChildPath 'guided-events.jsonl'
$migrationReportJsonPath = Join-Path -Path $reportDir -ChildPath 'migration_report.json'
$migrationReportHtmlPath = Join-Path -Path $reportDir -ChildPath 'migration_report.html'
$databaseNames = @()

function Update-MigrationReportArtifacts {
    param([string[]]$DatabaseNameList)

    $events = Read-MigrationEventLog -Paths @($wrapperEventLogPath, $transferEventLogPath, $guidedEventLogPath)
    Export-MigrationArtifacts -JsonPath $migrationReportJsonPath -HtmlPath $migrationReportHtmlPath -Events $events -Metadata @{
        mode = 'Offline'
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
        reportDirectory = $reportDir
    }
}

function Get-ChildPowerShellPath {
    $pwshCommand = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($pwshCommand) {
        return $pwshCommand.Source
    }

    $windowsPowerShell = Get-Command powershell.exe -ErrorAction SilentlyContinue
    if ($windowsPowerShell) {
        return $windowsPowerShell.Source
    }

    throw 'Unable to locate pwsh or powershell.exe for guided restore execution.'
}

function Invoke-GuidedRestoreProcess {
    param(
        [string]$InstanceFolderName,
        [string[]]$DatabaseNameList,
        [string]$StorageUriTemplate,
        [hashtable]$LastBackupMap,
        [string]$StorageIdentity,
        [string]$StorageSasToken
    )

    $safeInstanceName = ($InstanceFolderName -replace '[^a-zA-Z0-9.-]', '_')
    $configPath = Join-Path -Path $reportDir -ChildPath "lrs-guided-$safeInstanceName.config.json"
    $guidedLogPath = Join-Path -Path $logsDir -ChildPath ("lrs-guided-$reportStamp-$safeInstanceName.log")
    $config = [ordered]@{
        ResourceGroupName = $ResourceGroupName
        InstanceName = $ManagedInstanceName
        DatabaseNames = $DatabaseNameList
        StorageContainerUriTemplate = $StorageUriTemplate
        StorageContainerIdentity = $StorageIdentity
        StorageContainerSasToken = $StorageSasToken
        LastBackupNames = $LastBackupMap
        MonitorMinutes = 60
        PollSeconds = 60
        LogPath = $guidedLogPath
    }

    $config | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $configPath

    $shellPath = Get-ChildPowerShellPath
    $argumentList = @(
        '-NoProfile'
        '-File'
        $guidedScript
        '-Mode'
        'Offline'
        '-ConfigPath'
        $configPath
        '-EventLogPath'
        $guidedEventLogPath
        '-RunId'
        $runId
        '-QuietConsole'
    )

    Show-InstanceRestorePlan -InstanceName $InstanceFolderName -DatabaseNames $DatabaseNameList -StorageUriTemplate $StorageUriTemplate -GuidedLogPath $guidedLogPath

    & $shellPath @argumentList
    if ($LASTEXITCODE -ne 0) {
        throw "Guided restore process failed for instance folder '$InstanceFolderName' with exit code $LASTEXITCODE."
    }
}

try {
if ($StorageAuthMode -eq 'Sas' -and -not $StorageContainerSasToken) {
    throw 'StorageContainerSasToken is required when StorageAuthMode is Sas.'
}

$storageContainerIdentity = 'ManagedIdentity'

Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Info' -Phase 'Wrapper' -Action 'Start' -Message 'Offline migration wrapper started.' -Data @{
    backupRootPath = $BackupRootPath
    selectedInstanceNames = $SelectedInstanceNames
    selectedDatabaseNames = $SelectedDatabaseNames
    storageAuthMode = $StorageAuthMode
    storageContainerIdentity = $storageContainerIdentity
}
Update-MigrationReportArtifacts -DatabaseNameList @()

$resolvedAzureContext = Set-AzureExecutionContext -Tenant $TenantId -Subscription $SubscriptionId -AllowAutoReauthenticate $AutoReauthenticate
$TenantId = $resolvedAzureContext.TenantId
$SubscriptionId = $resolvedAzureContext.SubscriptionId
Assert-AzureControlPlaneMfaReady -Tenant $TenantId -Subscription $SubscriptionId -AllowAutoReauthenticate $AutoReauthenticate -OperationLabel 'Offline LRS start'

$wrapperStartedAt = Get-Date

$instanceFolders = @(Get-InstanceFolders -RootPath $BackupRootPath)
$instanceFolders = @(Select-InstanceFolders -InstanceFolderList $instanceFolders -RequestedNames $SelectedInstanceNames)
$databaseSelectionRules = @(Get-DatabaseSelectionRules -RequestedNames $SelectedDatabaseNames)

$selectedDatabaseEntries = @(Get-SelectedDatabaseEntries -InstanceFolderList $instanceFolders -RequestedDatabaseNames $SelectedDatabaseNames)
Assert-NoCrossInstanceDatabaseNameConflicts -DatabaseEntries $selectedDatabaseEntries
Assert-RequestedDatabasesResolved -SelectionRules $databaseSelectionRules -DatabaseEntries $selectedDatabaseEntries

$databaseNames = @($selectedDatabaseEntries | Select-Object -ExpandProperty DatabaseName -Unique)

if (-not $instanceFolders -or $instanceFolders.Count -eq 0) {
    throw "No instance folders were found under '$BackupRootPath'."
}

Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Info' -Phase 'Discovery' -Action 'TopologyResolved' -Message 'Resolved backup topology for offline migration.' -Data @{
    instanceNames = @($instanceFolders | Select-Object -ExpandProperty Name)
    databaseNames = $databaseNames
}
Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
Write-PhaseStatusBanner -Phase 'Discovery' -Message 'Backup topology resolved for offline migration.' -NextUpdateText 'immediately after transfer begins'
Show-OfflineSelectionSummary -InstanceFolders $instanceFolders -DatabaseNames $databaseNames

$transferArgs = @{
    Mode = 'Offline'
    SourcePathBase = $BackupRootPath
    StorageAccountName = $StorageAccountName
    StorageAuthMode = $StorageAuthMode
    StatePath = $transferStatePath
    EventLogPath = $transferEventLogPath
    RunId = $runId
    QuietConsole = $true
}

if ($IncludeDiffs) {
    $transferArgs['IncludeDiffs'] = $true
}

if ($TenantId) {
    $transferArgs['TenantId'] = $TenantId
}

if ($StorageContainerSasToken) {
    $transferArgs['StorageContainerSasToken'] = $StorageContainerSasToken
}

if ($SelectedInstanceNames -and $SelectedInstanceNames.Count -gt 0) {
    $transferArgs['InstanceNames'] = $SelectedInstanceNames
}

if ($SelectedDatabaseNames -and $SelectedDatabaseNames.Count -gt 0) {
    $transferArgs['DatabaseNames'] = $SelectedDatabaseNames
}

$transferStartedAt = Get-Date
Write-PhaseStatusBanner -Phase 'Transfer' -Message 'Uploading offline backup sets to storage for the selected databases.' -NextUpdateText 'when transfer completes'
& $transferScript @transferArgs
Write-OfflinePhaseCompletion -Phase 'Transfer' -StartedAt $transferStartedAt -Message 'Offline backup transfer finished.'
Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Success' -Phase 'Transfer' -Action 'TransferCompleted' -Message 'Offline backup transfer finished.'
Update-MigrationReportArtifacts -DatabaseNameList $databaseNames

foreach ($instanceFolder in $instanceFolders) {
    $containerName = Get-ContainerNameFromInstanceFolder -Value $instanceFolder.Name
    $storageUriTemplate = "https://$StorageAccountName.blob.core.windows.net/$containerName/{db}/"
    $instanceDatabaseNames = @(Select-DatabaseNames -DatabaseNameList (Get-DatabaseNames -InstancePath $instanceFolder.FullName) -RequestedNames $SelectedDatabaseNames -InstanceName $instanceFolder.Name)

    if (-not $instanceDatabaseNames -or $instanceDatabaseNames.Count -eq 0) {
        continue
    }

    $instanceLastBackupMap = @{}
    foreach ($dbName in $instanceDatabaseNames) {
        $databaseRootPath = Join-Path -Path $instanceFolder.FullName -ChildPath $dbName
        $instanceLastBackupMap[$dbName] = Get-LastBackupFileNameForDatabase -DatabaseRootPath $databaseRootPath
    }

    $restoreStartedAt = Get-Date
    Write-PhaseStatusBanner -Phase 'Restore' -Message ("Submitting guided offline restore for instance {0}." -f $instanceFolder.Name) -NextUpdateText 'after guided restore returns for this instance'
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Info' -Phase 'LRS' -Action 'InstanceRestoreStart' -InstanceName $instanceFolder.Name -Message 'Starting guided offline restore for instance folder.' -Data @{ databaseNames = $instanceDatabaseNames; storageUriTemplate = $storageUriTemplate }
    Invoke-GuidedRestoreProcess -InstanceFolderName $instanceFolder.Name -DatabaseNameList $instanceDatabaseNames -StorageUriTemplate $storageUriTemplate -LastBackupMap $instanceLastBackupMap -StorageIdentity $storageContainerIdentity -StorageSasToken $StorageContainerSasToken
    Write-OfflinePhaseCompletion -Phase 'Restore' -StartedAt $restoreStartedAt -Message ("Guided restore completed for instance {0}." -f $instanceFolder.Name)
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Success' -Phase 'LRS' -Action 'InstanceRestoreCompleted' -InstanceName $instanceFolder.Name -Message 'Guided offline restore completed for instance folder.' -Data @{ databaseNames = $instanceDatabaseNames }
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
}

Write-PhaseStatusBanner -Phase 'Completed' -Message 'Offline migration wrapper completed successfully.' -NextUpdateText $null
Write-Host ("Total runtime: {0}" -f (Format-Duration -Duration ((Get-Date) - $wrapperStartedAt)))
Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Success' -Phase 'Wrapper' -Action 'Completed' -Message 'Offline migration wrapper completed successfully.'
} catch {
    Write-MigrationEvent -EventLogPath $wrapperEventLogPath -RunId $runId -Source $eventSourceScriptName -Mode 'Offline' -Level 'Error' -Phase 'Wrapper' -Action 'Failed' -Message $_.Exception.Message
    throw
} finally {
    Update-MigrationReportArtifacts -DatabaseNameList $databaseNames
    Write-Host "Migration report: $migrationReportHtmlPath"
}
