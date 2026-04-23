[CmdletBinding()]
param(
    [ValidateSet('Offline', 'Online')]
    [string]$Mode,

    [string]$SourcePath,
    [string]$SourcePathBase,
    [string[]]$InstanceNames,
    [string[]]$DatabaseNames,
    [string]$InstanceNamesList,
    [string]$DatabaseNamesList,
    [string]$InstanceNamesJson,
    [string]$DatabaseNamesJson,
    [string]$FullBackupPath,
    [string]$FullBackupFile,
    [string]$DiffPath,
    [string]$TranPath,
    [string]$TranStartFile,
    [string]$StorageAccountName,
    [string]$StorageContainerUri,
    [string]$StorageContainerUriTemplate,
    [ValidateSet('EntraAzCli', 'EntraDevice', 'Sas')]
    [string]$StorageAuthMode = 'EntraAzCli',
    [string]$TenantId,
    [string]$StorageContainerSasToken,

    [int]$IntervalSeconds = 300,
    [string]$StatePath = "$(Split-Path -Parent $MyInvocation.MyCommand.Path)\lrs-backup-transfer.state.json",
    [string]$EventLogPath,
    [string]$ReportContextPath,
    [string]$RunId,
    [int]$ConnectionRetryCount = 5,
    [int]$ConnectionRetryDelaySeconds = 5,
    [switch]$AssumeDeviceLoginReady,
    [switch]$QuietConsole,

    [switch]$IncludeDiffs,
    [switch]$WhatIf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$DefaultSqlBackupsRoot = 'C:\SqlBackups'
$reportHelperPath = Join-Path -Path (Split-Path -Parent $MyInvocation.MyCommand.Path) -ChildPath 'migration-report.ps1'
. $reportHelperPath
$script:AzCopyExecutablePath = $null
$script:TransferReportContext = $null
$script:CurrentAzCopyAuthMode = $null
$script:CurrentAzCopyTenantId = $null

if (-not $RunId) {
    $RunId = [guid]::NewGuid().ToString()
}

function ConvertTo-NormalizedStringArray {
    param(
        [string[]]$Values,
        [string]$DelimitedValue,
        [string]$JsonValue
    )

    if ($DelimitedValue) {
        return @($DelimitedValue -split '\|' | ForEach-Object { [string]$_ } | Where-Object { $_ })
    }

    if ($JsonValue) {
        try {
            $parsed = $JsonValue | ConvertFrom-Json -ErrorAction Stop
            return @($parsed | ForEach-Object { [string]$_ } | Where-Object { $_ })
        } catch {
            throw "Failed to parse serialized string array. $($_.Exception.Message)"
        }
    }

    if (-not $Values) {
        return @()
    }

    return @($Values | ForEach-Object { [string]$_ } | Where-Object { $_ })
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

function Select-DatabaseDirectories {
    param(
        [object[]]$DatabaseDirectories,
        [string[]]$RequestedNames,
        [string]$CurrentInstanceName
    )

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return @($DatabaseDirectories)
    }

    $selectionRules = @(Get-DatabaseSelectionRules -RequestedNames $RequestedNames)
    $globalLookup = @($selectionRules | Where-Object { -not $_.InstanceName } | Select-Object -ExpandProperty DatabaseName -Unique)
    $instanceLookup = @($selectionRules | Where-Object { $_.InstanceName -eq $CurrentInstanceName } | Select-Object -ExpandProperty DatabaseName -Unique)
    $effectiveLookup = @($globalLookup + $instanceLookup | Select-Object -Unique)

    if ($effectiveLookup.Count -eq 0) {
        return @()
    }

    return @($DatabaseDirectories | Where-Object { $_.Name -in $effectiveLookup })
}

function Assert-RequestedDatabaseSelectionsResolved {
    param(
        [object[]]$InstanceRoots,
        [string[]]$RequestedNames
    )

    if (-not $RequestedNames -or $RequestedNames.Count -eq 0) {
        return
    }

    $selectionRules = @(Get-DatabaseSelectionRules -RequestedNames $RequestedNames)
    $databaseEntries = @()
    foreach ($instanceRoot in $InstanceRoots) {
        $instanceDatabaseDirs = if ($instanceRoot.DatabaseDirs) {
            @($instanceRoot.DatabaseDirs)
        } else {
            @(Get-ChildItem -LiteralPath $instanceRoot.Path -Directory -ErrorAction SilentlyContinue)
        }

        foreach ($dbDir in $instanceDatabaseDirs) {
            $databaseEntries += [pscustomobject]@{
                InstanceName = $instanceRoot.Name
                DatabaseName = $dbDir.Name
            }
        }
    }

    $missingSelections = @()
    foreach ($selectionRule in $selectionRules) {
        if ($selectionRule.InstanceName) {
            $match = $databaseEntries | Where-Object { $_.InstanceName -eq $selectionRule.InstanceName -and $_.DatabaseName -eq $selectionRule.DatabaseName } | Select-Object -First 1
        } else {
            $match = $databaseEntries | Where-Object { $_.DatabaseName -eq $selectionRule.DatabaseName } | Select-Object -First 1
        }

        if (-not $match) {
            $missingSelections += $selectionRule.RawName
        }
    }

    if ($missingSelections.Count -gt 0) {
        throw "Requested database folder(s) were not found for transfer: $($missingSelections -join ', ')"
    }
}

$InstanceNames = ConvertTo-NormalizedStringArray -Values $InstanceNames -DelimitedValue $InstanceNamesList -JsonValue $InstanceNamesJson
$DatabaseNames = ConvertTo-NormalizedStringArray -Values $DatabaseNames -DelimitedValue $DatabaseNamesList -JsonValue $DatabaseNamesJson

function ConvertTo-PlainHashtable {
    param([object]$InputObject)

    if ($null -eq $InputObject) {
        return $null
    }

    if ($InputObject -is [hashtable]) {
        return $InputObject
    }

    $table = @{}
    foreach ($property in $InputObject.PSObject.Properties) {
        $table[$property.Name] = $property.Value
    }

    return $table
}

function Read-TransferReportContext {
    param([string]$Path)

    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    try {
        $context = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
        return [pscustomobject]@{
            JsonPath = [string]$context.jsonPath
            HtmlPath = [string]$context.htmlPath
            EventLogPaths = @($context.eventLogPaths)
            Metadata = ConvertTo-PlainHashtable -InputObject $context.metadata
        }
    } catch {
        Write-Warning "Unable to read transfer report context '$Path'. $($_.Exception.Message)"
        return $null
    }
}

function Update-TransferReportArtifacts {
    if (-not $ReportContextPath) {
        return
    }

    if (-not $script:TransferReportContext) {
        $script:TransferReportContext = Read-TransferReportContext -Path $ReportContextPath
    }

    if (-not $script:TransferReportContext) {
        return
    }

    try {
        $events = Read-MigrationEventLog -Paths $script:TransferReportContext.EventLogPaths
        Export-MigrationArtifacts -JsonPath $script:TransferReportContext.JsonPath -HtmlPath $script:TransferReportContext.HtmlPath -Events $events -Metadata $script:TransferReportContext.Metadata
    } catch {
        Write-Warning "Unable to refresh migration report artifacts. $($_.Exception.Message)"
    }
}

function Write-TransferMigrationEvent {
    param(
        [string]$Level = 'Info',
        [string]$Phase,
        [string]$Action,
        [string]$Mode,
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$Message,
        [hashtable]$Data
    )

    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-backup-transfer.ps1' -Level $Level -Phase $Phase -Action $Action -Mode $Mode -InstanceName $InstanceName -DatabaseName $DatabaseName -Message $Message -Data $Data
    Update-TransferReportArtifacts
}

function Write-Log {
    param([string]$Message)

    if ($QuietConsole) {
        $noisePatterns = @(
            '^Authentication starting:',
            '^Using Microsoft Entra authentication',
            '^Using SAS authentication',
            '^Step \d+:',
            '^Connecting to storage account',
            '^Create storage container:',
            '^Container already exists:',
            '^Offline copy from ',
            '^Initial copy from ',
            '^Transferring file ',
            '^Copy file ',
            '^Transfer logs for database .*: copying ',
            '^Transfer initial logs for database .*: copying ',
            '^Transfer polled logs for database .*: copying '
        )

        foreach ($pattern in $noisePatterns) {
            if ($Message -match $pattern) {
                return
            }
        }
    }

    Write-Host ("[{0}] {1}" -f (Get-Date -Format 'HH:mm:ss'), $Message)
}

function Write-AzCopyConsoleOutput {
    param([string]$OutputText)

    if (-not $OutputText) {
        return
    }

    if (-not $QuietConsole) {
        $OutputText | Out-Host
        return
    }
}

function Get-StorageEndpointHost {
    param([string]$TargetUri)

    return ([Uri]$TargetUri).Host
}

function Test-StorageEndpointReachable {
    param([string]$TargetUri)

    $hostName = Get-StorageEndpointHost -TargetUri $TargetUri

    try {
        $addresses = [System.Net.Dns]::GetHostAddresses($hostName)
        return ($addresses | Measure-Object).Count -gt 0
    } catch {
        return $false
    }
}

function Test-AzCopyRetryableFailure {
    param([string]$OutputText)

    if (-not $OutputText) {
        return $false
    }

    return $OutputText -match '(?i)no such host|temporary failure|name resolution|dial tcp|timeout|connection reset|eof|tls handshake timeout|connection refused|ContainerBeingDeleted|specified container is being deleted|RESPONSE\s+409'
}

function Test-AzCopyAuthenticationFailure {
    param([string]$OutputText)

    if (-not $OutputText) {
        return $false
    }

    return $OutputText -match '(?i)failed to perform auto-login|azureclicredential|please authenticate using microsoft entra id|azcopy login|login credentials|authentication failed|unauthorized|status 401|status 403'
}

function Wait-StorageEndpointReachable {
    param(
        [string]$TargetUri,
        [string]$OperationName
    )

    $attemptLimit = [Math]::Max(1, $ConnectionRetryCount)
    $hostName = Get-StorageEndpointHost -TargetUri $TargetUri

    for ($attempt = 1; $attempt -le $attemptLimit; $attempt++) {
        Write-Log "${OperationName}: checking connectivity to storage endpoint '$hostName' (attempt $attempt of $attemptLimit)."

        if (Test-StorageEndpointReachable -TargetUri $TargetUri) {
            Write-Log "${OperationName}: storage endpoint '$hostName' is reachable."
            return
        }

        if ($attempt -lt $attemptLimit) {
            Write-Log "${OperationName}: connectivity check failed for '$hostName'. Waiting $ConnectionRetryDelaySeconds second(s) before retrying."
            Start-Sleep -Seconds $ConnectionRetryDelaySeconds
        }
    }

    throw "${OperationName}: failed to reach storage endpoint '$hostName' after $attemptLimit attempt(s). Terminating transfer step."
}

function Invoke-AzCopyCommandWithRetry {
    param(
        [string]$OperationName,
        [string]$TargetUri,
        [string[]]$Arguments
    )

    $azCopyPath = Resolve-AzCopyExecutablePath
    $attemptLimit = [Math]::Max(1, $ConnectionRetryCount)

    for ($attempt = 1; $attempt -le $attemptLimit; $attempt++) {
        Wait-StorageEndpointReachable -TargetUri $TargetUri -OperationName $OperationName
        Write-Log "${OperationName}: starting AzCopy attempt $attempt of $attemptLimit."

        $output = & $azCopyPath @Arguments 2>&1
        $outputText = $output | Out-String
        Write-AzCopyConsoleOutput -OutputText $outputText

        if ($LASTEXITCODE -eq 0) {
            Write-Log "${OperationName}: completed successfully."
            return $outputText
        }

        if (($script:CurrentAzCopyAuthMode -eq 'EntraAzCli') -and (Test-AzCopyAuthenticationFailure -OutputText $outputText)) {
            if ($AssumeDeviceLoginReady) {
                throw "${OperationName} failed because Azure CLI-based AzCopy authentication was rejected and interactive fallback is disabled for this transfer process. Complete AzCopy device login in the foreground, or rerun with a non-background-friendly auth mode such as SAS. $outputText"
            }

            Write-Log "${OperationName}: Azure CLI-based AzCopy authentication failed. Falling back to AzCopy device login."
            Ensure-AzCopyAuthentication -AuthMode 'EntraDevice' -TenantIdValue $script:CurrentAzCopyTenantId -SasToken $null -UseExistingDeviceLogin:$AssumeDeviceLoginReady

            $output = & $azCopyPath @Arguments 2>&1
            $outputText = $output | Out-String
            Write-AzCopyConsoleOutput -OutputText $outputText

            if ($LASTEXITCODE -eq 0) {
                Write-Log "${OperationName}: completed successfully after AzCopy device-login fallback."
                return $outputText
            }
        }

        if ($outputText -match '(?i)already exists|container.*exists') {
            Write-Log "${OperationName}: target already exists. Continuing."
            return $outputText
        }

        if (($attempt -lt $attemptLimit) -and (Test-AzCopyRetryableFailure -OutputText $outputText)) {
            Write-Log "${OperationName}: attempt $attempt of $attemptLimit failed with a retryable connectivity or transient storage error. Waiting $ConnectionRetryDelaySeconds second(s) before retrying."
            Start-Sleep -Seconds $ConnectionRetryDelaySeconds
            continue
        }

        throw "$OperationName failed after $attempt attempt(s). $outputText"
    }
}

function Resolve-AzCopyExecutablePath {
    if ($script:AzCopyExecutablePath -and (Test-Path -LiteralPath $script:AzCopyExecutablePath)) {
        return $script:AzCopyExecutablePath
    }

    $azCopyCommand = Get-Command azcopy -ErrorAction SilentlyContinue
    if ($azCopyCommand -and $azCopyCommand.Source -and (Test-Path -LiteralPath $azCopyCommand.Source)) {
        $script:AzCopyExecutablePath = $azCopyCommand.Source
        return $script:AzCopyExecutablePath
    }

    $wingetPackageRoots = @(
        (Join-Path -Path $env:LOCALAPPDATA -ChildPath 'Microsoft\WinGet\Packages'),
        (Join-Path -Path $env:ProgramFiles -ChildPath 'WindowsApps')
    )

    foreach ($packageRoot in $wingetPackageRoots) {
        if (-not $packageRoot -or -not (Test-Path -LiteralPath $packageRoot)) {
            continue
        }

        $candidate = Get-ChildItem -Path $packageRoot -Filter 'azcopy.exe' -File -Recurse -ErrorAction SilentlyContinue |
            Sort-Object -Property FullName -Descending |
            Select-Object -First 1

        if ($candidate) {
            $script:AzCopyExecutablePath = $candidate.FullName
            return $script:AzCopyExecutablePath
        }
    }

    return $null
}

function Refresh-ProcessPath {
    $machinePath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
    $userPath = [System.Environment]::GetEnvironmentVariable('Path', 'User')
    $pathSegments = @()

    if ($machinePath) {
        $pathSegments += $machinePath
    }

    if ($userPath) {
        $pathSegments += $userPath
    }

    if ($pathSegments.Count -gt 0) {
        $env:Path = $pathSegments -join ';'
    }
}

function Install-AzCopy {
    if ($WhatIf) {
        Write-Log 'WhatIf: AzCopy is missing. Skipping automatic installation.'
        return $false
    }

    $wingetCommand = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $wingetCommand) {
        return $false
    }

    Write-Log 'AzCopy is not available in PATH. Attempting installation via winget.'
    & $wingetCommand.Source install --id Microsoft.Azure.AZCopy.10 --exact --accept-source-agreements --accept-package-agreements --disable-interactivity | Out-Host
    if ($LASTEXITCODE -ne 0) {
        return $false
    }

    Refresh-ProcessPath
    return [bool](Resolve-AzCopyExecutablePath)
}

function Ensure-AzCopy {
    if (-not (Resolve-AzCopyExecutablePath)) {
        $installed = Install-AzCopy
        if (-not $installed) {
            throw 'AzCopy is not available in PATH, and automatic installation via winget was not successful. Install AzCopy and retry.'
        }

        Write-Log "AzCopy is available at '$script:AzCopyExecutablePath'."
    }
}

function Ensure-AzureCli {
    if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
        throw 'Azure CLI is not available in PATH. Install Azure CLI and run az login, or use -StorageAuthMode EntraDevice.'
    }
}

function Get-TargetUri {
    param(
        [string]$BaseUri,
        [string]$AuthMode,
        [string]$SasToken
    )

    if ($BaseUri -match '\?') {
        throw 'StorageContainerUri must not include a SAS token.'
    }

    $normalizedBaseUri = $BaseUri.TrimEnd('/') + '/'

    if ($AuthMode -eq 'Sas') {
        if (-not $SasToken) {
            throw 'StorageContainerSasToken is required when StorageAuthMode is Sas.'
        }

        $cleanToken = $SasToken.TrimStart('?')
        return "$normalizedBaseUri`?$cleanToken"
    }

    return $normalizedBaseUri
}

function Resolve-StorageUri {
    param(
        [string]$BaseUri,
        [string]$Template,
        [string]$DbName
    )

    if ($Template) {
        $resolved = $Template -replace '\{db\}', $DbName
        return ($resolved -replace '\{database\}', $DbName)
    }

    return $BaseUri
}

function Convert-ToContainerName {
    param([string]$Value)

    if (-not $Value) {
        throw 'Instance name is required to derive a storage container name.'
    }

    $normalized = $Value.ToLowerInvariant()
    $normalized = $normalized -replace '[^a-z0-9-]', '-'
    $normalized = $normalized -replace '-+', '-'
    $normalized = $normalized.Trim('-')

    if (-not $normalized) {
        $normalized = 'sql-instance'
    }

    if ($normalized.Length -lt 3) {
        $normalized = ($normalized + '-mi').Substring(0, 3)
    }

    if ($normalized.Length -gt 63) {
        $normalized = $normalized.Substring(0, 63).Trim('-')
    }

    if ($normalized -match '(?i)backup') {
        throw "Derived container name '$normalized' contains 'backup', which LRS does not allow. Rename the source instance folder or provide explicit container URIs."
    }

    return $normalized
}

function Test-IsInstanceRoot {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    $childDirs = Get-ChildItem -LiteralPath $Path -Directory -ErrorAction SilentlyContinue
    foreach ($childDir in $childDirs) {
        $candidateNames = @('FULL', 'Full', 'full', 'LOG', 'Log', 'log', 'DIFF', 'Diff', 'diff', 'TRAN', 'Tran', 'tran')
        foreach ($candidateName in $candidateNames) {
            if (Test-Path -LiteralPath (Join-Path -Path $childDir.FullName -ChildPath $candidateName)) {
                return $true
            }
        }
    }

    return $false
}

function Get-ContainerUri {
    param(
        [string]$AccountName,
        [string]$ContainerName
    )

    return "https://$AccountName.dfs.core.windows.net/$ContainerName"
}

function Ensure-StorageContainer {
    param([string]$ContainerUri)

    if ($WhatIf) {
        Write-Log "WhatIf: azcopy make '$ContainerUri'"
        return
    }

    Write-Log "Connecting to storage account container endpoint '$ContainerUri'."
    $outputText = Invoke-AzCopyCommandWithRetry -OperationName 'Create storage container' -TargetUri $ContainerUri -Arguments @('make', $ContainerUri)
    if ($outputText -match '(?i)already exists|container.*exists') {
        Write-Log "Container already exists: $ContainerUri"
        return
    }

    if ($outputText -match '(?i)AuthorizationFailure|not authorized to perform this operation|status\s*403') {
        throw @"
Failed to create container '$ContainerUri'.

Azure Blob data-plane authorization failed for the current Microsoft Entra identity.
This workflow uses AzCopy with Entra authentication, so public blob access does not apply.

Required fix:
- Assign 'Storage Blob Data Contributor' or 'Storage Blob Data Owner' to the signed-in user or service principal on the storage account or a parent scope.

Current storage auth assumptions:
- Microsoft Entra auth is enabled for AzCopy.
- Shared key auth is not being used.

Original AzCopy output:
$outputText
"@
    }

    Write-Log "Container is ready: $ContainerUri"
    return

    throw "Failed to create container '$ContainerUri'. $outputText"
}

function New-TransferWorkItem {
    param(
        [string]$InstanceName,
        [string]$DbName,
        [string]$SourceRoot,
        [string]$StorageBase,
        [string]$ContainerUri
    )

    $stateKey = if ($InstanceName) { "$InstanceName::$DbName" } else { $DbName }

    return [pscustomobject]@{
        InstanceName = $InstanceName
        DbName       = $DbName
        SourceRoot   = $SourceRoot
        StorageBase  = $StorageBase
        ContainerUri = $ContainerUri
        StateKey     = $stateKey
    }
}

function Get-TransferWorkItems {
    param(
        [string]$SingleSourcePath,
        [string]$BasePath,
        [string[]]$InstanceNames,
        [string[]]$DbNames,
        [string]$AccountName,
        [string]$ExplicitUri,
        [string]$TemplateUri
    )

    $workItems = @()
    $useInstanceContainers = $AccountName -and -not $ExplicitUri -and -not $TemplateUri

    if (-not $useInstanceContainers) {
        $dbList = if ($DbNames -and $DbNames.Count -gt 0) { $DbNames } else { @('single') }
        foreach ($dbName in $dbList) {
            $sourceRoot = if ($dbName -eq 'single') { $SingleSourcePath } else { Join-Path -Path $BasePath -ChildPath $dbName }
            $storageBase = if ($dbName -eq 'single') { $ExplicitUri } else { Resolve-StorageUri -BaseUri $ExplicitUri -Template $TemplateUri -DbName $dbName }
            $workItems += New-TransferWorkItem -InstanceName $null -DbName $dbName -SourceRoot $sourceRoot -StorageBase $storageBase -ContainerUri $null
        }

        return $workItems
    }

    $instanceRoots = @()

    if ($SingleSourcePath) {
        $dbName = Split-Path -Leaf $SingleSourcePath
        $instancePath = Split-Path -Parent $SingleSourcePath
        $instanceRoots += [pscustomobject]@{ Name = (Split-Path -Leaf $instancePath); Path = $instancePath; DatabaseDirs = @([pscustomobject]@{ Name = $dbName; FullName = $SingleSourcePath }) }
    } elseif (Test-IsInstanceRoot -Path $BasePath) {
        $instanceRoots += [pscustomobject]@{ Name = (Split-Path -Leaf $BasePath); Path = $BasePath; DatabaseDirs = $null }
    } else {
        $childInstanceDirs = Get-ChildItem -LiteralPath $BasePath -Directory -ErrorAction SilentlyContinue
        foreach ($childInstanceDir in $childInstanceDirs) {
            if (Test-IsInstanceRoot -Path $childInstanceDir.FullName) {
                $instanceRoots += [pscustomobject]@{ Name = $childInstanceDir.Name; Path = $childInstanceDir.FullName; DatabaseDirs = $null }
            }
        }
    }

    if ($instanceRoots.Count -eq 0) {
        throw 'No SQL instance folders were found for instance-aware container creation.'
    }

    if ($InstanceNames -and $InstanceNames.Count -gt 0) {
        $requestedInstanceNames = $InstanceNames | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        $instanceRoots = $instanceRoots | Where-Object { $_.Name -in $requestedInstanceNames }

        $resolvedInstanceNames = $instanceRoots | ForEach-Object { $_.Name }
        $missingInstanceNames = $requestedInstanceNames | Where-Object { $_ -notin $resolvedInstanceNames }
        if ($missingInstanceNames) {
            throw "Requested instance folder(s) were not found for transfer: $($missingInstanceNames -join ', ')"
        }
    }

    Assert-RequestedDatabaseSelectionsResolved -InstanceRoots $instanceRoots -RequestedNames $DbNames

    foreach ($instanceRoot in $instanceRoots) {
        $dbDirs = if ($instanceRoot.DatabaseDirs) {
            $instanceRoot.DatabaseDirs
        } else {
            Get-ChildItem -LiteralPath $instanceRoot.Path -Directory -ErrorAction SilentlyContinue
        }

        $dbDirs = @(Select-DatabaseDirectories -DatabaseDirectories $dbDirs -RequestedNames $DbNames -CurrentInstanceName $instanceRoot.Name)

        $containerName = Convert-ToContainerName -Value $instanceRoot.Name
        $containerUri = Get-ContainerUri -AccountName $AccountName -ContainerName $containerName

        foreach ($dbDir in $dbDirs) {
            $storageBase = "$containerUri/$($dbDir.Name)"
            $workItems += New-TransferWorkItem -InstanceName $instanceRoot.Name -DbName $dbDir.Name -SourceRoot $dbDir.FullName -StorageBase $storageBase -ContainerUri $containerUri
        }
    }

    if ($workItems.Count -eq 0) {
        throw 'No database folders were found for transfer.'
    }

    return $workItems
}

function Validate-StorageUriRules {
    param(
        [string]$Value,
        [string]$Label
    )

    if (-not $Value) { return }

    if ($Value -match '(?i)backup') {
        throw "$Label contains 'backup'. LRS does not allow 'backup' in container or folder names."
    }

    if ($Value -match '(?i)(/|\\)full(/|$)') {
        throw "$Label contains a nested 'full' folder. LRS requires a flat folder per database."
    }

    if ($Value -match '(?i)(/|\\)tran(/|$)') {
        throw "$Label contains a nested 'tran' folder. LRS requires a flat folder per database."
    }

    if ($Value -match '\?') {
        throw "$Label must not include a SAS token or question mark."
    }
}

function Load-State {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{ LastSyncUtc = @{} }
    }

    $rawState = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    $state = [pscustomobject]@{}

    foreach ($property in $rawState.PSObject.Properties) {
        if ($property.Name -ne 'LastSyncUtc') {
            $state | Add-Member -NotePropertyName $property.Name -NotePropertyValue $property.Value
        }
    }

    $lastSyncUtc = @{}
    if ($rawState.PSObject.Properties.Match('LastSyncUtc').Count -gt 0 -and $rawState.LastSyncUtc) {
        if ($rawState.LastSyncUtc -is [System.Collections.IDictionary]) {
            foreach ($key in $rawState.LastSyncUtc.Keys) {
                $lastSyncUtc[$key] = [string]$rawState.LastSyncUtc[$key]
            }
        } else {
            foreach ($property in $rawState.LastSyncUtc.PSObject.Properties) {
                $lastSyncUtc[$property.Name] = [string]$property.Value
            }
        }
    }

    $state | Add-Member -NotePropertyName 'LastSyncUtc' -NotePropertyValue $lastSyncUtc -Force
    return $state
}

function Save-State {
    param(
        [string]$Path,
        [object]$State
    )

    $State | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $Path
}

function Get-FileNameChunks {
    param(
        [string[]]$FileNames,
        [int]$ChunkSize = 64
    )

    $chunks = @()
    $currentChunk = @()

    foreach ($fileName in $FileNames) {
        $currentChunk += $fileName
        if ($currentChunk.Count -ge $ChunkSize) {
            $chunks += ,@($currentChunk)
            $currentChunk = @()
        }
    }

    if ($currentChunk.Count -gt 0) {
        $chunks += ,@($currentChunk)
    }

    return $chunks
}

function Invoke-AzCopyFiles {
    param(
        [object[]]$Files,
        [string]$TargetUri,
        [string]$Description
    )

    if (-not $Files -or $Files.Count -eq 0) {
        return
    }

    $orderedFiles = @($Files | Sort-Object -Property DirectoryName, Name)
    foreach ($file in $orderedFiles) {
        $fileDescription = if ($Description) { $Description } else { 'Transfer files' }
        Write-Log "${fileDescription}: copying '$($file.FullName)' to '$TargetUri'."
        Invoke-AzCopyFile -SourceFile $file.FullName -TargetUri $TargetUri
    }
}

function Invoke-AzCopyFile {
    param(
        [string]$SourceFile,
        [string]$TargetUri
    )

    if (Test-Path -LiteralPath $SourceFile -PathType Container) {
        throw "Source path '$SourceFile' is a directory. Pass individual backup files so uploads remain flat under the database folder."
    }

    if (-not (Test-Path -LiteralPath $SourceFile -PathType Leaf)) {
        throw "Source file '$SourceFile' does not exist."
    }

    if ($WhatIf) {
        Write-Log "WhatIf: azcopy copy '$SourceFile' '$TargetUri'"
        return
    }

    $fileName = Split-Path -Leaf $SourceFile
    Write-Log "Transferring file '$fileName' to '$TargetUri'."
    Invoke-AzCopyCommandWithRetry -OperationName "Copy file '$fileName'" -TargetUri $TargetUri -Arguments @('copy', $SourceFile, $TargetUri, '--overwrite=ifSourceNewer') | Out-Null
}

function Ensure-AzCopyAuthentication {
    param(
        [string]$AuthMode,
        [string]$TenantIdValue,
        [string]$SasToken,
        [switch]$UseExistingDeviceLogin
    )

    switch ($AuthMode) {
        'Sas' {
            if (-not $SasToken) {
                throw 'StorageContainerSasToken is required when StorageAuthMode is Sas.'
            }

            $script:CurrentAzCopyAuthMode = 'Sas'
            $script:CurrentAzCopyTenantId = $TenantIdValue

            Write-Log 'Authentication starting: using SAS authentication for AzCopy uploads.'
            Write-Log 'Using SAS authentication for AzCopy uploads.'
            return
        }

        'EntraAzCli' {
            $script:CurrentAzCopyAuthMode = 'EntraAzCli'
            $script:CurrentAzCopyTenantId = $TenantIdValue

            Write-Log 'Authentication starting: validating Azure CLI session for AzCopy.'
            Ensure-AzureCli
            & az account show --output none 2>$null
            if ($LASTEXITCODE -ne 0) {
                throw 'Azure CLI is not signed in. Run az login first, or use -StorageAuthMode EntraDevice.'
            }

            $Env:AZCOPY_AUTO_LOGIN_TYPE = 'AZCLI'
            if ($TenantIdValue) {
                $Env:AZCOPY_TENANT_ID = $TenantIdValue
            } else {
                Remove-Item Env:AZCOPY_TENANT_ID -ErrorAction SilentlyContinue
            }

            Write-Log 'Using Microsoft Entra authentication for AzCopy via the current Azure CLI session.'
            return
        }

        'EntraDevice' {
            $script:CurrentAzCopyAuthMode = 'EntraDevice'
            $script:CurrentAzCopyTenantId = $TenantIdValue

            $azCopyPath = Resolve-AzCopyExecutablePath
            if ($UseExistingDeviceLogin) {
                Write-Log 'Authentication starting: using existing AzCopy device-login session for uploads.'
            } else {
                Write-Log 'Authentication starting: invoking AzCopy device login.'
                $loginArgs = @('login')
                if ($TenantIdValue) {
                    $loginArgs += "--tenant-id=$TenantIdValue"
                }

                & $azCopyPath @loginArgs | Out-Host
                if ($LASTEXITCODE -ne 0) {
                    throw 'AzCopy device login failed.'
                }
            }

            Remove-Item Env:AZCOPY_AUTO_LOGIN_TYPE -ErrorAction SilentlyContinue
            if ($TenantIdValue) {
                $Env:AZCOPY_TENANT_ID = $TenantIdValue
            } else {
                Remove-Item Env:AZCOPY_TENANT_ID -ErrorAction SilentlyContinue
            }

            Write-Log 'Using Microsoft Entra authentication for AzCopy via device login.'
            return
        }
    }

    throw "Unsupported StorageAuthMode '$AuthMode'."
}

function Get-BackupFiles {
    param(
        [string]$Root,
        [string[]]$Extensions,
        [AllowNull()]
        [Nullable[DateTime]]$Since,
        [string]$StartFileName
    )

    $files = Get-ChildItem -LiteralPath $Root -File -Recurse | Where-Object {
        $Extensions -contains $_.Extension.ToLowerInvariant()
    }
        $files = @($files)

    if ($Since) {
            $files = @($files | Where-Object { $_.LastWriteTime -gt $Since })
    }

        $ordered = @($files | Sort-Object -Property Name)

    if ($StartFileName) {
        $startIndex = -1
        for ($i = 0; $i -lt $ordered.Count; $i++) {
            if ($ordered[$i].Name -ieq $StartFileName) {
                $startIndex = $i
                break
            }
        }

        if ($startIndex -lt 0) {
            throw "Start file '$StartFileName' not found in $Root."
        }

            return ,@($ordered[$startIndex..($ordered.Count - 1)])
    }

        return ,@($ordered)
}

function Get-SelectedBackupFile {
    param(
        [string]$Root,
        [string[]]$Extensions,
        [string]$ExplicitFileName,
        [ValidateSet('Earliest', 'Latest')]
        [string]$Selection,
        [string]$Label
    )

    if ($ExplicitFileName) {
        $explicitPath = Join-Path -Path $Root -ChildPath $ExplicitFileName
        if (-not (Test-Path -LiteralPath $explicitPath)) {
            throw "$Label file '$explicitPath' not found."
        }

        return Get-Item -LiteralPath $explicitPath
    }

    $files = Get-BackupFiles -Root $Root -Extensions $Extensions -Since $null -StartFileName $null
    if (-not $files -or $files.Count -eq 0) {
        throw "No $Label files were found in $Root."
    }

    if ($Selection -eq 'Earliest') {
        return $files[0]
    }

    return $files[$files.Count - 1]
}

function Resolve-InitialTranStartFile {
    param(
        [string]$TranPath,
        [string]$ExplicitStartFileName,
        [datetime]$AnchorTime
    )

    if ($ExplicitStartFileName) {
        return $ExplicitStartFileName
    }

    $since = $null
    if ($AnchorTime) {
        $since = $AnchorTime.AddSeconds(-1)
    }

    $candidateLogs = Get-BackupFiles -Root $TranPath -Extensions @('.trn') -Since $since -StartFileName $null
    if ($candidateLogs -and $candidateLogs.Count -gt 0) {
        return $candidateLogs[0].Name
    }

    return $null
}

function Get-ApplicableDiffBackupFile {
    param(
        [string]$DiffPath,
        [datetime]$FullBackupTime
    )

    if (-not $DiffPath -or -not (Test-Path -LiteralPath $DiffPath)) {
        return $null
    }

    $diffFiles = Get-BackupFiles -Root $DiffPath -Extensions @('.diff', '.dif', '.bak') -Since $null -StartFileName $null
    if (-not $diffFiles -or $diffFiles.Count -eq 0) {
        return $null
    }

    $applicableDiffs = $diffFiles
    if ($FullBackupTime) {
        $applicableDiffs = @($diffFiles | Where-Object { $_.LastWriteTime -ge $FullBackupTime })
    }

    if (-not $applicableDiffs -or $applicableDiffs.Count -eq 0) {
        return $null
    }

    return ($applicableDiffs | Sort-Object -Property Name | Select-Object -Last 1)
}

function Get-BackupStripePattern {
    param([string]$BaseName)

    if (-not $BaseName) { return $null }

    # Patterns are evaluated top-to-bottom; the first match wins.
    # Stronger / more specific patterns (NofM, explicit stripe/part/file keywords,
    # and timestamp-anchored numerics) come first. The loose 'TrailingNumeric'
    # pattern is intentionally last and is only honoured later when sibling
    # confirmation is found in Get-BackupStripeSetGroups.
    $patterns = @(
        @{ Name = 'NofM';               Regex = '^(?<stem>.+?)[._-](?<idx>\d{1,3})of(?<tot>\d{1,3})$' },
        @{ Name = 'StripeN';            Regex = '^(?<stem>.+?)[._-]stripe(?<idx>\d{1,3})$' },
        @{ Name = 'PartN';              Regex = '^(?<stem>.+?)[._-]part(?<idx>\d{1,3})$' },
        @{ Name = 'FileN';              Regex = '^(?<stem>.+?)[._-]file(?<idx>\d{1,3})$' },
        # Timestamped: requires an 8-digit date (YYYYMMDD) and optional 4-6 digit
        # time component anywhere in the stem before the trailing stripe index.
        # Examples that match:
        #   AG2_20260422_120000_1.bak  -> stem='AG2_20260422_120000', idx=1
        #   AG2-20260422-1.bak         -> stem='AG2-20260422',        idx=1
        #   AG2_full_20260422T1200_03.bak (T-form) handled by allowing letters in stem
        @{ Name = 'TimestampedStripe'; Regex = '^(?<stem>.+?[._-]\d{8}(?:[._T-]\d{4,6})?)[._-](?<idx>\d{1,3})$' },
        @{ Name = 'TrailingNumeric';    Regex = '^(?<stem>.+?)[._-](?<idx>\d{1,3})$' }
    )

    foreach ($pattern in $patterns) {
        if ($BaseName -imatch $pattern.Regex) {
            $totalValue = $null
            if ($Matches.ContainsKey('tot')) { $totalValue = [int]$Matches['tot'] }
            return [pscustomobject]@{
                Stem    = [string]$Matches['stem']
                Index   = [int]$Matches['idx']
                Total   = $totalValue
                Pattern = $pattern.Name
            }
        }
    }

    return $null
}

function Get-BackupStripeSetGroups {
    param(
        [object[]]$Files,
        [int]$WindowMinutes = 120
    )

    if (-not $Files -or $Files.Count -eq 0) { return @() }

    $tagged = foreach ($file in $Files) {
        $base = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $stripeInfo = Get-BackupStripePattern -BaseName $base
        # Loose 'TrailingNumeric' matches must only count as stripes when there
        # are sibling files sharing the same stem; otherwise treat as a
        # single-file backup so that names like 'MyDb_5.bak' (a one-off backup
        # with a numeric suffix) are not mis-grouped or wrongly assumed to be
        # part of an incomplete stripe set.
        $stem = if ($stripeInfo) { $stripeInfo.Stem } else { $base }
        $stemKey = ($stem.ToLowerInvariant() + '|' + $file.Extension.ToLowerInvariant())
        [pscustomobject]@{
            File       = $file
            StemKey    = $stemKey
            StripeInfo = $stripeInfo
            BaseName   = $base
        }
    }

    $groups = @()
    foreach ($groupEntry in ($tagged | Group-Object -Property StemKey)) {
        $items = @($groupEntry.Group)

        # Defensive: if a group of size 1 was matched only by the loose
        # TrailingNumeric pattern, re-key it under its full base name so we
        # do not accidentally combine it with future siblings later.
        if ($items.Count -eq 1 -and $items[0].StripeInfo -and $items[0].StripeInfo.Pattern -eq 'TrailingNumeric') {
            $entry = $items[0]
            $groups += [pscustomobject]@{
                Stem       = ($entry.BaseName.ToLowerInvariant() + '|' + $entry.File.Extension.ToLowerInvariant())
                Files      = @($entry.File)
                LatestTime = $entry.File.LastWriteTime
            }
            continue
        }

        $times = @($items | ForEach-Object { $_.File.LastWriteTime })
        $minTime = ($times | Measure-Object -Minimum).Minimum
        $maxTime = ($times | Measure-Object -Maximum).Maximum

        if ($items.Count -gt 1 -and (($maxTime - $minTime).TotalMinutes -gt $WindowMinutes)) {
            foreach ($entry in $items) {
                $groups += [pscustomobject]@{
                    Stem       = ($entry.BaseName.ToLowerInvariant() + '|' + $entry.File.Extension.ToLowerInvariant())
                    Files      = @($entry.File)
                    LatestTime = $entry.File.LastWriteTime
                }
            }
            continue
        }

        $orderedFiles = @($items | Sort-Object -Property @{ Expression = { if ($_.StripeInfo) { $_.StripeInfo.Index } else { 0 } } }, @{ Expression = { $_.File.Name } } | ForEach-Object { $_.File })
        $groups += [pscustomobject]@{
            Stem       = $groupEntry.Name
            Files      = @($orderedFiles)
            LatestTime = $maxTime
        }
    }

    return ,@($groups)
}

function Get-SelectedBackupFileSet {
    param(
        [string]$Root,
        [string[]]$Extensions,
        [string]$ExplicitFileName,
        [ValidateSet('Earliest', 'Latest')]
        [string]$Selection,
        [string]$Label
    )

    $allFiles = Get-BackupFiles -Root $Root -Extensions $Extensions -Since $null -StartFileName $null
    if (-not $allFiles -or $allFiles.Count -eq 0) {
        throw "No $Label files were found in $Root."
    }

    $groups = Get-BackupStripeSetGroups -Files $allFiles
    if (-not $groups -or $groups.Count -eq 0) {
        throw "No $Label files were found in $Root."
    }

    if ($ExplicitFileName) {
        $explicitPath = Join-Path -Path $Root -ChildPath $ExplicitFileName
        if (-not (Test-Path -LiteralPath $explicitPath)) {
            throw "$Label file '$explicitPath' not found."
        }

        $explicitItem = Get-Item -LiteralPath $explicitPath
        $matchedGroup = $groups | Where-Object { @($_.Files | Where-Object { $_.FullName -ieq $explicitItem.FullName }).Count -gt 0 } | Select-Object -First 1
        if ($matchedGroup) {
            return $matchedGroup.Files
        }

        return $explicitItem
    }

    $sortedGroups = $groups | Sort-Object -Property LatestTime, Stem
    $picked = if ($Selection -eq 'Earliest') { $sortedGroups | Select-Object -First 1 } else { $sortedGroups | Select-Object -Last 1 }
    return $picked.Files
}

function Get-ApplicableDiffBackupFileSet {
    param(
        [string]$DiffPath,
        [datetime]$FullBackupTime
    )

    if (-not $DiffPath -or -not (Test-Path -LiteralPath $DiffPath)) {
        return @()
    }

    $diffFiles = Get-BackupFiles -Root $DiffPath -Extensions @('.diff', '.dif', '.bak') -Since $null -StartFileName $null
    if (-not $diffFiles -or $diffFiles.Count -eq 0) {
        return @()
    }

    $groups = Get-BackupStripeSetGroups -Files $diffFiles
    if (-not $groups -or $groups.Count -eq 0) {
        return @()
    }

    if ($FullBackupTime) {
        $groups = @($groups | Where-Object { $_.LatestTime -ge $FullBackupTime })
    }

    if (-not $groups -or $groups.Count -eq 0) {
        return @()
    }

    $picked = $groups | Sort-Object -Property LatestTime, Stem | Select-Object -Last 1
    return $picked.Files
}

function Resolve-PathTemplate {
    param(
        [string]$Value,
        [string]$DbName
    )

    if (-not $Value) { return $null }
    return ($Value -replace '\{db\}', $DbName -replace '\{database\}', $DbName)
}

function Resolve-DefaultSourcePathBase {
    param(
        [string]$ConfiguredBase,
        [string]$SingleSourcePath
    )

    if ($ConfiguredBase -or $SingleSourcePath) {
        return $ConfiguredBase
    }

    if (-not (Test-Path -LiteralPath $DefaultSqlBackupsRoot)) {
        return $null
    }

    $childDirs = Get-ChildItem -LiteralPath $DefaultSqlBackupsRoot -Directory -ErrorAction SilentlyContinue
    if ($childDirs.Count -eq 1) {
        return $childDirs[0].FullName
    }

    return $DefaultSqlBackupsRoot
}

function Resolve-DefaultChildPath {
    param(
        [string]$ExplicitPath,
        [string]$SourceRoot,
        [string[]]$CandidateNames
    )

    if ($ExplicitPath) {
        return $ExplicitPath
    }

    if (-not $SourceRoot) {
        return $null
    }

    foreach ($candidateName in $CandidateNames) {
        $candidatePath = Join-Path -Path $SourceRoot -ChildPath $candidateName
        if (Test-Path -LiteralPath $candidatePath) {
            return $candidatePath
        }
    }

    return $null
}

function Ensure-PathExists {
    param(
        [string]$PathValue,
        [string]$Label
    )

    if (-not $PathValue) { return }
    if (-not (Test-Path -LiteralPath $PathValue)) {
        throw "$Label path '$PathValue' does not exist."
    }
}

if (-not $Mode) {
    $Mode = Read-Host -Prompt 'Mode (Offline|Online)'
}

if ($Mode -notin @('Offline', 'Online')) {
    throw "Invalid mode '$Mode'. Use Offline or Online."
}

if (-not $SourcePathBase) {
    $SourcePathBase = Resolve-DefaultSourcePathBase -ConfiguredBase $SourcePathBase -SingleSourcePath $SourcePath
}

if (-not $SourcePath -and -not $SourcePathBase) {
    $SourcePath = Read-Host -Prompt 'Source backup folder path (single database)'
}

if (-not $SourcePathBase) {
    $SourcePathBase = Read-Host -Prompt "Source base path (optional, for multiple databases; default root is $DefaultSqlBackupsRoot)"
}

if (-not $DatabaseNames) {
    $dbInput = Read-Host -Prompt 'Database names (comma-separated, optional)'
    if ($dbInput) {
        $DatabaseNames = $dbInput -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    }
}

if (-not $StorageAccountName) {
    $StorageAccountName = Read-Host -Prompt 'Storage account name (optional when using explicit storage container URI/template)'
}

$preferInstanceContainers = $StorageAccountName -and -not $StorageContainerUri -and -not $StorageContainerUriTemplate

if (-not $preferInstanceContainers -and -not $StorageContainerUri) {
    $StorageContainerUri = Read-Host -Prompt 'Storage container URI (no SAS, single database folder)'
}

if (-not $preferInstanceContainers -and -not $StorageContainerUriTemplate) {
    $StorageContainerUriTemplate = Read-Host -Prompt 'Storage container URI template (optional, use {db})'
}

if (-not $StorageAuthMode) {
    $StorageAuthMode = Read-Host -Prompt 'Storage auth mode for AzCopy (EntraAzCli|EntraDevice|Sas)'
}

if ($StorageAuthMode -eq 'Sas' -and -not $StorageContainerSasToken) {
    $StorageContainerSasToken = Read-Host -Prompt 'SAS token for AzCopy uploads (no leading ?)'
}

if (($StorageAuthMode -eq 'EntraAzCli' -or $StorageAuthMode -eq 'EntraDevice') -and -not $TenantId) {
    $TenantId = Read-Host -Prompt 'Tenant ID for Microsoft Entra auth (optional)'
}

try {
Ensure-AzCopy
Ensure-AzCopyAuthentication -AuthMode $StorageAuthMode -TenantIdValue $TenantId -SasToken $StorageContainerSasToken -UseExistingDeviceLogin:$AssumeDeviceLoginReady

Write-TransferMigrationEvent -Mode $Mode -Level 'Info' -Phase 'Transfer' -Action 'Start' -Message 'Backup transfer started.' -Data @{
    sourcePath = $SourcePath
    sourcePathBase = $SourcePathBase
    instanceNames = $InstanceNames
    databaseNames = $DatabaseNames
    statePath = $StatePath
    storageAccountName = $StorageAccountName
}

Write-Log 'Step 1: authentication starting.'

$isNamedDatabaseMode = $DatabaseNames -and $DatabaseNames.Count -gt 0
$isMultiDatabaseMode = $isNamedDatabaseMode -and $DatabaseNames.Count -gt 1
$useInstanceContainers = $StorageAccountName -and -not $StorageContainerUri -and -not $StorageContainerUriTemplate

if ($useInstanceContainers -and $StorageAuthMode -eq 'Sas') {
    throw 'Instance-aware container creation requires Entra-based auth. Use EntraAzCli or EntraDevice.'
}

if ($useInstanceContainers) {
    if (-not $SourcePathBase -and -not $SourcePath) {
        throw 'SourcePathBase or SourcePath is required when using instance-aware container creation.'
    }
} elseif ($isNamedDatabaseMode) {
    if (-not $SourcePathBase) {
        throw 'SourcePathBase is required when DatabaseNames is used.'
    }

    if ($isMultiDatabaseMode -and -not $StorageContainerUriTemplate) {
        throw 'StorageContainerUriTemplate is required for multiple databases.'
    }
} else {
    if (-not $SourcePath) {
        throw 'SourcePath is required for a single database.'
    }

    if (-not $StorageContainerUri) {
        throw 'StorageContainerUri is required for a single database.'
    }
}

if ($SourcePath -and -not (Test-Path -LiteralPath $SourcePath)) {
    throw "Source path '$SourcePath' does not exist."
}

if ($SourcePathBase -and -not (Test-Path -LiteralPath $SourcePathBase)) {
    throw "Source base path '$SourcePathBase' does not exist."
}

Validate-StorageUriRules -Value $StorageContainerUri -Label 'StorageContainerUri'
Validate-StorageUriRules -Value $StorageContainerUriTemplate -Label 'StorageContainerUriTemplate'

$workItems = Get-TransferWorkItems -SingleSourcePath $SourcePath -BasePath $SourcePathBase -InstanceNames $InstanceNames -DbNames $DatabaseNames -AccountName $StorageAccountName -ExplicitUri $StorageContainerUri -TemplateUri $StorageContainerUriTemplate

$ensuredContainers = @{}
foreach ($workItem in $workItems) {
    if ($workItem.ContainerUri -and -not $ensuredContainers.ContainsKey($workItem.ContainerUri)) {
        Write-Log "Step 2: connecting to storage account and ensuring container '$($workItem.ContainerUri)'."
        Ensure-StorageContainer -ContainerUri $workItem.ContainerUri
        $ensuredContainers[$workItem.ContainerUri] = $true
    }
}

$state = Load-State -Path $StatePath
if (-not $state.LastSyncUtc) {
    $state.LastSyncUtc = @{}
}

$extensions = @('.bak', '.trn')
if ($IncludeDiffs) {
    $extensions += '.diff'
}

Write-Log "Step 3: copy phase starting for $($workItems.Count) database(s)."
Write-Log "Copying backups for $($workItems.Count) database(s)"
Write-TransferMigrationEvent -Mode $Mode -Level 'Info' -Phase 'Transfer' -Action 'WorkItemsPrepared' -Message "Prepared $($workItems.Count) transfer work item(s)." -Data @{ workItemCount = $workItems.Count }

if ($Mode -eq 'Offline') {
    foreach ($workItem in $workItems) {
        $dbName = $workItem.DbName
        $stateKey = $workItem.StateKey
        $sourceRoot = $workItem.SourceRoot
        $storageBase = $workItem.StorageBase
        $targetUri = Get-TargetUri -BaseUri $storageBase -AuthMode $StorageAuthMode -SasToken $StorageContainerSasToken

        $resolvedFullPath = Resolve-PathTemplate -Value $FullBackupPath -DbName $dbName
        $resolvedFullFile = Resolve-PathTemplate -Value $FullBackupFile -DbName $dbName
        $resolvedDiffPath = Resolve-PathTemplate -Value $DiffPath -DbName $dbName
        $resolvedTranPath = Resolve-PathTemplate -Value $TranPath -DbName $dbName
        $resolvedTranStart = Resolve-PathTemplate -Value $TranStartFile -DbName $dbName

        if (-not (Test-Path -LiteralPath $sourceRoot)) {
            throw "Source path '$sourceRoot' does not exist."
        }

        $resolvedFullPath = Resolve-DefaultChildPath -ExplicitPath $resolvedFullPath -SourceRoot $sourceRoot -CandidateNames @('FULL', 'Full', 'full')
        $resolvedDiffPath = Resolve-DefaultChildPath -ExplicitPath $resolvedDiffPath -SourceRoot $sourceRoot -CandidateNames @('DIFF', 'Diff', 'diff')
        $resolvedTranPath = Resolve-DefaultChildPath -ExplicitPath $resolvedTranPath -SourceRoot $sourceRoot -CandidateNames @('LOG', 'Log', 'log', 'TRAN', 'Tran', 'tran', 'TLOG', 'tlog', 'Logs', 'logs')

        if (-not $resolvedFullPath) { $resolvedFullPath = $sourceRoot }
        if (-not $resolvedTranPath) { $resolvedTranPath = $sourceRoot }

        if ($IncludeDiffs -and -not $resolvedDiffPath) {
            Write-Warning "IncludeDiffs was specified, but no DIFF folder was found under '$sourceRoot'."
        }

        Ensure-PathExists -PathValue $resolvedFullPath -Label "Full backup"
        Ensure-PathExists -PathValue $resolvedDiffPath -Label "Diff"
        Ensure-PathExists -PathValue $resolvedTranPath -Label "Tran"

        Write-Log "Preparing offline transfer for database '$dbName'."
        Write-Log "Offline copy from $sourceRoot to $storageBase"

        $fullFileSet = @(Get-SelectedBackupFileSet -Root $resolvedFullPath -Extensions @('.bak') -ExplicitFileName $resolvedFullFile -Selection 'Latest' -Label 'Full backup')
        $fullFileInfo = $fullFileSet | Sort-Object -Property LastWriteTime | Select-Object -Last 1
        $logAnchorTime = $fullFileInfo.LastWriteTime
        $selectedDiffFiles = @()
        $selectedDiffFile = $null

        if ($fullFileSet.Count -gt 1) {
            Write-Log "Detected striped FULL backup for '$dbName' with $($fullFileSet.Count) stripe(s): $((($fullFileSet | ForEach-Object { $_.Name }) -join ', '))"
        }
        Invoke-AzCopyFiles -Files $fullFileSet -TargetUri $targetUri -Description "Transfer full backup for database '$dbName'"

        if ($IncludeDiffs -and $resolvedDiffPath) {
            $selectedDiffFiles = @(Get-ApplicableDiffBackupFileSet -DiffPath $resolvedDiffPath -FullBackupTime $fullFileInfo.LastWriteTime)
            if ($selectedDiffFiles.Count -gt 0) {
                $selectedDiffFile = $selectedDiffFiles | Sort-Object -Property LastWriteTime | Select-Object -Last 1
                if ($selectedDiffFiles.Count -gt 1) {
                    Write-Log "Detected striped DIFF backup for '$dbName' with $($selectedDiffFiles.Count) stripe(s): $((($selectedDiffFiles | ForEach-Object { $_.Name }) -join ', '))"
                }
                Invoke-AzCopyFiles -Files $selectedDiffFiles -TargetUri $targetUri -Description "Transfer diff backup for database '$dbName'"
                $logAnchorTime = $selectedDiffFile.LastWriteTime
            } else {
                Write-Log "No applicable diff backup found for $dbName after full backup $($fullFileInfo.Name)."
            }
        }

        $resolvedTranStart = Resolve-InitialTranStartFile -TranPath $resolvedTranPath -ExplicitStartFileName $resolvedTranStart -AnchorTime $logAnchorTime
        $logScanStart = Get-Date
        $logFiles = Get-BackupFiles -Root $resolvedTranPath -Extensions @('.trn') -Since $null -StartFileName $resolvedTranStart
        Write-TransferMigrationEvent -Mode $Mode -Level 'Info' -Phase 'Transfer' -Action 'OfflineCopyPlanned' -InstanceName $workItem.InstanceName -DatabaseName $dbName -Message 'Prepared offline backup copy set.' -Data @{
            fullBackup = $fullFileInfo.Name
            fullBackupStripes = $fullFileSet.Count
            fullBackupFiles = @($fullFileSet | ForEach-Object { $_.Name })
            diffBackup = if ($selectedDiffFile) { $selectedDiffFile.Name } else { $null }
            diffBackupStripes = $selectedDiffFiles.Count
            diffBackupFiles = @($selectedDiffFiles | ForEach-Object { $_.Name })
            firstLog = if ($logFiles.Count -gt 0) { $logFiles[0].Name } else { $null }
            lastLog = if ($logFiles.Count -gt 0) { $logFiles[$logFiles.Count - 1].Name } else { $null }
            logCount = $logFiles.Count
            targetUri = $targetUri
        }
        if ($logFiles.Count -eq 0) {
            Write-Warning "No log backups found in $resolvedTranPath for $dbName."
        }
        Write-Log "Transferring $($logFiles.Count) log backup file(s) for database '$dbName'."
        Invoke-AzCopyFiles -Files $logFiles -TargetUri $targetUri -Description "Transfer logs for database '$dbName'"

        if ($logFiles.Count -gt 0) {
            $lastLogTime = ($logFiles | Sort-Object -Property LastWriteTime | Select-Object -Last 1).LastWriteTime
            $state.LastSyncUtc[$stateKey] = $lastLogTime.ToUniversalTime().ToString('o')
        } else {
            $state.LastSyncUtc[$stateKey] = $logScanStart.ToUniversalTime().ToString('o')
        }
        Save-State -Path $StatePath -State $state
        Write-TransferMigrationEvent -Mode $Mode -Level 'Success' -Phase 'Transfer' -Action 'OfflineCopyCompleted' -InstanceName $workItem.InstanceName -DatabaseName $dbName -Message 'Offline backup copy completed.' -Data @{ lastSyncUtc = $state.LastSyncUtc[$stateKey] }
    }

    Write-Log "Offline copy complete. State saved to $StatePath"
    Write-TransferMigrationEvent -Mode $Mode -Level 'Success' -Phase 'Transfer' -Action 'OfflineComplete' -Message 'Offline backup transfer completed successfully.' -Data @{ statePath = $StatePath }
    exit 0
}

Write-Log "Step 3: online copy phase starting. Polling every $IntervalSeconds second(s)."
Write-Log "Online mode: initial copy and then poll every $IntervalSeconds seconds"

foreach ($workItem in $workItems) {
    $dbName = $workItem.DbName
    $stateKey = $workItem.StateKey
    $sourceRoot = $workItem.SourceRoot
    $storageBase = $workItem.StorageBase
    $targetUri = Get-TargetUri -BaseUri $storageBase -AuthMode $StorageAuthMode -SasToken $StorageContainerSasToken

    $resolvedFullPath = Resolve-PathTemplate -Value $FullBackupPath -DbName $dbName
    $resolvedFullFile = Resolve-PathTemplate -Value $FullBackupFile -DbName $dbName
    $resolvedDiffPath = Resolve-PathTemplate -Value $DiffPath -DbName $dbName
    $resolvedTranPath = Resolve-PathTemplate -Value $TranPath -DbName $dbName
    $resolvedTranStart = Resolve-PathTemplate -Value $TranStartFile -DbName $dbName

    if (-not (Test-Path -LiteralPath $sourceRoot)) {
        throw "Source path '$sourceRoot' does not exist."
    }

    $resolvedFullPath = Resolve-DefaultChildPath -ExplicitPath $resolvedFullPath -SourceRoot $sourceRoot -CandidateNames @('FULL', 'Full', 'full')
    $resolvedDiffPath = Resolve-DefaultChildPath -ExplicitPath $resolvedDiffPath -SourceRoot $sourceRoot -CandidateNames @('DIFF', 'Diff', 'diff')
    $resolvedTranPath = Resolve-DefaultChildPath -ExplicitPath $resolvedTranPath -SourceRoot $sourceRoot -CandidateNames @('LOG', 'Log', 'log', 'TRAN', 'Tran', 'tran', 'TLOG', 'tlog', 'Logs', 'logs')

    if (-not $resolvedFullPath) { $resolvedFullPath = $sourceRoot }
    if (-not $resolvedTranPath) { $resolvedTranPath = $sourceRoot }

    if ($IncludeDiffs -and -not $resolvedDiffPath) {
        Write-Warning "IncludeDiffs was specified, but no DIFF folder was found under '$sourceRoot'."
    }

    Ensure-PathExists -PathValue $resolvedFullPath -Label "Full backup"
    Ensure-PathExists -PathValue $resolvedDiffPath -Label "Diff"
    Ensure-PathExists -PathValue $resolvedTranPath -Label "Tran"

    Write-Log "Preparing initial online transfer for database '$dbName'."
    Write-Log "Initial copy from $sourceRoot to $storageBase"

    $fullFileSet = @(Get-SelectedBackupFileSet -Root $resolvedFullPath -Extensions @('.bak') -ExplicitFileName $resolvedFullFile -Selection 'Latest' -Label 'Full backup')
    $fullFileInfo = $fullFileSet | Sort-Object -Property LastWriteTime | Select-Object -Last 1
    $logAnchorTime = $fullFileInfo.LastWriteTime
    $selectedDiffFiles = @()
    $selectedDiffFile = $null

    if ($fullFileSet.Count -gt 1) {
        Write-Log "Detected striped FULL backup for '$dbName' with $($fullFileSet.Count) stripe(s): $((($fullFileSet | ForEach-Object { $_.Name }) -join ', '))"
    }
    Invoke-AzCopyFiles -Files $fullFileSet -TargetUri $targetUri -Description "Transfer full backup for database '$dbName'"

    if ($IncludeDiffs -and $resolvedDiffPath) {
        $selectedDiffFiles = @(Get-ApplicableDiffBackupFileSet -DiffPath $resolvedDiffPath -FullBackupTime $fullFileInfo.LastWriteTime)
        if ($selectedDiffFiles.Count -gt 0) {
            $selectedDiffFile = $selectedDiffFiles | Sort-Object -Property LastWriteTime | Select-Object -Last 1
            if ($selectedDiffFiles.Count -gt 1) {
                Write-Log "Detected striped DIFF backup for '$dbName' with $($selectedDiffFiles.Count) stripe(s): $((($selectedDiffFiles | ForEach-Object { $_.Name }) -join ', '))"
            }
            Invoke-AzCopyFiles -Files $selectedDiffFiles -TargetUri $targetUri -Description "Transfer diff backup for database '$dbName'"
            $logAnchorTime = $selectedDiffFile.LastWriteTime
        } else {
            Write-Log "No applicable diff backup found for $dbName after full backup $($fullFileInfo.Name)."
        }
    }

    $resolvedTranStart = Resolve-InitialTranStartFile -TranPath $resolvedTranPath -ExplicitStartFileName $resolvedTranStart -AnchorTime $logAnchorTime
    $logScanStart = Get-Date
    $logFiles = Get-BackupFiles -Root $resolvedTranPath -Extensions @('.trn') -Since $null -StartFileName $resolvedTranStart
    Write-TransferMigrationEvent -Mode $Mode -Level 'Info' -Phase 'Transfer' -Action 'InitialOnlineCopyPlanned' -InstanceName $workItem.InstanceName -DatabaseName $dbName -Message 'Prepared initial online backup copy set.' -Data @{
        fullBackup = $fullFileInfo.Name
        fullBackupStripes = $fullFileSet.Count
        fullBackupFiles = @($fullFileSet | ForEach-Object { $_.Name })
        diffBackup = if ($selectedDiffFile) { $selectedDiffFile.Name } else { $null }
        diffBackupStripes = $selectedDiffFiles.Count
        diffBackupFiles = @($selectedDiffFiles | ForEach-Object { $_.Name })
        firstLog = if ($logFiles.Count -gt 0) { $logFiles[0].Name } else { $null }
        lastLog = if ($logFiles.Count -gt 0) { $logFiles[$logFiles.Count - 1].Name } else { $null }
        logCount = $logFiles.Count
        targetUri = $targetUri
    }
    if ($logFiles.Count -eq 0) {
        Write-Warning "No log backups found in $resolvedTranPath for $dbName."
    }
    Write-Log "Transferring $($logFiles.Count) initial log backup file(s) for database '$dbName'."
    Invoke-AzCopyFiles -Files $logFiles -TargetUri $targetUri -Description "Transfer initial logs for database '$dbName'"

    if ($logFiles.Count -gt 0) {
        $lastLogTime = ($logFiles | Sort-Object -Property LastWriteTime | Select-Object -Last 1).LastWriteTime
        $state.LastSyncUtc[$stateKey] = $lastLogTime.ToUniversalTime().ToString('o')
    } else {
        $state.LastSyncUtc[$stateKey] = $logScanStart.ToUniversalTime().ToString('o')
    }
    Save-State -Path $StatePath -State $state
    Write-TransferMigrationEvent -Mode $Mode -Level 'Success' -Phase 'Transfer' -Action 'InitialOnlineCopyCompleted' -InstanceName $workItem.InstanceName -DatabaseName $dbName -Message 'Initial online backup copy completed.' -Data @{ lastSyncUtc = $state.LastSyncUtc[$stateKey] }
}

while ($true) {
    Start-Sleep -Seconds $IntervalSeconds

    $state = Load-State -Path $StatePath
    if (-not $state.LastSyncUtc) {
        $state.LastSyncUtc = @{}
    }

    foreach ($workItem in $workItems) {
        $dbName = $workItem.DbName
        $stateKey = $workItem.StateKey
        $sourceRoot = $workItem.SourceRoot
        $storageBase = $workItem.StorageBase
        $targetUri = Get-TargetUri -BaseUri $storageBase -AuthMode $StorageAuthMode -SasToken $StorageContainerSasToken

        $resolvedTranPath = Resolve-PathTemplate -Value $TranPath -DbName $dbName
        $resolvedTranStart = Resolve-PathTemplate -Value $TranStartFile -DbName $dbName

        $resolvedTranPath = Resolve-DefaultChildPath -ExplicitPath $resolvedTranPath -SourceRoot $sourceRoot -CandidateNames @('LOG', 'Log', 'log', 'TRAN', 'Tran', 'tran', 'TLOG', 'tlog', 'Logs', 'logs')

        if (-not $resolvedTranPath) { $resolvedTranPath = $sourceRoot }

        Ensure-PathExists -PathValue $resolvedTranPath -Label "Tran"

        $lastSync = $null
        if ($state.LastSyncUtc.ContainsKey($stateKey)) {
            $lastSyncRaw = $state.LastSyncUtc[$stateKey]
            if ($lastSyncRaw -is [datetime]) {
                $lastSync = ([datetime]$lastSyncRaw).ToLocalTime()
            } else {
                $lastSyncText = [string]$lastSyncRaw
                $parsedLastSync = [datetime]::MinValue
                if ([datetime]::TryParse($lastSyncText, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind, [ref]$parsedLastSync)) {
                    $lastSync = $parsedLastSync.ToLocalTime()
                } elseif ([datetime]::TryParse($lastSyncText, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal, [ref]$parsedLastSync)) {
                    $lastSync = $parsedLastSync.ToLocalTime()
                } else {
                    $lastSync = [DateTime]::Parse($lastSyncText).ToLocalTime()
                }
            }
        }

        $startFileForPoll = if (-not $lastSync) { $resolvedTranStart } else { $null }
        $newFiles = Get-BackupFiles -Root $resolvedTranPath -Extensions @('.trn') -Since $lastSync -StartFileName $startFileForPoll
        if ($newFiles.Count -gt 0) {
            Write-Log "Found $($newFiles.Count) new log backups for $dbName"
            Write-Log "Transferring $($newFiles.Count) new log backup file(s) for database '$dbName'."
            Invoke-AzCopyFiles -Files $newFiles -TargetUri $targetUri -Description "Transfer polled logs for database '$dbName'"

            $lastLogTime = ($newFiles | Sort-Object -Property LastWriteTime | Select-Object -Last 1).LastWriteTime
            $state.LastSyncUtc[$stateKey] = $lastLogTime.ToUniversalTime().ToString('o')
            Save-State -Path $StatePath -State $state
            Write-TransferMigrationEvent -Mode $Mode -Level 'Info' -Phase 'Transfer' -Action 'PolledLogsUploaded' -InstanceName $workItem.InstanceName -DatabaseName $dbName -Message "Uploaded $($newFiles.Count) new transaction log backup(s)." -Data @{
                firstLog = $newFiles[0].Name
                lastLog = $newFiles[$newFiles.Count - 1].Name
                logCount = $newFiles.Count
                lastSyncUtc = $state.LastSyncUtc[$stateKey]
            }
        } else {
            Write-Warning "No new log backups found for $dbName in $resolvedTranPath"
        }
    }
}
} catch {
    Write-TransferMigrationEvent -Mode $Mode -Level 'Error' -Phase 'Transfer' -Action 'Failure' -Message $_.Exception.Message -Data @{
        sourcePath = $SourcePath
        sourcePathBase = $SourcePathBase
        instanceNames = $InstanceNames
        databaseNames = $DatabaseNames
    }
    throw
}
