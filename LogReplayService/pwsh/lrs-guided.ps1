[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [ValidateSet('Offline', 'Online')]
    [string]$Mode,

    [string]$ConfigPath = "$(Split-Path -Parent $MyInvocation.MyCommand.Path)\lrs-guided.config.json",

    [string]$ResourceGroupName,
    [string]$InstanceName,
    [string]$DatabaseName,
    [string[]]$DatabaseNames,
    [string]$Collation,
    [string]$StorageContainerUri,
    [string]$StorageContainerUriTemplate,
    [ValidateSet('ManagedIdentity', 'SharedAccessSignature')]
    [string]$StorageContainerIdentity = 'ManagedIdentity',
    [string]$StorageContainerSasToken,
    [string]$LastBackupName,
    [hashtable]$LastBackupNameMap,

    [int]$MonitorMinutes,
    [int]$PollSeconds,
    [string]$LogPath,
    [string]$EventLogPath,
    [string]$RunId,
    [switch]$QuietConsole,

    [switch]$CompleteOnlineCutover
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$reportHelperPath = Join-Path -Path (Split-Path -Parent $MyInvocation.MyCommand.Path) -ChildPath 'migration-report.ps1'
. $reportHelperPath
$repoDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$logsDir = Join-Path -Path $repoDir -ChildPath 'logs'

if (-not $RunId) {
    $RunId = [guid]::NewGuid().ToString()
}

function Write-Log {
    param([string]$Message)

    if ($QuietConsole) {
        $noisePatterns = @(
            '^Logging to ',
            '^Using Az.Accounts version ',
            '^Using Az.Sql version ',
            '^Using SQL Managed Instance managed identity '
        )

        foreach ($pattern in $noisePatterns) {
            if ($Message -match $pattern) {
                return
            }
        }
    }

    Write-Host $Message
}

function Read-ConfigFile {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    } catch {
        throw "Failed to read config JSON at '$Path'. $($_.Exception.Message)"
    }
}

function Get-ConfigValue {
    param(
        [object]$ConfigObject,
        [string]$PropertyName
    )

    if (-not $ConfigObject) {
        return $null
    }

    $property = $ConfigObject.PSObject.Properties[$PropertyName]
    if ($property) {
        return $property.Value
    }

    return $null
}

function Resolve-ConfiguredValue {
    param(
        [string]$Current,
        [string]$FromConfig
    )

    if ($Current) { return $Current }
    if ($FromConfig) { return $FromConfig }
    return $null
}

function Read-PromptValue {
    param(
        [string]$Name,
        [string]$CurrentValue,
        [string]$PromptText,
        [switch]$Secret
    )

    if ($CurrentValue) { return $CurrentValue }

    if ($Secret) {
        $secure = Read-Host -Prompt $PromptText -AsSecureString
        if (-not $secure) { return $null }
        $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
        try {
            return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
        } finally {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
        }
    }

    return Read-Host -Prompt $PromptText
}

function Initialize-AzContext {
    $context = Get-AzContext -ErrorAction SilentlyContinue
    if (-not $context) {
        Write-Log "No Azure context found. Signing in..."
        Connect-AzAccount | Out-Null
    }
}

function Test-AzureClaimsChallenge {
    param([string]$Text)

    if (-not $Text) {
        return $false
    }

    return $Text -match '(?i)claims challenge|Status_InteractionRequired|Response_Status\.Status_InteractionRequired|interaction required|acrs|-ClaimsChallenge'
}

function Get-AzureClaimsChallengeToken {
    param([string]$Text)

    if (-not $Text) {
        return $null
    }

    $claimsChallengeMatches = [regex]::Matches($Text, '(?im)-ClaimsChallenge\s+["“]?([A-Za-z0-9+/=_-]{20,})["”]?', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    foreach ($match in $claimsChallengeMatches) {
        $candidate = [string]$match.Groups[1].Value
        if (-not $candidate) {
            continue
        }

        $normalized = $candidate.Replace('-', '+').Replace('_', '/')
        switch ($normalized.Length % 4) {
            2 { $normalized += '==' }
            3 { $normalized += '=' }
        }

        try {
            $decoded = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($normalized))
            if ($decoded -match '"access_token"' -or $decoded -match '"acrs"') {
                return $candidate
            }
        } catch {
        }
    }

    return $null
}

function Connect-AzPowerShellInteractive {
    param(
        [string]$Tenant,
        [string]$Subscription,
        [string]$ClaimsChallenge,
        [bool]$UseDeviceAuthentication
    )

    $connectArgs = @{}
    if ($Tenant) {
        $connectArgs['Tenant'] = $Tenant
    }

    if ($Subscription) {
        $connectArgs['Subscription'] = $Subscription
    }

    if ($ClaimsChallenge) {
        $connectArgs['ClaimsChallenge'] = $ClaimsChallenge
    }

    if ($UseDeviceAuthentication) {
        $connectArgs['UseDeviceAuthentication'] = $true
    }

    Connect-AzAccount @connectArgs | Out-Null
}

function Invoke-AzOperationWithClaimsChallengeRetry {
    param(
        [scriptblock]$Operation,
        [string]$OperationName
    )

    try {
        return & $Operation
    } catch {
        $errorTextParts = @(
            ($_ | Out-String),
            $_.Exception.Message,
            $_.Exception.ToString(),
            $_.ScriptStackTrace
        ) | Where-Object { $_ }
        $errorText = $errorTextParts -join [Environment]::NewLine

        if (-not (Test-AzureClaimsChallenge -Text $errorText)) {
            throw
        }

        $claimsChallenge = Get-AzureClaimsChallengeToken -Text $errorText
        if (-not $claimsChallenge) {
            Write-Log "$OperationName failed with an MFA claims challenge, but the challenge token could not be parsed from the error text."
            throw
        }

        $tenantId = $null
        $subscriptionId = $null
        $context = Get-AzContext -ErrorAction SilentlyContinue
        if ($context) {
            if ($context.Tenant -and $context.Tenant.Id) {
                $tenantId = [string]$context.Tenant.Id
            }

            if ($context.Subscription -and $context.Subscription.Id) {
                $subscriptionId = [string]$context.Subscription.Id
            }
        }

        Write-Log "$OperationName requires MFA claims-challenge reauthentication. Starting device authentication..."
        Write-Log "Claims challenge token detected."

        try {
            Connect-AzPowerShellInteractive -Tenant $tenantId -Subscription $subscriptionId -UseDeviceAuthentication $true
        } catch {
            Write-Log 'Device authentication without an explicit claims challenge failed. Falling back to interactive claims-challenge sign-in...'
            Connect-AzPowerShellInteractive -Tenant $tenantId -ClaimsChallenge $claimsChallenge -UseDeviceAuthentication $false
        }

        try {
            return & $Operation
        } catch {
            $retryErrorParts = @(
                ($_ | Out-String),
                $_.Exception.Message,
                $_.Exception.ToString(),
                $_.ScriptStackTrace
            ) | Where-Object { $_ }
            $retryErrorText = $retryErrorParts -join [Environment]::NewLine

            if (-not (Test-AzureClaimsChallenge -Text $retryErrorText)) {
                throw
            }

            Write-Log 'Device authentication completed, but Azure still requested an explicit claims challenge. Retrying device authentication with the claims challenge token...'

            $claimsChallengeSatisfied = $false
            try {
                Connect-AzPowerShellInteractive -Tenant $tenantId -Subscription $subscriptionId -ClaimsChallenge $claimsChallenge -UseDeviceAuthentication $true
                $claimsChallengeSatisfied = $true
            } catch {
                Write-Log 'Device authentication with an explicit claims challenge failed. Falling back to interactive claims-challenge sign-in...'
                Connect-AzPowerShellInteractive -Tenant $tenantId -Subscription $subscriptionId -ClaimsChallenge $claimsChallenge -UseDeviceAuthentication $false
                $claimsChallengeSatisfied = $true
            }

            if ($claimsChallengeSatisfied) {
                try {
                    return & $Operation
                } catch {
                    $finalErrorParts = @(
                        ($_ | Out-String),
                        $_.Exception.Message,
                        $_.Exception.ToString(),
                        $_.ScriptStackTrace
                    ) | Where-Object { $_ }
                    $finalErrorText = $finalErrorParts -join [Environment]::NewLine

                    if (-not (Test-AzureClaimsChallenge -Text $finalErrorText)) {
                        throw
                    }
                }
            }

            $manualCommandParts = @('Connect-AzAccount')
            if ($tenantId) {
                $manualCommandParts += "-Tenant '$tenantId'"
            }

            if ($subscriptionId) {
                $manualCommandParts += "-Subscription '$subscriptionId'"
            }

            $manualCommandParts += "-ClaimsChallenge '$claimsChallenge'"
            $manualCommandParts += '-UseDeviceAuthentication'
            $manualCommand = $manualCommandParts -join ' '

            Write-Log 'Automated claims-challenge reauthentication is not succeeding with the current Az.Accounts module in this flow.'
            Write-Log "Run this command in the current terminal, then rerun the wrapper: $manualCommand"

            throw "Azure still requires an explicit claims challenge after device authentication. Run this command in the current terminal, then rerun the wrapper: $manualCommand"
        }
    }
}

function Get-SasTokenParts {
    param([string]$Token)

    $clean = $Token.Trim()
    if ($clean.StartsWith('?')) {
        $clean = $clean.Substring(1)
    }

    $pairs = $clean -split '&'
    $result = @{}
    foreach ($pair in $pairs) {
        if (-not $pair) { continue }
        $kv = $pair -split '=', 2
        if ($kv.Count -eq 2) {
            $result[$kv[0]] = [System.Uri]::UnescapeDataString($kv[1])
        }
    }

    return $result
}

function ConvertTo-SasTime {
    param([string]$Value)

    if (-not $Value) { return $null }
    $styles = [System.Globalization.DateTimeStyles]::AssumeUniversal
    return [DateTimeOffset]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture, $styles)
}

function Test-SasToken {
    param(
        [string]$Token,
        [string]$ModeName,
        [int]$MonitorMinutesValue
    )

    if (-not $Token) {
        throw 'SAS token is required.'
    }

    $parts = Get-SasTokenParts -Token $Token
    $sp = $parts['sp']
    $se = $parts['se']
    $st = $parts['st']

    if (-not $sp) {
        throw 'SAS token is missing sp (permissions).'
    }

    $invalid = ($sp.ToCharArray() | Where-Object { $_ -notin @('r','l','w') })
    if ($invalid) {
        throw "SAS token permissions must be Read, List, and optionally Write. Found: $sp"
    }

    $startTime = ConvertTo-SasTime -Value $st
    $endTime = ConvertTo-SasTime -Value $se

    if (-not $endTime) {
        throw 'SAS token is missing se (expiry time).'
    }

    $now = [DateTimeOffset]::UtcNow

    if ($startTime -and $startTime -gt $now) {
        Write-Warning "SAS token start time is in the future: $startTime"
    }

    if ($endTime -le $now) {
        throw "SAS token has expired at $endTime."
    }

    if ($ModeName -eq 'Online') {
        $minBuffer = [Math]::Max($MonitorMinutesValue, 60)
        $bufferTime = $now.AddMinutes($minBuffer)
        if ($endTime -lt $bufferTime) {
            throw "SAS token expires too soon ($endTime). Extend expiry beyond $bufferTime UTC for online migration."
        }
    }
}

function Test-StorageAuthInputs {
    param(
        [string]$IdentityMode,
        [string]$SasToken,
        [string]$ModeName,
        [int]$MonitorMinutesValue
    )

    if ($IdentityMode -ne 'ManagedIdentity') {
        throw 'This workflow requires StorageContainerIdentity ManagedIdentity for all LRS restore operations.'
    }

    if ($SasToken) {
        Write-Warning 'StorageContainerSasToken is ignored when StorageContainerIdentity is ManagedIdentity.'
    }

    Write-Log 'Using SQL Managed Instance managed identity for LRS storage access. Ensure the managed identity has Storage Blob Data Reader or equivalent Read/List access on the target container.'
}

function Import-LatestAzModule {
    param(
        [string]$ModuleName,
        [version]$MinimumVersion
    )

    $attemptedInstall = $false

    while ($true) {
        $module = Get-Module -ListAvailable -Name $ModuleName | Sort-Object Version -Descending | Select-Object -First 1

        if ($module -and $module.Version -ge $MinimumVersion) {
            $loadedModule = Get-Module -Name $ModuleName | Sort-Object Version -Descending | Select-Object -First 1
            if (-not $loadedModule -or $loadedModule.Version -ne $module.Version) {
                $loadedAzModules = @(Get-Module -Name 'Az.*' | Sort-Object Name -Descending)
                foreach ($loadedAzModule in $loadedAzModules) {
                    Remove-Module -Name $loadedAzModule.Name -Force -ErrorAction SilentlyContinue
                }

                Import-Module -Name $ModuleName -RequiredVersion $module.Version -Force -ErrorAction Stop
                Write-Log "Using $ModuleName version $($module.Version)."
            }

            return $module
        }

        if ($attemptedInstall) {
            throw "$ModuleName module is required. Automatic installation did not succeed."
        }

        Write-Log "$ModuleName module was not found or is below the required version. Attempting installation for the current user..."
        Install-Module -Name $ModuleName -MinimumVersion $MinimumVersion.ToString() -Scope CurrentUser -Repository PSGallery -Force -AllowClobber -ErrorAction Stop
        $attemptedInstall = $true
    }
}

function Test-AzSqlModule {
    Import-LatestAzModule -ModuleName 'Az.Accounts' -MinimumVersion ([version]'5.3.3') | Out-Null
    Import-LatestAzModule -ModuleName 'Az.Sql' -MinimumVersion ([version]'4.0.0') | Out-Null
}

function ConvertTo-Hashtable {
    param([object]$InputObject)

    if (-not $InputObject) { return $null }
    if ($InputObject -is [hashtable]) { return $InputObject }

    $table = @{}
    foreach ($prop in $InputObject.PSObject.Properties) {
        $table[$prop.Name] = $prop.Value
    }

    return $table
}

function Resolve-LastBackupForDb {
    param(
        [string]$DbName,
        [hashtable]$Map,
        [string]$DefaultValue
    )

    if ($Map -and $Map.ContainsKey($DbName)) {
        return [string]$Map[$DbName]
    }

    return $DefaultValue
}

function Test-StorageUriRules {
    param(
        [string]$Value,
        [string]$Label
    )

    if (-not $Value) { return }

    $pathOnly = $Value -replace '^[a-z]+://[^/]+', ''

    if ($pathOnly -match '(?i)(^|/|\\)[^/\\]*backup[^/\\]*(/|\\|$)') {
        throw "$Label contains 'backup'. LRS does not allow 'backup' in container or folder names."
    }

    if ($pathOnly -match '(?i)(/|\\)full(/|$)') {
        throw "$Label contains a nested 'full' folder. LRS requires a flat folder per database."
    }

    if ($pathOnly -match '(?i)(/|\\)tran(/|$)') {
        throw "$Label contains a nested 'tran' folder. LRS requires a flat folder per database."
    }

    if ($Value -match '\?') {
        throw "$Label must not include a SAS token or question mark."
    }
}

function Resolve-StorageUri {
    param(
        [string]$BaseUri,
        [string]$Template,
        [string]$DbName,
        [switch]$AppendDb
    )

    function Convert-ToLrsBlobEndpointUri {
        param([string]$Value)

        if (-not $Value) {
            return $null
        }

        $uri = [Uri]$Value
        $builder = [UriBuilder]::new($uri)
        if ($builder.Host -match '^(?<account>[^.]+)\.dfs\.core\.windows\.net$') {
            $builder.Host = "$($Matches.account).blob.core.windows.net"
        }

        return $builder.Uri.AbsoluteUri
    }

    function Join-StorageUriPath {
        param(
            [string]$Value,
            [string]$PathSegment
        )

        $uri = [Uri]$Value
        $builder = [UriBuilder]::new($uri)
        $builder.Path = ($builder.Path.TrimEnd('/') + '/' + [Uri]::EscapeDataString($PathSegment) + '/')
        return $builder.Uri.AbsoluteUri
    }

    if ($Template) {
        $resolved = $Template -replace '\{db\}', $DbName
        $resolved = $resolved -replace '\{database\}', $DbName
        return Convert-ToLrsBlobEndpointUri -Value $resolved
    }

    if (-not $BaseUri) {
        return $null
    }

    if ($AppendDb) {
        return Convert-ToLrsBlobEndpointUri -Value (Join-StorageUriPath -Value $BaseUri -PathSegment $DbName)
    }

    return Convert-ToLrsBlobEndpointUri -Value $BaseUri
}

function Invoke-LrsMigration {
    [CmdletBinding()]
    param(
        [string]$Rg,
        [string]$Mi,
        [string]$DatabaseName,
        [string]$StorageUri,
        [string]$StorageIdentity,
        [string]$SasToken,
        [string]$ModeName,
        [string]$CollationName,
        [string]$LastBackup,
        [int]$MonitorMinutesValue,
        [int]$PollSecondsValue,
        [switch]$CompleteCutover
    )

    Test-StorageUriRules -Value $StorageUri -Label "StorageContainerUri for $DatabaseName"

    $startParams = @{
        ResourceGroupName        = $Rg
        InstanceName             = $Mi
        Name                     = $DatabaseName
        StorageContainerUri      = $StorageUri
        StorageContainerIdentity = $StorageIdentity
    }

    if ($StorageIdentity -eq 'SharedAccessSignature') {
        $startParams['StorageContainerSasToken'] = $SasToken
    }

    if ($CollationName) {
        $startParams['Collation'] = $CollationName
    }

    if ($ModeName -eq 'Offline') {
        if (-not $LastBackup) {
            throw "Offline mode requires LastBackupName for ${DatabaseName}."
        }

        $startParams['AutoCompleteRestore'] = $true
        $startParams['LastBackupName'] = $LastBackup
    }

    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Info' -Phase 'LRS' -Action 'StartRequested' -InstanceName $Mi -DatabaseName $DatabaseName -Message 'Submitting Log Replay Service start request.' -Data @{
        storageUri = $StorageUri
        storageIdentity = $StorageIdentity
        lastBackupName = $LastBackup
        collation = $CollationName
    }
    [void](Invoke-AzOperationWithClaimsChallengeRetry -Operation { Start-AzSqlInstanceDatabaseLogReplay @startParams } -OperationName "Start LRS for $DatabaseName")
    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Success' -Phase 'LRS' -Action 'StartSubmitted' -InstanceName $Mi -DatabaseName $DatabaseName -Message 'Log Replay Service start request submitted.' -Data @{ storageUri = $StorageUri }

    Write-Log "Monitoring LRS status for $DatabaseName for $MonitorMinutesValue minutes (poll every $PollSecondsValue seconds) ..."
    $stopWatch = [Diagnostics.Stopwatch]::StartNew()

    while ($stopWatch.Elapsed.TotalMinutes -lt $MonitorMinutesValue) {
        try {
            $status = Show-LrsStatus -Rg $Rg -Mi $Mi -DatabaseName $DatabaseName
            $statusText = ''
            if ($status.PSObject.Properties.Match('Status').Count -gt 0) {
                $statusText = [string]$status.Status
            }

            Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Info' -Phase 'LRS' -Action 'StatusSnapshot' -InstanceName $Mi -DatabaseName $DatabaseName -Message "Current LRS status: $statusText" -Data @{
                status = $statusText
                percentComplete = if ($status.PSObject.Properties.Match('PercentComplete').Count -gt 0) { $status.PercentComplete } else { $null }
                lastBackupApplied = if ($status.PSObject.Properties.Match('LastBackupApplied').Count -gt 0) { $status.LastBackupApplied } else { $null }
                lastRestoredBackupFileName = if ($status.PSObject.Properties.Match('LastRestoredBackupFileName').Count -gt 0) { $status.LastRestoredBackupFileName } else { $null }
            }

            if ($statusText -match '(?i)failed|canceled|cancelled') {
                Write-Warning "LRS status indicates failure for ${DatabaseName}: $statusText"
                Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Error' -Phase 'LRS' -Action 'StatusFailed' -InstanceName $Mi -DatabaseName $DatabaseName -Message "LRS status indicates failure: $statusText" -Data @{ status = $statusText }
                Show-RecentOperations -Rg $Rg -Mi $Mi
                break
            }

            if ($statusText -match '(?i)completed|succeeded|success') {
                Write-Host ("LRS status indicates completion for {0}: {1}" -f $DatabaseName, $statusText) -ForegroundColor Blue
                Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Success' -Phase 'LRS' -Action 'StatusCompleted' -InstanceName $Mi -DatabaseName $DatabaseName -Message "LRS status indicates completion: $statusText" -Data @{ status = $statusText }
                break
            }
        } catch {
            Write-Warning "Failed to fetch status for ${DatabaseName}: $($_.Exception.Message)"
            Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Warning' -Phase 'LRS' -Action 'StatusReadFailed' -InstanceName $Mi -DatabaseName $DatabaseName -Message $_.Exception.Message
        }

        Start-Sleep -Seconds $PollSecondsValue
    }

    if ($ModeName -eq 'Online' -and $CompleteCutover) {
        if (-not $LastBackup) {
            throw "CompleteOnlineCutover requires LastBackupName for ${DatabaseName}."
        }

        Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Info' -Phase 'Cutover' -Action 'CutoverRequested' -InstanceName $Mi -DatabaseName $DatabaseName -Message 'Submitting online cutover request.' -Data @{ lastBackupName = $LastBackup }
        Invoke-AzOperationWithClaimsChallengeRetry -Operation {
            Complete-AzSqlInstanceDatabaseLogReplay -ResourceGroupName $Rg -InstanceName $Mi -Name $DatabaseName -LastBackupName $LastBackup
        } -OperationName "Complete LRS cutover for $DatabaseName"
        Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $ModeName -Level 'Success' -Phase 'Cutover' -Action 'CutoverSubmitted' -InstanceName $Mi -DatabaseName $DatabaseName -Message 'Online cutover request submitted.' -Data @{ lastBackupName = $LastBackup }
    }
}

function Show-LrsStatus {
    param(
        [string]$Rg,
        [string]$Mi,
        [string]$DatabaseName
    )

    $lrs = Get-AzSqlInstanceDatabaseLogReplay -ResourceGroupName $Rg -InstanceName $Mi -Name $DatabaseName

    $keys = @(
        'Status',
        'StartTime',
        'LastRestoreTime',
        'LastBackupFileName',
        'LastRestoredBackupFileName',
        'LastRestoredFileName',
        'LastBackupApplied',
        'PercentComplete',
        'RestoredFilesCount',
        'PendingFilesCount'
    )

    $summary = [ordered]@{}
    foreach ($key in $keys) {
        if ($lrs.PSObject.Properties.Match($key).Count -gt 0) {
            $summary[$key] = $lrs.$key
        }
    }

    if ($summary.Count -gt 0) {
        $statusText = if ($summary.Contains('Status')) { [string]$summary['Status'] } else { 'Unknown' }
        $percentText = if ($summary.Contains('PercentComplete') -and $null -ne $summary['PercentComplete']) { [string]$summary['PercentComplete'] } else { 'n/a' }
        $lastRestoredText = $null
        foreach ($candidateKey in @('LastRestoredBackupFileName', 'LastRestoredFileName', 'LastBackupApplied', 'LastBackupFileName')) {
            if ($summary.Contains($candidateKey) -and $summary[$candidateKey]) {
                $lastRestoredText = [string]$summary[$candidateKey]
                break
            }
        }

        if ($lastRestoredText) {
            Write-Host ("{0} status: {1} | Progress: {2} | Last restored: {3}" -f $DatabaseName, $statusText, $percentText, $lastRestoredText)
        } else {
            Write-Host ("{0} status: {1} | Progress: {2}" -f $DatabaseName, $statusText, $percentText)
        }
    } else {
        Write-Host ("{0} status: unavailable" -f $DatabaseName)
    }

    return $lrs
}

function Show-RecentOperations {
    param(
        [string]$Rg,
        [string]$Mi
    )

    $ops = Get-AzSqlInstanceOperation -ResourceGroupName $Rg -InstanceName $Mi |
        Sort-Object -Property StartTime -Descending |
        Select-Object -First 5

    if ($ops) {
        Write-Host "Recent managed instance operations (latest 5):"
        $ops | Select-Object Operation, State, StartTime, EndTime | Format-Table -AutoSize
    }
}

$config = Read-ConfigFile -Path $ConfigPath

$ResourceGroupName = Resolve-ConfiguredValue -Current $ResourceGroupName -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'ResourceGroupName')
$InstanceName = Resolve-ConfiguredValue -Current $InstanceName -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'InstanceName')
$DatabaseName = Resolve-ConfiguredValue -Current $DatabaseName -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'DatabaseName')
$DatabaseNames = if ($DatabaseNames) { $DatabaseNames } else { Get-ConfigValue -ConfigObject $config -PropertyName 'DatabaseNames' }
$Collation = Resolve-ConfiguredValue -Current $Collation -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'Collation')
$StorageContainerUri = Resolve-ConfiguredValue -Current $StorageContainerUri -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'StorageContainerUri')
$StorageContainerUriTemplate = Resolve-ConfiguredValue -Current $StorageContainerUriTemplate -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'StorageContainerUriTemplate')
$StorageContainerIdentity = Resolve-ConfiguredValue -Current $StorageContainerIdentity -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'StorageContainerIdentity')
$StorageContainerSasToken = Resolve-ConfiguredValue -Current $StorageContainerSasToken -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'StorageContainerSasToken')
$LastBackupName = Resolve-ConfiguredValue -Current $LastBackupName -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'LastBackupName')
$LastBackupNameMap = if ($LastBackupNameMap) { $LastBackupNameMap } else { ConvertTo-Hashtable -InputObject (Get-ConfigValue -ConfigObject $config -PropertyName 'LastBackupNames') }
$LogPath = Resolve-ConfiguredValue -Current $LogPath -FromConfig (Get-ConfigValue -ConfigObject $config -PropertyName 'LogPath')

if (-not $MonitorMinutes) { $MonitorMinutes = Get-ConfigValue -ConfigObject $config -PropertyName 'MonitorMinutes' }
if (-not $MonitorMinutes) { $MonitorMinutes = 30 }
if (-not $PollSeconds) { $PollSeconds = Get-ConfigValue -ConfigObject $config -PropertyName 'PollSeconds' }
if (-not $PollSeconds) { $PollSeconds = 60 }

if (-not $LogPath) {
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $logName = "lrs-guided-$stamp.log"
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
    $LogPath = Join-Path -Path $logsDir -ChildPath $logName
}

if (-not $Mode) {
    $Mode = Read-Host -Prompt "Mode (Offline|Online)"
}

if ($Mode -notin @('Offline', 'Online')) {
    throw "Invalid mode '$Mode'. Use Offline or Online."
}

$ResourceGroupName = Read-PromptValue -Name 'ResourceGroupName' -CurrentValue $ResourceGroupName -PromptText 'Resource group name'
$InstanceName = Read-PromptValue -Name 'InstanceName' -CurrentValue $InstanceName -PromptText 'Managed instance name'
$DatabaseName = if ($DatabaseName -or -not $DatabaseNames) { Read-PromptValue -Name 'DatabaseName' -CurrentValue $DatabaseName -PromptText 'Target managed database name (leave blank if using DatabaseNames)' } else { $null }
$DatabaseNamesInput = if (-not $DatabaseNames) { Read-Host -Prompt 'Database names (comma-separated, optional)' } else { $null }
$DatabaseNames = if ($DatabaseNames) { $DatabaseNames } elseif ($DatabaseNamesInput) { $DatabaseNamesInput -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ } } else { $null }
$StorageContainerUri = if ($StorageContainerUri -or -not $StorageContainerUriTemplate) { Read-PromptValue -Name 'StorageContainerUri' -CurrentValue $StorageContainerUri -PromptText 'Storage container URI (no SAS, base or database folder)' } else { $null }
$StorageContainerUriTemplate = if ($StorageContainerUriTemplate -or -not $StorageContainerUri) { Read-PromptValue -Name 'StorageContainerUriTemplate' -CurrentValue $StorageContainerUriTemplate -PromptText 'Storage container URI template (optional, use {db})' } else { $null }

if (-not $StorageContainerIdentity) {
    $StorageContainerIdentity = Read-Host -Prompt 'Storage container identity (ManagedIdentity|SharedAccessSignature)'
}

if ($StorageContainerIdentity -eq 'SharedAccessSignature') {
    $StorageContainerSasToken = Read-PromptValue -Name 'StorageContainerSasToken' -CurrentValue $StorageContainerSasToken -PromptText 'SAS token (Read+List only, no leading ?)' -Secret
}

if (-not $Collation -and -not $config) {
    $Collation = Read-Host -Prompt 'Collation (press Enter to skip)'
}

if ($Mode -eq 'Offline' -or $CompleteOnlineCutover) {
    if (-not $LastBackupNameMap -or $LastBackupNameMap.Count -eq 0) {
        $LastBackupName = Read-PromptValue -Name 'LastBackupName' -CurrentValue $LastBackupName -PromptText 'Last backup file name (for autocomplete or cutover, optional if using LastBackupNames map)'
    }
}

if (-not $ResourceGroupName -or -not $InstanceName) {
    throw 'Missing required inputs. Provide values via parameters, config, or prompts.'
}

$databaseList = @()
if ($DatabaseNames -and $DatabaseNames.Count -gt 0) {
    $databaseList = $DatabaseNames
} elseif ($DatabaseName) {
    $databaseList = @($DatabaseName)
}

if ($databaseList.Count -eq 0) {
    throw 'Provide DatabaseName or DatabaseNames.'
}

if (-not $StorageContainerUri -and -not $StorageContainerUriTemplate) {
    throw 'Provide StorageContainerUri or StorageContainerUriTemplate.'
}

if ($databaseList.Count -gt 1 -and -not $StorageContainerUriTemplate) {
    throw 'Multi-database mode requires StorageContainerUriTemplate to avoid ambiguous base URIs.'
}

Test-StorageUriRules -Value $StorageContainerUri -Label 'StorageContainerUri'
Test-StorageUriRules -Value $StorageContainerUriTemplate -Label 'StorageContainerUriTemplate'

try {
    Start-Transcript -Path $LogPath -Append | Out-Null
    Write-Log "Logging to $LogPath"
    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $Mode -Level 'Info' -Phase 'LRS' -Action 'ScriptStart' -InstanceName $InstanceName -Message 'Guided LRS execution started.' -Data @{
        resourceGroupName = $ResourceGroupName
        databaseNames = $databaseList
        storageContainerIdentity = $StorageContainerIdentity
        logPath = $LogPath
    }

    Test-AzSqlModule
    Test-StorageAuthInputs -IdentityMode $StorageContainerIdentity -SasToken $StorageContainerSasToken -ModeName $Mode -MonitorMinutesValue $MonitorMinutes

    Initialize-AzContext

    $appendDb = $databaseList.Count -gt 1
    foreach ($dbName in $databaseList) {
        $resolvedUri = Resolve-StorageUri -BaseUri $StorageContainerUri -Template $StorageContainerUriTemplate -DbName $dbName -AppendDb:$appendDb
        if (-not $resolvedUri) {
            throw "Unable to resolve storage URI for $dbName."
        }

        $dbLastBackup = Resolve-LastBackupForDb -DbName $dbName -Map $LastBackupNameMap -DefaultValue $LastBackupName

        Invoke-LrsMigration -Rg $ResourceGroupName -Mi $InstanceName -DatabaseName $dbName -StorageUri $resolvedUri -StorageIdentity $StorageContainerIdentity -SasToken $StorageContainerSasToken -ModeName $Mode -CollationName $Collation -LastBackup $dbLastBackup -MonitorMinutesValue $MonitorMinutes -PollSecondsValue $PollSeconds -CompleteCutover:$CompleteOnlineCutover
    }
    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $Mode -Level 'Success' -Phase 'LRS' -Action 'ScriptCompleted' -InstanceName $InstanceName -Message 'Guided LRS execution completed.' -Data @{ databaseNames = $databaseList }
} catch {
    Write-MigrationEvent -EventLogPath $EventLogPath -RunId $RunId -Source 'lrs-guided.ps1' -Mode $Mode -Level 'Error' -Phase 'LRS' -Action 'ScriptFailed' -InstanceName $InstanceName -Message $_.Exception.Message -Data @{ resourceGroupName = $ResourceGroupName; databaseNames = $databaseList }
    throw
} finally {
    try {
        Stop-Transcript | Out-Null
    } catch {
    }
}
