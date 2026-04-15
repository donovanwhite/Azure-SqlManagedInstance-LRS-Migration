[CmdletBinding()]
param(
    [string]$TenantId,
    [string]$SubscriptionId,
    [string]$ResourceGroupName,
    [string]$ManagedInstanceName,
    [string]$DatabaseName,
    [string]$StorageContainerUri,
    [ValidateSet('ManagedIdentity')]
    [string]$StorageContainerIdentity,
    [string]$StorageContainerSasToken,
    [string]$ResultPath,
    [switch]$WhatIfStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-WorkerTrace {
    param([string]$Message)

    Write-Output ("[{0}] {1}" -f ((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')), $Message)
}

function Import-LatestAzModule {
    param([string]$ModuleName)

    $attemptedInstall = $false

    while ($true) {
        $module = Get-Module -ListAvailable -Name $ModuleName | Sort-Object Version -Descending | Select-Object -First 1

        if ($module) {
            $loadedModule = Get-Module -Name $ModuleName | Sort-Object Version -Descending | Select-Object -First 1
            if (-not $loadedModule -or $loadedModule.Version -ne $module.Version) {
                Import-Module -Name $ModuleName -RequiredVersion $module.Version -Force -ErrorAction Stop
            }

            return
        }

        if ($attemptedInstall) {
            throw "$ModuleName module is required in the LRS start worker process. Automatic installation did not succeed."
        }

        Install-Module -Name $ModuleName -Scope CurrentUser -Repository PSGallery -Force -AllowClobber -ErrorAction Stop
        $attemptedInstall = $true
    }
}

function Write-WorkerResult {
    param(
        [bool]$Success,
        [string]$Message,
        [string]$OutputText
    )

    $resultDirectory = Split-Path -Parent $ResultPath
    if ($resultDirectory) {
        New-Item -ItemType Directory -Path $resultDirectory -Force | Out-Null
    }

    [ordered]@{
        success = $Success
        message = $Message
        output = $OutputText
        databaseName = $DatabaseName
        managedInstanceName = $ManagedInstanceName
        resourceGroupName = $ResourceGroupName
        timestampUtc = (Get-Date).ToUniversalTime().ToString('o')
    } | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $ResultPath
}

function Get-ExistingOnlineRestoreState {
    $existingLrs = $null
    try {
        $existingLrs = Get-AzSqlInstanceDatabaseLogReplay -ResourceGroupName $ResourceGroupName -InstanceName $ManagedInstanceName -Name $DatabaseName -ErrorAction Stop
    } catch {
    }

    if ($existingLrs) {
        return [pscustomobject]@{
            Found = $true
            Kind = 'Lrs'
            Status = [string]$existingLrs.Status
        }
    }

    $managedDatabase = $null
    try {
        $managedDatabase = Get-AzSqlInstanceDatabase -ResourceGroupName $ResourceGroupName -InstanceName $ManagedInstanceName -Name $DatabaseName -ErrorAction Stop
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

try {
    Write-WorkerTrace "Starting online LRS worker for database '$DatabaseName'."

    Write-WorkerTrace 'Importing Az.Accounts module.'
    Import-LatestAzModule -ModuleName 'Az.Accounts'
    Write-WorkerTrace 'Importing Az.Sql module.'
    Import-LatestAzModule -ModuleName 'Az.Sql'

    if (-not (Get-Command Get-AzContext -ErrorAction SilentlyContinue)) {
        throw 'Az.Accounts is not available in the worker process.'
    }

    Write-WorkerTrace 'Checking Az PowerShell context.'
    $context = Get-AzContext -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
    if (-not $context) {
        throw 'No Az PowerShell context is available in the LRS start worker. Complete authentication in the parent session and rerun the wrapper.'
    }

    if ($SubscriptionId) {
        Write-WorkerTrace "Setting Az context to subscription '$SubscriptionId'."
        $setContextArgs = @{ Subscription = $SubscriptionId }
        if ($TenantId) {
            $setContextArgs['Tenant'] = $TenantId
        }

        Set-AzContext @setContextArgs | Out-Null
    }

    $existingState = Get-ExistingOnlineRestoreState
    if ($existingState.Found) {
        $message = "Existing online restore already present with status $($existingState.Status)."
        Write-WorkerTrace $message
        Write-WorkerResult -Success $true -Message $message -OutputText $message
        return
    }

    $startParameters = @{
        ResourceGroupName = $ResourceGroupName
        InstanceName = $ManagedInstanceName
        Name = $DatabaseName
        StorageContainerUri = $StorageContainerUri
        StorageContainerIdentity = $StorageContainerIdentity
        ErrorAction = 'Stop'
    }

    if ($StorageContainerSasToken) {
        Write-Warning 'StorageContainerSasToken was provided but will be ignored because online LRS start requires ManagedIdentity.'
    }

    if ($WhatIfStart) {
        $startParameters['WhatIf'] = $true
    }

    Write-WorkerTrace ("Submitting Start-AzSqlInstanceDatabaseLogReplay for '{0}' on '{1}'." -f $DatabaseName, $ManagedInstanceName)
    $result = Start-AzSqlInstanceDatabaseLogReplay @startParameters
    Write-WorkerTrace 'Start-AzSqlInstanceDatabaseLogReplay returned control to the worker.'
    $outputText = ($result | Out-String).Trim()
    $message = if ($WhatIfStart) { 'WhatIf submission validated.' } else { 'LRS start submitted successfully.' }
    Write-WorkerResult -Success $true -Message $message -OutputText $outputText
} catch {
    Write-WorkerTrace ("Worker failed: {0}" -f $_.Exception.Message)
    $errorText = $_ | Out-String
    Write-WorkerResult -Success $false -Message $_.Exception.Message -OutputText $errorText
    throw
}