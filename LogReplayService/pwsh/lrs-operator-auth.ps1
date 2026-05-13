# Shared operator-side authentication and token diagnostics
# helpers used by the multi-DB LRS wrappers (offline + online).
#
# Operator auth modes supported:
#   ExistingContext              - reuse whatever Get-AzContext already returns (default)
#   Interactive                  - Connect-AzAccount with browser/device flow
#   EntraUser                    - Connect-AzAccount -AccountId <upn> (interactive)
#   ServicePrincipal             - Connect-AzAccount -ServicePrincipal (secret or cert)
#   UserAssignedManagedIdentity  - Connect-AzAccount -Identity -AccountId <client-id>
#   SystemAssignedManagedIdentity - Connect-AzAccount -Identity (host SAMI)
#
# Why this exists:
#   The SQL Managed Instance Log Replay Service has been observed to fail at
#   completeRestore with InternalServerError when the operator's AAD access
#   token is large (heavy group membership, optional claims, CAE claims,
#   group-based MI admin, etc.). Running the wrapper under a low-claim
#   identity (UAMI on an Azure VM is the leanest) reduces this risk.

Set-StrictMode -Version Latest

$script:OperatorAuthDefaults = @{
    JwtSoftWarnChars = 6000
    JwtHardWarnChars = 9000
}

function ConvertTo-OperatorAuthPlainTextToken {
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

function ConvertFrom-OperatorAuthBase64Url {
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

function Write-OperatorAuthEvent {
    param(
        [string]$EventLogPath,
        [string]$RunId,
        [string]$Source,
        [string]$Mode,
        [string]$Level = 'Info',
        [string]$Action,
        [string]$Message,
        [hashtable]$Data
    )

    if (-not $EventLogPath) { return }
    if (-not (Get-Command Write-MigrationEvent -ErrorAction SilentlyContinue)) { return }

    $params = @{
        EventLogPath = $EventLogPath
        RunId        = $RunId
        Source       = $Source
        Mode         = $Mode
        Level        = $Level
        Phase        = 'Auth'
        Action       = $Action
        Message      = $Message
    }

    if ($PSBoundParameters.ContainsKey('Data') -and $null -ne $Data) {
        $params['Data'] = $Data
    }

    Write-MigrationEvent @params
}

function Get-OperatorAuthPropertyValue {
    param(
        [object]$InputObject,
        [Parameter(Mandatory)]
        [string]$Name
    )

    if ($null -eq $InputObject) {
        return $null
    }

    if ($InputObject -is [System.Collections.IDictionary]) {
        foreach ($key in $InputObject.Keys) {
            if ([string]::Equals([string]$key, $Name, [System.StringComparison]::OrdinalIgnoreCase)) {
                return $InputObject[$key]
            }
        }
        return $null
    }

    # Iterate Properties via the enumerator rather than the indexer. The PSObject
    # property indexer can raise PropertyNotFoundException under Set-StrictMode -Latest
    # for some object shapes (notably JWT claims parsed by ConvertFrom-Json that
    # lack optional claims such as 'upn' for app-only tokens). Enumerating is safe.
    try {
        foreach ($prop in $InputObject.PSObject.Properties) {
            if ([string]::Equals($prop.Name, $Name, [System.StringComparison]::OrdinalIgnoreCase)) {
                return $prop.Value
            }
        }
    } catch {
        return $null
    }

    return $null
}

function Get-OperatorTokenDiagnostics {
    [CmdletBinding()]
    param(
        [int]$SoftWarnChars = $script:OperatorAuthDefaults.JwtSoftWarnChars,
        [int]$HardWarnChars = $script:OperatorAuthDefaults.JwtHardWarnChars
    )

    $diagnostics = [ordered]@{
        JwtChars     = $null
        IdType       = $null
        ObjectId     = $null
        AppId        = $null
        TenantId     = $null
        UPN          = $null
        GroupsCount  = 0
        HasGroupsOverflow = $false
        Audience     = $null
        Issuer       = $null
        RiskLevel    = 'Unknown'
        Warnings     = @()
        Error        = $null
    }

    if (-not (Get-Command Get-AzAccessToken -ErrorAction SilentlyContinue)) {
        $diagnostics.Error = 'Get-AzAccessToken is not available; cannot inspect operator JWT.'
        return [pscustomobject]$diagnostics
    }

    try {
        $tokenResponse = Get-AzAccessToken -ResourceUrl 'https://management.azure.com/' -ErrorAction Stop
    } catch {
        $diagnostics.Error = "Failed to acquire ARM access token: $($_.Exception.Message)"
        return [pscustomobject]$diagnostics
    }

    $accessToken = ConvertTo-OperatorAuthPlainTextToken -TokenValue $tokenResponse.Token
    if (-not $accessToken) {
        $diagnostics.Error = 'Acquired ARM access token was empty.'
        return [pscustomobject]$diagnostics
    }

    $diagnostics.JwtChars = $accessToken.Length

    $tokenParts = $accessToken -split '\.'
    if ($tokenParts.Count -ge 2) {
        try {
            $payloadJson = ConvertFrom-OperatorAuthBase64Url -Value $tokenParts[1]
            if ($payloadJson) {
                $claims = $payloadJson | ConvertFrom-Json -ErrorAction Stop
                $diagnostics.IdType   = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'idtyp')
                $diagnostics.ObjectId = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'oid')
                $diagnostics.AppId    = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'appid')
                $diagnostics.TenantId = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'tid')
                $diagnostics.UPN      = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'upn')
                $diagnostics.Audience = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'aud')
                $diagnostics.Issuer   = [string](Get-OperatorAuthPropertyValue -InputObject $claims -Name 'iss')

                if ($claims.PSObject.Properties['groups']) {
                    $diagnostics.GroupsCount = @($claims.groups).Count
                }
                if ($claims.PSObject.Properties['hasgroups'] -and $claims.hasgroups) {
                    $diagnostics.HasGroupsOverflow = $true
                }
            }
        } catch {
            $diagnostics.Warnings += "Could not parse JWT payload: $($_.Exception.Message)"
        }
    }

    if ($diagnostics.JwtChars -ge $HardWarnChars) {
        $diagnostics.RiskLevel = 'High'
        $diagnostics.Warnings += ("Operator JWT is {0} chars (>= {1}). Strongly correlated with InternalServerError on completeRestore. Consider running under a UserAssignedManagedIdentity or using -StorageAuthMode Sas." -f $diagnostics.JwtChars, $HardWarnChars)
    } elseif ($diagnostics.JwtChars -ge $SoftWarnChars) {
        $diagnostics.RiskLevel = 'Elevated'
        $diagnostics.Warnings += ("Operator JWT is {0} chars (>= {1}). Approaches sizes that have been correlated with InternalServerError on completeRestore." -f $diagnostics.JwtChars, $SoftWarnChars)
    } elseif ($null -ne $diagnostics.JwtChars) {
        $diagnostics.RiskLevel = 'Low'
    }

    if ($diagnostics.HasGroupsOverflow) {
        $diagnostics.Warnings += 'Token carries hasgroups=true (group claim overflow indicator). Many transitive groups may be in scope.'
    }

    return [pscustomobject]$diagnostics
}

function Resolve-OperatorPrincipalId {
    param([pscustomobject]$TokenDiagnostics)

    if ($TokenDiagnostics -and $TokenDiagnostics.ObjectId) {
        return $TokenDiagnostics.ObjectId
    }

    if (Get-Command Get-AzContext -ErrorAction SilentlyContinue) {
        $context = Get-AzContext -ErrorAction SilentlyContinue
        $account = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Account'
        $accountId = Get-OperatorAuthPropertyValue -InputObject $account -Name 'Id'
        $accountType = Get-OperatorAuthPropertyValue -InputObject $account -Name 'Type'
        if ($context -and $accountId) {
            # For SP/MI the Account.Id is the appId; for users it is UPN.
            if ($accountType -in @('ServicePrincipal', 'ManagedService') -and (Get-Command Get-AzADServicePrincipal -ErrorAction SilentlyContinue)) {
                try {
                    $sp = Get-AzADServicePrincipal -ApplicationId $accountId -ErrorAction Stop
                    if ($sp) { return $sp.Id }
                } catch { }
            }
            if (Get-Command Get-AzADUser -ErrorAction SilentlyContinue) {
                try {
                    $user = Get-AzADUser -UserPrincipalName $accountId -ErrorAction Stop
                    if ($user) { return $user.Id }
                } catch { }
            }
        }
    }

    return $null
}

function Connect-OperatorAuthIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('ExistingContext','Interactive','EntraUser','ServicePrincipal','UserAssignedManagedIdentity','SystemAssignedManagedIdentity')]
        [string]$Mode,
        [string]$TenantId,
        [string]$SubscriptionId,
        [string]$AccountUpn,
        [string]$ApplicationId,
        [securestring]$ClientSecret,
        [string]$CertificateThumbprint
    )

    if ($Mode -eq 'ExistingContext') {
        if (Get-Command Get-AzContext -ErrorAction SilentlyContinue) {
            $existing = Get-AzContext -ErrorAction SilentlyContinue
            if (-not $existing) {
                throw "OperatorAuthMode 'ExistingContext' was selected but no Az PowerShell context is available. Run Connect-AzAccount first or choose a different OperatorAuthMode."
            }
        }
        return
    }

    $connectArgs = @{}
    if ($TenantId)       { $connectArgs['Tenant']       = $TenantId }
    if ($SubscriptionId) { $connectArgs['Subscription'] = $SubscriptionId }

    switch ($Mode) {
        'Interactive' {
            # Default browser flow; falls back to device if no browser.
            Connect-AzAccount @connectArgs -ErrorAction Stop | Out-Null
        }
        'EntraUser' {
            if ($AccountUpn) { $connectArgs['AccountId'] = $AccountUpn }
            Connect-AzAccount @connectArgs -ErrorAction Stop | Out-Null
        }
        'ServicePrincipal' {
            if (-not $TenantId)      { throw "OperatorAuthMode 'ServicePrincipal' requires -OperatorTenantId." }
            if (-not $ApplicationId) { throw "OperatorAuthMode 'ServicePrincipal' requires -OperatorApplicationId." }
            $haveSecret = $null -ne $ClientSecret
            $haveCert   = -not [string]::IsNullOrWhiteSpace($CertificateThumbprint)
            if ($haveSecret -and $haveCert) {
                throw "OperatorAuthMode 'ServicePrincipal' requires exactly one of -OperatorClientSecret or -OperatorCertificateThumbprint, not both."
            }
            if (-not $haveSecret -and -not $haveCert) {
                throw "OperatorAuthMode 'ServicePrincipal' requires either -OperatorClientSecret or -OperatorCertificateThumbprint."
            }
            # SP modes specify Tenant explicitly; remove the duplicate from the splat to avoid a binding conflict.
            $spArgs = @{}
            if ($SubscriptionId) { $spArgs['Subscription'] = $SubscriptionId }
            if ($haveSecret) {
                $cred = [pscredential]::new($ApplicationId, $ClientSecret)
                Connect-AzAccount -ServicePrincipal -Tenant $TenantId -Credential $cred @spArgs -ErrorAction Stop | Out-Null
            } else {
                Connect-AzAccount -ServicePrincipal -Tenant $TenantId -ApplicationId $ApplicationId -CertificateThumbprint $CertificateThumbprint @spArgs -ErrorAction Stop | Out-Null
            }
        }
        'UserAssignedManagedIdentity' {
            if (-not $ApplicationId) { throw "OperatorAuthMode 'UserAssignedManagedIdentity' requires -OperatorApplicationId (the UAMI client ID)." }
            Connect-AzAccount -Identity -AccountId $ApplicationId @connectArgs -ErrorAction Stop | Out-Null
        }
        'SystemAssignedManagedIdentity' {
            try {
                Connect-AzAccount -Identity @connectArgs -ErrorAction Stop | Out-Null
            } catch {
                throw "OperatorAuthMode 'SystemAssignedManagedIdentity' failed. This mode requires the wrapper to run on Azure compute (VM/VMSS/Arc/ACI/App Service/Container App/Function) with system-assigned managed identity enabled. Underlying error: $($_.Exception.Message)"
            }
        }
    }

    if ($SubscriptionId -and (Get-Command Set-AzContext -ErrorAction SilentlyContinue)) {
        $setArgs = @{ Subscription = $SubscriptionId }
        if ($TenantId) { $setArgs['Tenant'] = $TenantId }
        Set-AzContext @setArgs -ErrorAction Stop | Out-Null
    }
}

function Resolve-OperatorSubscriptionSelection {
    <#
    .SYNOPSIS
        Ensures an Az PowerShell context has a subscription selected, prompting interactively when ambiguous.
    .DESCRIPTION
        Multi-subscription tenants (the common case) require an explicit subscription pick;
        otherwise downstream resource-group / managed-instance lookups fail with confusing
        "ResourceGroupNotFound" errors against whatever sub Az happened to pick. This helper
        is called after Connect-OperatorAuthIdentity. If a subscription is already selected
        in the current context, it is returned as-is. Otherwise Get-AzSubscription is called
        (scoped to TenantId when provided), and:
            - 0 results : throws with guidance.
            - 1 result  : auto-selects and logs.
            - >1 results: prompts the operator to pick one (by number).
    #>
    [CmdletBinding()]
    param(
        [string]$TenantId
    )

    if (-not (Get-Command Get-AzContext -ErrorAction SilentlyContinue)) {
        return $null
    }

    $context = Get-AzContext -ErrorAction SilentlyContinue
    $contextSubscription = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Subscription'
    $contextSubscriptionId = Get-OperatorAuthPropertyValue -InputObject $contextSubscription -Name 'Id'
    if ($context -and $contextSubscriptionId) {
        return [string]$contextSubscriptionId
    }

    if (-not (Get-Command Get-AzSubscription -ErrorAction SilentlyContinue)) {
        throw "No subscription is selected on the current Az PowerShell context and Get-AzSubscription is unavailable. Re-run with -SubscriptionId (or -OperatorSubscriptionId) explicitly set."
    }

    $listArgs = @{ ErrorAction = 'Stop' }
    if ($TenantId) { $listArgs['TenantId'] = $TenantId }

    try {
        $subs = @(Get-AzSubscription @listArgs)
    } catch {
        throw "Failed to enumerate subscriptions for the operator identity. Underlying error: $($_.Exception.Message). Re-run with -SubscriptionId explicitly set to bypass enumeration."
    }

    $subs = @($subs | Where-Object { $_ -and $_.State -eq 'Enabled' })
    if ($subs.Count -eq 0) {
        $tenantMessage = if ($TenantId) { " in tenant $TenantId" } else { '' }
        throw "No enabled subscriptions are visible to the operator identity$tenantMessage. Re-run with -SubscriptionId explicitly set, or grant the operator identity access to at least one subscription."
    }

    if ($subs.Count -eq 1) {
        $only = $subs[0]
        Write-Host ("Subscription auto-selected (only one visible): {0} ({1})" -f $only.Name, $only.Id) -ForegroundColor Cyan
        Set-AzContext -Subscription $only.Id -Tenant $only.TenantId -ErrorAction Stop | Out-Null
        return [string]$only.Id
    }

    Write-Host ''
    Write-Host '--- Subscription selection required ---' -ForegroundColor Yellow
    Write-Host 'No subscription was supplied and the operator identity can see more than one. Pick one to scope this run:' -ForegroundColor Yellow
    for ($i = 0; $i -lt $subs.Count; $i++) {
        Write-Host ("  [{0}] {1}  ({2})  tenant={3}" -f ($i + 1), $subs[$i].Name, $subs[$i].Id, $subs[$i].TenantId)
    }

    $selected = $null
    while (-not $selected) {
        $answer = Read-Host ("Enter selection 1-{0} (or full subscription ID)" -f $subs.Count)
        if ([string]::IsNullOrWhiteSpace($answer)) { continue }

        $trimmed = $answer.Trim()
        $asInt = 0
        if ([int]::TryParse($trimmed, [ref]$asInt) -and $asInt -ge 1 -and $asInt -le $subs.Count) {
            $selected = $subs[$asInt - 1]
            break
        }

        $match = $subs | Where-Object { $_.Id -eq $trimmed } | Select-Object -First 1
        if ($match) {
            $selected = $match
            break
        }

        Write-Host "Invalid selection. Enter a number from the list or paste a full subscription ID." -ForegroundColor Red
    }

    Write-Host ("Subscription selected: {0} ({1})" -f $selected.Name, $selected.Id) -ForegroundColor Cyan
    Set-AzContext -Subscription $selected.Id -Tenant $selected.TenantId -ErrorAction Stop | Out-Null
    return [string]$selected.Id
}

function Initialize-OperatorAuthContext {
    <#
    .SYNOPSIS
        Connects the operator identity and measures token risk.
    .DESCRIPTION
        Returns a hashtable describing the resolved operator auth state. Use this
        early in a wrapper script (before any Azure control-plane work) so that
        any auth misconfiguration fails fast with a clear message and is recorded
        in the wrapper-events log.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('ExistingContext','Interactive','EntraUser','ServicePrincipal','UserAssignedManagedIdentity','SystemAssignedManagedIdentity')]
        [string]$Mode,

        [string]$TenantId,
        [string]$SubscriptionId,
        [string]$AccountUpn,
        [string]$ApplicationId,
        [securestring]$ClientSecret,
        [string]$CertificateThumbprint,

        [switch]$SkipTokenSizeCheck,

        [string]$EventLogPath,
        [string]$RunId,
        [string]$EventSource,
        [string]$EventMode = 'Operator',
        [string]$ReportDir
    )

    Write-Host ''
    Write-Host '--- Operator authentication ---' -ForegroundColor Cyan
    Write-Host ("Mode: {0}" -f $Mode)

    Connect-OperatorAuthIdentity `
        -Mode $Mode `
        -TenantId $TenantId `
        -SubscriptionId $SubscriptionId `
        -AccountUpn $AccountUpn `
        -ApplicationId $ApplicationId `
        -ClientSecret $ClientSecret `
        -CertificateThumbprint $CertificateThumbprint

    # Multi-subscription tenants are the norm. If the caller did not pin a subscription,
    # resolve one now (auto-select when unambiguous, prompt when not) so downstream RG/MI
    # lookups don't silently target the wrong subscription.
    $resolvedSubscriptionId = Resolve-OperatorSubscriptionSelection -TenantId $TenantId
    if ($resolvedSubscriptionId) { $SubscriptionId = $resolvedSubscriptionId }

    $context = Get-AzContext -ErrorAction SilentlyContinue
    if ($context) {
        $contextAccount = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Account'
        $contextTenant = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Tenant'
        $contextSubscription = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Subscription'
        $accountIdValue = Get-OperatorAuthPropertyValue -InputObject $contextAccount -Name 'Id'
        $accountTypeValue = Get-OperatorAuthPropertyValue -InputObject $contextAccount -Name 'Type'
        $tenantIdValue = Get-OperatorAuthPropertyValue -InputObject $contextTenant -Name 'Id'
        $subIdValue = Get-OperatorAuthPropertyValue -InputObject $contextSubscription -Name 'Id'
        $subNameValue = Get-OperatorAuthPropertyValue -InputObject $contextSubscription -Name 'Name'

        $accountId   = if ($accountIdValue)   { [string]$accountIdValue }   else { '<unknown>' }
        $accountType = if ($accountTypeValue) { [string]$accountTypeValue } else { '<unknown>' }
        $tenantText  = if ($tenantIdValue)    { [string]$tenantIdValue }    else { '<none>' }
        $subId       = if ($subIdValue)       { [string]$subIdValue }       else { '<none>' }
        $subName     = if ($subNameValue)     { [string]$subNameValue }     else { '<none>' }
        Write-Host ("Account: {0} ({1})" -f $accountId, $accountType)
        Write-Host ("Tenant : {0}" -f $tenantText)
        Write-Host ("Sub    : {0} ({1})" -f $subId, $subName)
    }

    $diagnostics = $null
    if (-not $SkipTokenSizeCheck) {
        Write-Host ''
        Write-Host '--- Operator JWT diagnostics ---' -ForegroundColor Green
        $diagnostics = Get-OperatorTokenDiagnostics
        if ($diagnostics) {
            $sizeText = if ($null -ne $diagnostics.JwtChars) { "$($diagnostics.JwtChars) chars" } else { 'unknown' }
            $idText   = if ($diagnostics.IdType) { $diagnostics.IdType } else { 'unknown' }
            $riskColor = switch ($diagnostics.RiskLevel) {
                'High'     { 'Red' }
                'Elevated' { 'Yellow' }
                'Low'      { 'Green' }
                default    { 'Gray' }
            }
            Write-Host ("  JWT size : {0}" -f $sizeText) -ForegroundColor Green
            Write-Host ("  Identity : idtyp={0}, groups={1}, groupsOverflow={2}" -f $idText, $diagnostics.GroupsCount, $diagnostics.HasGroupsOverflow) -ForegroundColor Green
            Write-Host ("  Risk     : {0}" -f $diagnostics.RiskLevel) -ForegroundColor $riskColor
            if ($diagnostics.Error) {
                Write-Warning $diagnostics.Error
            }
            foreach ($w in @($diagnostics.Warnings)) {
                if ($w) { Write-Warning $w }
            }
        }
    } else {
        Write-Host ''
        Write-Host '--- Operator JWT diagnostics ---' -ForegroundColor Green
        Write-Host '  Skipped (-SkipTokenSizeCheck).' -ForegroundColor Yellow
    }

    $principalId = Resolve-OperatorPrincipalId -TokenDiagnostics $diagnostics

    $effectiveTenantValue = Get-OperatorAuthPropertyValue -InputObject (Get-OperatorAuthPropertyValue -InputObject $context -Name 'Tenant') -Name 'Id'
    $effectiveSubValue = Get-OperatorAuthPropertyValue -InputObject (Get-OperatorAuthPropertyValue -InputObject $context -Name 'Subscription') -Name 'Id'
    $effectiveTenant = if ($effectiveTenantValue) { [string]$effectiveTenantValue } else { $TenantId }
    $effectiveSub    = if ($effectiveSubValue)    { [string]$effectiveSubValue }    else { $SubscriptionId }

    $authAccount = Get-OperatorAuthPropertyValue -InputObject $context -Name 'Account'
    $authAccountId = Get-OperatorAuthPropertyValue -InputObject $authAccount -Name 'Id'
    $authAccountType = Get-OperatorAuthPropertyValue -InputObject $authAccount -Name 'Type'

    $authState = [pscustomobject][ordered]@{
        Mode               = $Mode
        TenantId           = $effectiveTenant
        SubscriptionId     = $effectiveSub
        AccountId          = if ($authAccountId) { [string]$authAccountId } else { $null }
        AccountType        = if ($authAccountType) { [string]$authAccountType } else { $null }
        PrincipalId        = $principalId
        ApplicationId      = $ApplicationId
        TokenDiagnostics   = $diagnostics
    }

    $eventLevel = if ($diagnostics -and $diagnostics.RiskLevel -in @('High', 'Elevated')) { 'Warning' } else { 'Info' }

    Write-OperatorAuthEvent `
        -EventLogPath $EventLogPath `
        -RunId $RunId `
        -Source $EventSource `
        -Mode $EventMode `
        -Level $eventLevel `
        -Action 'OperatorAuthInitialized' `
        -Message ("Operator auth initialized using mode '{0}'." -f $Mode) `
        -Data @{
            mode             = $Mode
            tenantId         = $effectiveTenant
            subscriptionId   = $effectiveSub
            accountId        = $authState.AccountId
            accountType      = $authState.AccountType
            principalId      = $principalId
            applicationId    = $ApplicationId
            jwtChars         = if ($diagnostics) { $diagnostics.JwtChars } else { $null }
            idType           = if ($diagnostics) { $diagnostics.IdType } else { $null }
            groupsCount      = if ($diagnostics) { $diagnostics.GroupsCount } else { $null }
            tokenRisk        = if ($diagnostics) { $diagnostics.RiskLevel } else { $null }
            tokenWarnings    = if ($diagnostics) { @($diagnostics.Warnings) } else { @() }
        }

    return $authState
}
