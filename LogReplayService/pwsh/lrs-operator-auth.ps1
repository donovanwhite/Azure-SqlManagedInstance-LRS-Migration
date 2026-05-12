# Shared operator-side authentication, token diagnostics, and RBAC preflight
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

function Test-OperatorRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$PrincipalId,
        [Parameter(Mandatory)]
        [hashtable[]]$Required
    )

    if (-not (Get-Command Invoke-AzRestMethod -ErrorAction SilentlyContinue)) {
        return @{
            Available = $false
            Missing   = @()
            Present   = @()
            Error     = "Az.Accounts is not available; cannot evaluate RBAC preflight. Install Az.Accounts to enable role checks."
        }
    }

    # Resolve role definition IDs once per role name. Role assignments returned by
    # ARM only carry roleDefinitionId, not the friendly name, so we match by GUID.
    # We do this via ARM REST as well to avoid Microsoft Graph dependencies that
    # Get-AzRoleDefinition / Get-AzRoleAssignment can implicitly require when the
    # current context is a System/User Assigned Managed Identity without Graph
    # permissions (a common failure mode that silently returns empty results).
    $roleDefIdCache = @{}
    function Resolve-OperatorRoleDefinitionId {
        param([string]$RoleName, [string]$Scope)
        if ($roleDefIdCache.ContainsKey($RoleName)) { return $roleDefIdCache[$RoleName] }

        $encodedFilter = [Uri]::EscapeDataString("roleName eq '$RoleName'")
        $defPath = "$Scope/providers/Microsoft.Authorization/roleDefinitions?api-version=2022-04-01&`$filter=$encodedFilter"
        try {
            $resp = Invoke-AzRestMethod -Method GET -Path $defPath -ErrorAction Stop
        } catch {
            return $null
        }
        if (-not $resp -or $resp.StatusCode -ge 400) { return $null }
        try {
            $body = $resp.Content | ConvertFrom-Json -ErrorAction Stop
        } catch {
            return $null
        }
        $defId = $null
        foreach ($item in @($body.value)) {
            if ($item.properties.roleName -eq $RoleName) {
                $defId = [string]$item.id
                break
            }
        }
        $roleDefIdCache[$RoleName] = $defId
        return $defId
    }

    # Fetch the operator's full effective assignment set ONCE, anchored at the
    # subscription extracted from the first requirement's scope. Using
    # `assignedTo('{principalId}')` (without atScope()) at the subscription scope
    # returns assignments inherited from parent management groups AND assignments
    # at any child scope (RG, resource, sub-resource). This lets us accept a
    # requirement when the operator has the role at ANY scope along the
    # ancestor/descendant chain of the target scope -- which is what customers
    # actually do (they grant at the MI, RG, sub, or MG level interchangeably).
    function Get-OperatorAssignmentAnchorScope {
        param([string]$Scope)
        if (-not $Scope) { return $null }
        if ($Scope -match '^(?i)(/subscriptions/[0-9a-f-]+)') { return $Matches[1] }
        # Fall back to the supplied scope (e.g. management-group scope) -- the
        # ARM list endpoint accepts any scope.
        return $Scope
    }

    function Test-OperatorScopeCovers {
        # Returns $true if $AssignmentScope is an ancestor of, equal to, or a
        # descendant of $TargetScope. Comparison is case-insensitive and ignores
        # trailing slashes.
        param([string]$AssignmentScope, [string]$TargetScope)
        if (-not $AssignmentScope -or -not $TargetScope) { return $false }
        $a = $AssignmentScope.TrimEnd('/')
        $t = $TargetScope.TrimEnd('/')
        if ([string]::Equals($a, $t, [System.StringComparison]::OrdinalIgnoreCase)) { return $true }
        if ($t.StartsWith($a + '/', [System.StringComparison]::OrdinalIgnoreCase)) { return $true }  # assignment is ancestor of target
        if ($a.StartsWith($t + '/', [System.StringComparison]::OrdinalIgnoreCase)) { return $true }  # assignment is descendant of target
        return $false
    }

    $assignmentCache = @{}
    function Get-OperatorAssignmentsForAnchor {
        param([string]$AnchorScope)
        if ($assignmentCache.ContainsKey($AnchorScope)) { return $assignmentCache[$AnchorScope] }

        $encoded = [Uri]::EscapeDataString("assignedTo('$PrincipalId')")
        $path = "$AnchorScope/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01&`$filter=$encoded"
        $collected = New-Object System.Collections.ArrayList
        $err = $null
        try {
            $resp = Invoke-AzRestMethod -Method GET -Path $path -ErrorAction Stop
            if (-not $resp -or $resp.StatusCode -ge 400) {
                $err = if ($resp) { "HTTP $($resp.StatusCode): $($resp.Content)" } else { 'no response' }
            } else {
                $body = $resp.Content | ConvertFrom-Json -ErrorAction Stop
                foreach ($a in @($body.value)) {
                    [void]$collected.Add($a)
                }
            }
        } catch {
            $err = $_.Exception.Message
        }
        $result = @{ Assignments = $collected.ToArray(); Error = $err }
        $assignmentCache[$AnchorScope] = $result
        return $result
    }

    $present = @()
    $missing = @()

    foreach ($req in $Required) {
        $roleName = [string]$req['RoleDefinitionName']
        $scope    = [string]$req['Scope']
        $reason   = [string]$req['Reason']

        if (-not $roleName -or -not $scope) { continue }

        # A requirement can be satisfied by any of an explicit list of roles
        # (e.g. Storage Blob Data Contributor is also satisfied by Storage Blob
        # Data Owner). If AcceptableRoleDefinitionNames is not provided, fall
        # back to the single primary role.
        $acceptableNames = @()
        if ($req.ContainsKey('AcceptableRoleDefinitionNames') -and $req['AcceptableRoleDefinitionNames']) {
            $acceptableNames = @($req['AcceptableRoleDefinitionNames'] | Where-Object { $_ })
        }
        if ($acceptableNames.Count -eq 0) {
            $acceptableNames = @($roleName)
        }

        $acceptableGuids = @()
        $lookupErrors = @()
        foreach ($candidateName in $acceptableNames) {
            $cid = Resolve-OperatorRoleDefinitionId -RoleName $candidateName -Scope $scope
            if ($cid) {
                $acceptableGuids += (($cid -split '/')[ -1 ])
            } else {
                $lookupErrors += "Could not resolve role definition id for '$candidateName' via ARM at or above scope '$scope'."
            }
        }

        if ($acceptableGuids.Count -eq 0) {
            $missing += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason; LookupError = ($lookupErrors -join ' ') }
            continue
        }

        $anchor = Get-OperatorAssignmentAnchorScope -Scope $scope
        if (-not $anchor) {
            $missing += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason; LookupError = "Could not derive a subscription anchor scope from '$scope'." }
            continue
        }

        $bundle = Get-OperatorAssignmentsForAnchor -AnchorScope $anchor
        if ($bundle.Error) {
            $missing += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason; LookupError = $bundle.Error }
            continue
        }

        $matchedName = $null
        $matchedScope = $null
        foreach ($a in $bundle.Assignments) {
            $props = $a.properties
            if (-not $props) { continue }
            $assignedPrincipalId = [string]$props.principalId
            # assignedTo() can include transitive (group-based) assignments where
            # principalId is a group; accept those regardless of direct principal match.
            if ($assignedPrincipalId -and $assignedPrincipalId -ne $PrincipalId -and ([string]$props.principalType) -notmatch '^(?i)group$') {
                # If principalId is set and is neither us nor a group, skip.
                # (assignedTo() should already constrain this, but be defensive.)
            }
            $assignmentScope = [string]$props.scope
            if (-not (Test-OperatorScopeCovers -AssignmentScope $assignmentScope -TargetScope $scope)) { continue }

            $rdId = [string]$props.roleDefinitionId
            $haveGuid = ($rdId -split '/')[ -1 ]
            for ($i = 0; $i -lt $acceptableGuids.Count; $i++) {
                if ([string]::Equals($acceptableGuids[$i], $haveGuid, [System.StringComparison]::OrdinalIgnoreCase)) {
                    $matchedName = $acceptableNames[$i]
                    $matchedScope = $assignmentScope
                    break
                }
            }
            if ($matchedName) { break }
        }

        if ($matchedName) {
            # Surface the role + the actual scope we matched at (may be a parent
            # or child of the requested target scope).
            $present += @{ RoleDefinitionName = $matchedName; Scope = $scope; MatchedScope = $matchedScope; Reason = $reason }
        } else {
            $missing += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason }
        }
    }

    return @{
        Available = $true
        Missing   = $missing
        Present   = $present
        Error     = $null
    }
}

function Initialize-OperatorAuthContext {
    <#
    .SYNOPSIS
        Connects the operator identity, measures token risk, and runs an RBAC preflight.
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

        [hashtable[]]$RequiredRoleAssignments,
        [switch]$SkipTokenSizeCheck,
        # Skip the RBAC preflight entirely. The operation will rely on the
        # downstream Azure control-plane call to surface authoritative auth
        # errors. Useful when customers grant permissions through complex
        # scope/group inheritance that the preflight cannot enumerate.
        [switch]$SkipOperatorRbacPreflight,
        # When set, a missing-role result causes a hard failure (legacy
        # behavior). When NOT set (default), a missing-role result is logged
        # as a warning so the wrapper proceeds and the real Azure call
        # produces the authoritative error message.
        [switch]$StrictOperatorRbacPreflight,

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
        $diagnostics = Get-OperatorTokenDiagnostics
        if ($diagnostics) {
            $sizeText = if ($null -ne $diagnostics.JwtChars) { "$($diagnostics.JwtChars) chars" } else { 'unknown' }
            $idText   = if ($diagnostics.IdType) { $diagnostics.IdType } else { 'unknown' }
            Write-Host ("Token  : {0}, idtyp={1}, groups={2}, risk={3}" -f $sizeText, $idText, $diagnostics.GroupsCount, $diagnostics.RiskLevel)
            foreach ($w in @($diagnostics.Warnings)) {
                if ($w) { Write-Warning $w }
            }
        }
    } else {
        Write-Host 'Token size check skipped (-SkipTokenSizeCheck).'
    }

    $principalId = Resolve-OperatorPrincipalId -TokenDiagnostics $diagnostics

    $rbac = $null
    if ($SkipOperatorRbacPreflight) {
        Write-Host ''
        Write-Host '--- RBAC preflight ---' -ForegroundColor Cyan
        Write-Host 'RBAC preflight skipped (-SkipOperatorRbacPreflight). Azure will enforce permissions on the actual control-plane calls.' -ForegroundColor Yellow
        $rbac = @{ Available = $false; Missing = @(); Present = @(); Error = 'Skipped by caller (-SkipOperatorRbacPreflight).' }
    }
    elseif ($RequiredRoleAssignments -and $RequiredRoleAssignments.Count -gt 0) {
        if (-not $principalId) {
            throw "Cannot run RBAC preflight: operator principal ID could not be resolved from the current Az context. Re-run with -OperatorAuthMode set explicitly, or install Az.Resources/Az.Accounts so the principal can be resolved."
        }

        Write-Host ''
        Write-Host '--- RBAC preflight ---' -ForegroundColor Cyan
        $rbac = Test-OperatorRoleAssignments -PrincipalId $principalId -Required $RequiredRoleAssignments

        if (-not $rbac.Available) {
            Write-Warning $rbac.Error
        } else {
            foreach ($p in $rbac.Present) {
                if ($p.MatchedScope -and -not [string]::Equals($p.MatchedScope, $p.Scope, [System.StringComparison]::OrdinalIgnoreCase)) {
                    Write-Host ("  [OK]      {0}  @  {1}   (inherited from {2})" -f $p.RoleDefinitionName, $p.Scope, $p.MatchedScope)
                } else {
                    Write-Host ("  [OK]      {0}  @  {1}" -f $p.RoleDefinitionName, $p.Scope)
                }
            }
            foreach ($m in $rbac.Missing) {
                Write-Host ("  [MISSING] {0}  @  {1}" -f $m.RoleDefinitionName, $m.Scope) -ForegroundColor Yellow
            }

            if ($rbac.Missing.Count -gt 0) {
                $missingList = ($rbac.Missing | ForEach-Object { "    - {0} @ {1}  ({2})" -f $_.RoleDefinitionName, $_.Scope, $_.Reason }) -join "`n"
                $message = @"
Operator RBAC preflight could not confirm one or more required role assignments for the current operator identity:
$missingList

This may be a false negative (e.g. role granted through nested groups, conditional access, or via a custom role) -- the preflight only inspects direct/inherited Azure RBAC assignments visible to the current identity.

If the operation fails with an authorization error, grant the missing roles to the operator identity (e.g. via 'az role assignment create' or 'New-AzRoleAssignment' run as a user holding 'Role Based Access Control Administrator', 'User Access Administrator', or 'Owner' at the relevant scope), then re-run the wrapper. The role assignment may be made at the resource, resource-group, subscription, or management-group scope -- whichever your environment standardises on.

To suppress this preflight entirely, pass -SkipOperatorRbacPreflight (or set the wrapper switch of the same name).
"@
                if ($StrictOperatorRbacPreflight) {
                    throw $message
                } else {
                    Write-Warning $message
                    Write-Host 'Continuing past RBAC preflight; the Azure control-plane call will be the authoritative authorization check. Use -StrictOperatorRbacPreflight to fail fast instead.' -ForegroundColor Yellow
                }
            }
        }
    }

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
        RoleCheck          = $rbac
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
            requiredRoles    = if ($RequiredRoleAssignments) { @($RequiredRoleAssignments | ForEach-Object { @{ role = $_['RoleDefinitionName']; scope = $_['Scope'] } }) } else { @() }
            missingRoles     = if ($rbac -and $rbac.Missing) { @($rbac.Missing | ForEach-Object { @{ role = $_['RoleDefinitionName']; scope = $_['Scope'] } }) } else { @() }
        }

    return $authState
}

function Get-OperatorRoleRequirementSet {
    <#
    .SYNOPSIS
        Builds the default required-role list for the LRS wrappers.
    .DESCRIPTION
        Returns an array of @{ RoleDefinitionName; Scope; Reason;
        AcceptableRoleDefinitionNames } entries suitable for
        Initialize-OperatorAuthContext. Always requires SQL Managed Instance
        Contributor at the resource group scope. Adds Storage Blob Data
        Contributor at the storage account scope only when a storage account
        is configured AND storage auth is not SAS. The operator needs write
        access because the transfer step creates the container (azcopy make)
        and uploads backup blobs; Storage Blob Data Owner is also accepted
        as a superset of Contributor.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$SubscriptionId,
        [Parameter(Mandatory)]
        [string]$ResourceGroupName,
        [string]$StorageAccountName,
        [string]$StorageAuthMode,
        [string[]]$Override
    )

    if ($Override -and $Override.Count -gt 0) {
        $rgScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName"
        return @(
            $Override | Where-Object { $_ } | ForEach-Object {
                @{ RoleDefinitionName = $_; Scope = $rgScope; Reason = 'OperatorRequiredRoles override' }
            }
        )
    }

    $rgScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName"
    $required = @(
        @{
            RoleDefinitionName = 'SQL Managed Instance Contributor'
            Scope              = $rgScope
            Reason             = 'Required to start, monitor, and complete LRS on the Managed Instance.'
        }
    )

    $usesStorageBlobAuth = $false
    if ($StorageAccountName) {
        $authNormalized = ($StorageAuthMode | ForEach-Object { if ($_) { $_.ToString().ToLowerInvariant() } else { '' } })
        if ($authNormalized -notin @('sas', 'sharedaccesssignature')) {
            $usesStorageBlobAuth = $true
        }
    }

    if ($usesStorageBlobAuth) {
        $required += @{
            RoleDefinitionName             = 'Storage Blob Data Contributor'
            AcceptableRoleDefinitionNames  = @('Storage Blob Data Contributor', 'Storage Blob Data Owner')
            Scope                          = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName"
            Reason                         = 'Required for the operator to create the LRS container and upload backup blobs (azcopy make + azcopy copy) without SAS. Storage Blob Data Owner also satisfies this.'
        }
    }

    return $required
}
