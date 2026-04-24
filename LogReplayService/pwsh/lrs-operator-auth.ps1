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
#   The SQL Managed Instance Log Replay Service can fail at completeRestore with
#   InternalServerError when the operator's AAD access token is large (heavy
#   group membership, optional claims, CAE claims, group-based MI admin, etc.).
#   The fix is in an upcoming SQL Server CU; in the meantime, running the
#   wrapper under a low-claim identity (UAMI on an Azure VM is the leanest)
#   avoids the buffer that the engine truncates.

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
                $diagnostics.IdType   = [string]$claims.idtyp
                $diagnostics.ObjectId = [string]$claims.oid
                $diagnostics.AppId    = [string]$claims.appid
                $diagnostics.TenantId = [string]$claims.tid
                $diagnostics.UPN      = [string]$claims.upn
                $diagnostics.Audience = [string]$claims.aud
                $diagnostics.Issuer   = [string]$claims.iss

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
        $diagnostics.Warnings += ("Operator JWT is {0} chars (>= {1}). Strongly correlated with the SQL engine InternalServerError defect on completeRestore. Consider running under a UserAssignedManagedIdentity or using -StorageAuthMode Sas." -f $diagnostics.JwtChars, $HardWarnChars)
    } elseif ($diagnostics.JwtChars -ge $SoftWarnChars) {
        $diagnostics.RiskLevel = 'Elevated'
        $diagnostics.Warnings += ("Operator JWT is {0} chars (>= {1}). Approaches sizes that have triggered the SQL engine InternalServerError defect on completeRestore." -f $diagnostics.JwtChars, $SoftWarnChars)
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
        if ($context -and $context.Account -and $context.Account.Id) {
            # For SP/MI the Account.Id is the appId; for users it is UPN.
            if ($context.Account.Type -in @('ServicePrincipal', 'ManagedService') -and (Get-Command Get-AzADServicePrincipal -ErrorAction SilentlyContinue)) {
                try {
                    $sp = Get-AzADServicePrincipal -ApplicationId $context.Account.Id -ErrorAction Stop
                    if ($sp) { return $sp.Id }
                } catch { }
            }
            if (Get-Command Get-AzADUser -ErrorAction SilentlyContinue) {
                try {
                    $user = Get-AzADUser -UserPrincipalName $context.Account.Id -ErrorAction Stop
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

function Test-OperatorRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$PrincipalId,
        [Parameter(Mandatory)]
        [hashtable[]]$Required
    )

    if (-not (Get-Command Get-AzRoleAssignment -ErrorAction SilentlyContinue)) {
        return @{
            Available = $false
            Missing   = @()
            Present   = @()
            Error     = "Az.Resources is not available; cannot evaluate RBAC preflight. Install Az.Resources to enable role checks."
        }
    }

    $present = @()
    $missing = @()

    foreach ($req in $Required) {
        $roleName = [string]$req['RoleDefinitionName']
        $scope    = [string]$req['Scope']
        $reason   = [string]$req['Reason']

        if (-not $roleName -or -not $scope) { continue }

        $found = $null
        try {
            $found = Get-AzRoleAssignment -ObjectId $PrincipalId -RoleDefinitionName $roleName -Scope $scope -ErrorAction Stop |
                Where-Object { $_.Scope -eq $scope -or $scope.StartsWith($_.Scope, [System.StringComparison]::OrdinalIgnoreCase) } |
                Select-Object -First 1
        } catch {
            $missing += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason; LookupError = $_.Exception.Message }
            continue
        }

        if ($found) {
            $present += @{ RoleDefinitionName = $roleName; Scope = $scope; Reason = $reason }
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

function Grant-OperatorRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$PrincipalId,
        [Parameter(Mandatory)]
        [hashtable[]]$Missing
    )

    if (-not (Get-Command New-AzRoleAssignment -ErrorAction SilentlyContinue)) {
        throw "Cannot auto-grant operator roles: New-AzRoleAssignment is not available. Install Az.Resources."
    }

    $applied = @()

    foreach ($req in $Missing) {
        $roleName = [string]$req['RoleDefinitionName']
        $scope    = [string]$req['Scope']

        try {
            New-AzRoleAssignment -ObjectId $PrincipalId -RoleDefinitionName $roleName -Scope $scope -ErrorAction Stop | Out-Null
            $applied += @{ RoleDefinitionName = $roleName; Scope = $scope }
        } catch {
            $msg = $_.Exception.Message
            if ($msg -match 'AuthorizationFailed|does not have authorization|RBAC') {
                throw ("Auto-grant of role '{0}' at scope '{1}' was denied. The principal running this wrapper needs 'Role Based Access Control Administrator', 'User Access Administrator', or 'Owner' at that scope. Either re-run with sufficient rights, grant the role manually, or omit -AutoGrantOperatorRoles. Underlying error: {2}" -f $roleName, $scope, $msg)
            }
            throw ("Failed to grant role '{0}' at scope '{1}'. {2}" -f $roleName, $scope, $msg)
        }
    }

    # Allow time for RBAC propagation before downstream calls evaluate the grant.
    if ($applied.Count -gt 0) {
        Start-Sleep -Seconds 10
    }

    return $applied
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
        [switch]$AutoGrantRoles,
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

    $context = Get-AzContext -ErrorAction SilentlyContinue
    if ($context) {
        Write-Host ("Account: {0} ({1})" -f $context.Account.Id, $context.Account.Type)
        Write-Host ("Tenant : {0}" -f $context.Tenant.Id)
        Write-Host ("Sub    : {0} ({1})" -f $context.Subscription.Id, $context.Subscription.Name)
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
    if ($RequiredRoleAssignments -and $RequiredRoleAssignments.Count -gt 0) {
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
                Write-Host ("  [OK]      {0}  @  {1}" -f $p.RoleDefinitionName, $p.Scope)
            }
            foreach ($m in $rbac.Missing) {
                Write-Host ("  [MISSING] {0}  @  {1}" -f $m.RoleDefinitionName, $m.Scope) -ForegroundColor Yellow
            }

            if ($rbac.Missing.Count -gt 0) {
                if ($AutoGrantRoles) {
                    Write-Host 'AutoGrantOperatorRoles is enabled; attempting to grant missing roles...' -ForegroundColor Cyan
                    $applied = Grant-OperatorRoleAssignments -PrincipalId $principalId -Missing $rbac.Missing
                    foreach ($a in $applied) {
                        Write-Host ("  [GRANTED] {0}  @  {1}" -f $a.RoleDefinitionName, $a.Scope) -ForegroundColor Green
                    }
                    $rbac['Applied'] = $applied

                    if ($ReportDir) {
                        try {
                            $auditPath = Join-Path -Path $ReportDir -ChildPath 'auth-grants-applied.json'
                            $audit = @{
                                principalId = $principalId
                                appliedAt   = (Get-Date).ToUniversalTime().ToString('o')
                                grants      = $applied
                            }
                            $audit | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $auditPath -Encoding UTF8
                        } catch {
                            Write-Warning "Failed to write auth-grants-applied.json: $($_.Exception.Message)"
                        }
                    }
                } else {
                    $missingList = ($rbac.Missing | ForEach-Object { "    - {0} @ {1}  ({2})" -f $_.RoleDefinitionName, $_.Scope, $_.Reason }) -join "`n"
                    throw @"
Operator RBAC preflight failed. The current operator identity is missing the following role assignments:
$missingList

Re-run the wrapper with -AutoGrantOperatorRoles to attempt automatic grant (requires the executing user to hold 'Role Based Access Control Administrator', 'User Access Administrator', or 'Owner' on the relevant scopes), or grant the roles manually before re-running.
"@
                }
            }
        }
    }

    $effectiveTenant = if ($context) { [string]$context.Tenant.Id } else { $TenantId }
    $effectiveSub    = if ($context) { [string]$context.Subscription.Id } else { $SubscriptionId }

    $authState = [pscustomobject][ordered]@{
        Mode               = $Mode
        TenantId           = $effectiveTenant
        SubscriptionId     = $effectiveSub
        AccountId          = if ($context) { [string]$context.Account.Id } else { $null }
        AccountType        = if ($context) { [string]$context.Account.Type } else { $null }
        PrincipalId        = $principalId
        ApplicationId      = $ApplicationId
        TokenDiagnostics   = $diagnostics
        RoleCheck          = $rbac
    }

    Write-OperatorAuthEvent `
        -EventLogPath $EventLogPath `
        -RunId $RunId `
        -Source $EventSource `
        -Mode $EventMode `
        -Level (if ($diagnostics -and $diagnostics.RiskLevel -eq 'High') { 'Warning' } elseif ($diagnostics -and $diagnostics.RiskLevel -eq 'Elevated') { 'Warning' } else { 'Info' }) `
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
            grantedRoles     = if ($rbac -and $rbac['Applied']) { @($rbac['Applied'] | ForEach-Object { @{ role = $_['RoleDefinitionName']; scope = $_['Scope'] } }) } else { @() }
            autoGrantEnabled = [bool]$AutoGrantRoles
        }

    return $authState
}

function Get-OperatorRoleRequirementSet {
    <#
    .SYNOPSIS
        Builds the default required-role list for the LRS wrappers.
    .DESCRIPTION
        Returns an array of @{ RoleDefinitionName; Scope; Reason } entries
        suitable for Initialize-OperatorAuthContext. Always requires SQL
        Managed Instance Contributor at the resource group scope. Adds
        Storage Blob Data Reader at the storage account scope only when
        a storage account is configured AND storage auth is not SAS.
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
            RoleDefinitionName = 'Storage Blob Data Reader'
            Scope              = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName"
            Reason             = 'Required for the operator identity to enumerate backup blobs without SAS.'
        }
    }

    return $required
}
