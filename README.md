# Log Replay Service Migration Wrappers

This repository provides PowerShell-based wrappers around Azure SQL Managed Instance Log Replay Service (LRS) for operators who need predictable, scriptable migrations without relying on portal-driven workflows.

It is designed for two main use cases:

- Flexible offline migrations across multiple SQL Server source instances and multiple databases in a single run.
- Flexible online migrations for multi-database groups that must be recovered to a transactionally consistent point in time.

It also emphasizes:

- Managed identity for the LRS restore path instead of SAS-based restore access.
- Automatic handling of MFA and claims-challenge reauthentication for Azure CLI and Az PowerShell control-plane calls.
- Operation in constrained environments where outbound HTTPS is available but opening additional inbound ports or using GUI-driven Azure Arc or Azure Migrate style workflows is not practical.

## Why this exists

Standard portal-led migration workflows are not always a fit for locked-down environments. In some estates:

- Only outbound connectivity is allowed.
- Additional inbound ports cannot be opened.
- Operators need repeatable script execution instead of interactive portal orchestration.
- Large migration waves require batch handling of many databases with explicit selection rules.
- Security teams prefer managed identity over long-lived SAS-based restore access.

These wrappers address that by using:

- AzCopy for backup upload.
- Azure CLI and Az PowerShell for control-plane access.
- SQL Managed Instance managed identity for the LRS data-read path.
- JSONL, JSON, and HTML reporting for traceability.

## What the wrappers do

### Offline wrapper

The offline wrapper uploads backup chains and completes guided restore orchestration.

Best fit:

- The backup chain is already available.
- You want to migrate many databases at once.
- You may need to include databases from multiple source instances in the same run.
- You need selection flexibility when the same database name exists on more than one source instance.

Key behavior:

- Discovers instance folders and database folders under the backup root.
- Supports `-SelectedInstanceNames` and `-SelectedDatabaseNames` filtering.
- Supports instance-qualified database selection such as `Instance\Database` or `Instance::Database` so you can target the intended source copy.
- Fails early if selected database names are ambiguous across instances unless they are explicitly qualified.
- Uploads backup chains quietly and produces migration-focused operator output.
- Uses managed identity for the LRS restore path.

### Online wrapper

The online wrapper starts a background uploader, starts or reuses online LRS restore state, monitors progress, and supports operator-driven or scheduled cutover.

Best fit:

- You need to keep restoring transaction logs while the source workload remains online.
- You want to cut over a group of related databases together.
- The database group must land at the same effective recovery point.

Key behavior:

- Intentionally supports exactly one source instance per run.
- Supports multiple databases within that instance.
- Tracks uploaded and restorable logs across the selected database set.
- At cutover, chooses the latest common valid recovery candidate so the selected database group remains transactionally consistent.
- Provides startup and steady-state status output suitable for long-running operator sessions.
- Distinguishes retryable LRS or service conditions from fatal local or runtime failures.

## Security and network model

This solution deliberately separates upload auth from restore auth.

Upload path:

- AzCopy uploads backups to Azure Storage.
- Upload auth can use Microsoft Entra ID through the current Azure CLI context, device login, or SAS when necessary.

Restore path:

- LRS always uses the managed identity of the target Azure SQL Managed Instance.
- The wrappers convert DFS-style upload URIs to the Blob endpoint expected by LRS while preserving the same per-database layout.

This model has practical security benefits:

- No need to embed broad SAS tokens for the restore workflow.
- Clear separation between operator upload permissions and managed instance read permissions.
- Better fit for least-privilege storage access.

This model also fits constrained networks better:

- The workflow is oriented around outbound connections to Azure services.
- It does not depend on opening extra inbound ports for agent-style migration tooling.
- It is useful where GUI-led migration approaches are operationally blocked by network policy.

## Repository structure

- `wrappers/`
  Operator entry points for offline and online migration orchestration.
- `pwsh/`
  Core transfer, guided restore, worker, configuration, and reporting scripts.
- `examples/`
  Copy-ready wrapper execution samples.
- `scripts/`
  Azure CLI helper snippets.
- `reports/`
  Per-run JSONL, JSON, and HTML artifacts.
- `logs/`
  Detailed worker and execution logs.
- `state/`
  Transfer-state tracking used by long-running migrations.

Primary entry points:

- `wrappers/wrapper-execution-multi-offline.ps1`
- `wrappers/wrapper-execution-multi-online.ps1`
- `examples/sample-wrapper-execution-commands.ps1`

## Prerequisites

- PowerShell 7 recommended.
- Azure CLI 2.42.0 or newer.
- Az.Sql PowerShell module 4.0.0 or newer.
- Access to Azure Storage for AzCopy uploads.
- SQL Managed Instance managed identity granted `Storage Blob Data Reader` or equivalent Read/List access on the target container or storage account.
- Operator identity granted upload permissions such as `Storage Blob Data Contributor` or `Storage Blob Data Owner` when using Entra-based uploads.
- Backup files arranged in the expected source layout.

Authentication behavior:


## Expected backup layout

The wrappers assume a source backup structure under `C:\SqlBackups` like this:

This layout is compatible with the instance/database/backup-type folder convention commonly used with Ola Hallengren maintenance jobs:

- https://ola.hallengren.com/

```text
C:\SqlBackups\<instance>\<database>\FULL
C:\SqlBackups\<instance>\<database>\DIFF
C:\SqlBackups\<instance>\<database>\LOG
```

Examples:

```text
C:\SqlBackups\SQLHOST01$INST01\SalesDb\FULL\SQLHOST01$INST01_SalesDb_FULL_20260415_145144.bak
C:\SqlBackups\SQLHOST01$INST01\SalesDb\LOG\SQLHOST01$INST01_SalesDb_LOG_20260415_145501.trn
```

Notes:

- `DIFF` is optional.
- `LOG` is used for transaction log discovery; common equivalents are also recognized.
- Backups are copied into a flat destination folder per database because LRS does not allow nested `FULL` or `LOG` subfolders in storage.
- Container and folder names must not contain the word `backup`.
- UNC paths are supported for `-BackupRootPath` (for example `\\fileserver\share\LRSBackup`). The account running the wrapper must already have read access to the share.

### Striped backup support

The transfer script detects multi-file (striped) FULL and DIFF backups automatically and uploads every stripe in the chosen set. Stripe detection is filename-based and evaluated in this order:

| Pattern              | Example                                              | Notes                                              |
|----------------------|------------------------------------------------------|----------------------------------------------------|
| `NofM`               | `SalesDb_FULL_20260415_1of3.bak`                     | Strongest signal; total stripe count is captured.  |
| `stripeN`            | `SalesDb_FULL_20260415_stripe1.bak`                  | Explicit `stripe` keyword.                         |
| `partN`              | `SalesDb_FULL_20260415_part1.bak`                    | Explicit `part` keyword.                           |
| `fileN`              | `SalesDb_FULL_20260415_file1.bak`                    | Explicit `file` keyword.                           |
| `TimestampedStripe`  | `SalesDb_20260415_120000_1.bak`                      | Stem must contain an 8-digit date and optional 4-6 digit time before the trailing index. High confidence. |
| `TrailingNumeric`    | `SalesDb_1.bak`, `SalesDb_2.bak`, `SalesDb_3.bak`    | Loose fallback. Only honoured when **two or more** sibling files share the same stem; a lone `SalesDb_5.bak` is treated as a single-file backup. |

Additional safeguards:

- Files that share a stem but were written more than 120 minutes apart are split into separate sets, so unrelated backups taken on different days are never merged.
- LOG (`.trn`) backups have always been uploaded as a list, so striped log backups continue to work.
- LRS on Managed Instance requires every stripe of a backup set to be present in the destination container — the wrapper now ensures this.
- If your stripe naming convention does not match any of the above, restripe to a single file (`BACKUP DATABASE ... TO DISK = '...full.bak' WITH COPY_ONLY, COMPRESSION;`) before transfer, or open an issue/PR to add the pattern.

## Default storage layout

When you provide `-StorageAccountName` without an explicit container URI, the transfer script creates a storage layout automatically:

- One blob container per source instance.
- One flat folder per database inside that container.

Example:

```text
Source:
C:\SqlBackups\SQLHOST01$INST01\SalesDb\FULL
C:\SqlBackups\SQLHOST01$INST01\SalesDb\LOG

Destination:
https://mystorageacct.dfs.core.windows.net/sqlhost01-inst01/SalesDb/
```

## Quick start

### 1. Authenticate to Azure

```powershell
az login
az account set --subscription '<subscription-id>'
Connect-AzAccount -Subscription '<subscription-id>'
```

### 2. Move to the wrappers folder

```powershell
Set-Location 'C:\AzureDataMigrationAssessments\LogReplayService\wrappers'
```

### 3. Run the appropriate wrapper

Use the examples in `examples/sample-wrapper-execution-commands.ps1` for your current environment.

## Offline migration patterns

### Migrate everything discovered under the backup root

```powershell
.\wrapper-execution-multi-offline.ps1 `
  -ResourceGroupName 'rg-sql-mi-migration' `
  -ManagedInstanceName 'mi-target-001' `
  -StorageAccountName 'mystorageacct' `
  -BackupRootPath 'C:\SqlBackups'
```

### Migrate only selected instances

```powershell
.\wrapper-execution-multi-offline.ps1 `
  -ResourceGroupName 'rg-sql-mi-migration' `
  -ManagedInstanceName 'mi-target-001' `
  -StorageAccountName 'mystorageacct' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedInstanceNames 'SQLHOST01$INST01','SQLHOST02$INST02'
```

### Migrate a mixed set of databases across instances

```powershell
.\wrapper-execution-multi-offline.ps1 `
  -AutoReauthenticate $true `
  -ResourceGroupName 'rg-sql-mi-migration' `
  -ManagedInstanceName 'mi-target-001' `
  -StorageAccountName 'mystorageacct' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedDatabaseNames 'SQLHOST01$INST01\SalesDb', 'SQLHOST01$INST01\TenantDb', 'SQLHOST02$INST02\SalesDb_Archive'
```

Selection rules:

- Plain database names apply globally across discovered instances.
- `Instance\Database` or `Instance::Database` targets a specific source instance copy.
- If every selected database is already fully qualified, `-SelectedInstanceNames` is optional.
- If the same database name exists under multiple selected instances and you do not qualify it, the wrapper fails fast so you do not accidentally restore the wrong source copy.

## Online migration patterns

Online mode is intentionally narrower than offline mode so cutover remains coherent.

Rules:

- Exactly one source instance per run.
- One or more selected databases from that instance.
- Cutover can be manual or scheduled.

### Start an online migration for a selected database group

```powershell
.\wrapper-execution-multi-online.ps1 `
  -ResourceGroupName 'rg-sql-mi-migration' `
  -ManagedInstanceName 'mi-target-001' `
  -StorageAccountName 'mystorageacct' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedInstanceNames 'SQLHOST01$INST01' `
  -SelectedDatabaseNames 'SalesDb', 'TenantDb' `
  -TransferPollSeconds 60 `
  -StatusIntervalMinutes 2
```

### Start an online migration with scheduled cutover

```powershell
.\wrapper-execution-multi-online.ps1 `
  -ResourceGroupName 'rg-sql-mi-migration' `
  -ManagedInstanceName 'mi-target-001' `
  -StorageAccountName 'mystorageacct' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedInstanceNames 'SQLHOST01$INST01' `
  -SelectedDatabaseNames 'SalesDb', 'TenantDb' `
  -TransferPollSeconds 60 `
  -StatusIntervalMinutes 2 `
  -ScheduledCutoverLocalTime '2026-04-15 15:15'
```

Operator notes:

- Online mode still uses the SQL Managed Instance managed identity for LRS restore access.
- The wrapper may prompt for an AzCopy device login because the uploader runs in a separate long-lived background process.
- The wrapper starts the background uploader, starts or reuses online LRS restore state, prints status snapshots, and waits for operator actions such as start monitoring, cutover, or quit.
- Database names in online mode do not need instance-qualified syntax because exactly one source instance is allowed per run.

## Parameter reference

Both wrappers (`wrapper-execution-multi-offline.ps1` and `wrapper-execution-multi-online.ps1`) share a common set of core, storage, and operator-auth parameters. The online wrapper adds a few parameters specific to its long-running transfer-and-cutover workflow.

### Core parameters (both wrappers)

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `-TenantId` | string | (auto) | Azure AD tenant used for Azure CLI and Az PowerShell context. Auto-detected from current context if omitted. |
| `-SubscriptionId` | string | (auto) | Azure subscription used for the MI. Auto-detected from current context if omitted. |
| `-AutoReauthenticate` | bool | `$true` | When set, interactively re-authenticates on claims-challenge or MFA-required responses. |
| `-IncludeDiffs` | switch | off | Include differential backup files in the transfer set (off by default). |
| `-ResourceGroupName` | string | `rg_sql_dev_zan` | Resource group containing the target Managed Instance. |
| `-ManagedInstanceName` | string | `dev-sql-mi-001` | Target Managed Instance name. |
| `-StorageAccountName` | string | `adlssqlbackups` | Storage account that holds backup containers (one container per source instance folder). |
| `-BackupRootPath` | string | `C:\SqlBackups` | Local root holding `<InstanceFolder>\<DatabaseFolder>\*.bak\|.trn` files. |
| `-SelectedInstanceNames` | string[] | (all) | Restrict to named source instance folders. Offline only; online requires exactly one instance. |
| `-SelectedDatabaseNames` | string[] | (all) | Restrict to named databases. Offline supports `Instance\Database` qualification; online does not. |

### Storage auth parameters (both wrappers)

| Parameter | Values | Default | Purpose |
|---|---|---|---|
| `-StorageAuthMode` | `EntraAzCli`, `EntraDevice`, `Sas` | `EntraAzCli` | How AzCopy authenticates to the storage container during the transfer phase. |
| `-StorageContainerSasToken` | string | (none) | Required when `-StorageAuthMode Sas`. Used for both the transfer leg and the LRS `StorageContainerIdentity=SharedAccessSignature` call when set. Also a recommended workaround when `completeRestore` returns `InternalServerError` under MI auth (see below). |

### Operator authentication parameters (both wrappers)

The operator identity is the principal that the **wrapper itself** runs under when it calls ARM (Azure Resource Manager). It is separate from the SQL Managed Instance's own system-assigned managed identity, which governs how the MI itself reads backup blobs.

Choosing a low-claim operator identity (for example a User-Assigned Managed Identity attached to an Azure VM) reduces the size of the ARM access token. Large operator tokens have been observed to correlate with `completeRestore` returning `InternalServerError`, so a small-claim identity is the safer default.

| Parameter | Values / Type | Default | Purpose |
|---|---|---|---|
| `-OperatorAuthMode` | `ExistingContext`, `Interactive`, `EntraUser`, `ServicePrincipal`, `UserAssignedManagedIdentity`, `SystemAssignedManagedIdentity` | `ExistingContext` | How the wrapper authenticates to ARM. `ExistingContext` reuses whatever `Get-AzContext` already returns (no behaviour change from earlier versions). |
| `-OperatorTenantId` | string | (auto) | Operator-specific tenant override; falls back to `-TenantId`. |
| `-OperatorSubscriptionId` | string | (auto) | Operator-specific subscription override; falls back to `-SubscriptionId`. |
| `-OperatorAccountUpn` | string | (none) | User principal name for `EntraUser` mode. Optional; Interactive works without it. |
| `-OperatorApplicationId` | string | (none) | Service principal app ID (for `ServicePrincipal`), or UAMI client ID (for `UserAssignedManagedIdentity`). |
| `-OperatorClientSecret` | SecureString | (none) | Service principal client secret. Mutually exclusive with `-OperatorCertificateThumbprint`. |
| `-OperatorCertificateThumbprint` | string | (none) | Service principal certificate thumbprint. Mutually exclusive with `-OperatorClientSecret`. |
| `-AutoGrantOperatorRoles` | switch | off | When set, the wrapper will attempt to create missing role assignments for the operator identity (requires the caller to hold `Role Based Access Control Administrator`, `User Access Administrator`, or `Owner` at the target scope). When off, the wrapper fails fast with an explanatory error listing any missing roles. |
| `-OperatorRequiredRoles` | string[] | (built-in) | Override the default required-role set. Defaults are `SQL Managed Instance Contributor` at the resource group scope, plus `Storage Blob Data Reader` at the storage account scope when not using SAS. |
| `-SkipTokenSizeCheck` | switch | off | Skip the JWT size diagnostic. Not recommended; the diagnostic is a useful signal when `completeRestore` returns `InternalServerError`. |

When the operator identity is missing a required role and `-AutoGrantOperatorRoles` is not set, the wrapper stops before the LRS phase with a message of the form:

```text
Operator RBAC preflight failed. The current operator identity is missing the following role assignments:
    - SQL Managed Instance Contributor @ /subscriptions/.../resourceGroups/rg_sql_dev_zan  (Required to start, monitor, and complete LRS on the Managed Instance.)
```

When `-AutoGrantOperatorRoles` is set, every grant is recorded in the run's report folder as `auth-grants-applied.json` so it can be audited or reverted later.

### Online-only parameters

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `-TransferPollSeconds` | int | `300` | Interval in seconds between backup-folder scans by the uploader. Lower values for small/frequent logs; higher for quieter estates. |
| `-StatusIntervalMinutes` | int | `15` | Minutes between status snapshots printed to the console. |
| `-InitialUploadTimeoutMinutes` | int | `30` | Maximum wait for the first FULL backup to complete uploading before the wrapper aborts. |
| `-CutoverCandidateCount` | int | `3` | Number of candidate log files shown in the cutover selector. |
| `-ScheduledCutoverLocalTime` | datetime | (none) | Local-time timestamp at which the wrapper triggers cutover automatically. Past times are rejected up-front. |
| `-TransferStatePath` | string | (auto) | Override the on-disk state file used by the uploader. |
| `-TransferOutputPath` | string | (auto) | Override stdout capture path for the background uploader process. |
| `-TransferErrorPath` | string | (auto) | Override stderr capture path for the background uploader process. |

### Operator auth mode quick-reference

| Mode | When to use | Token size (typical) | Requires |
|---|---|---|---|
| `ExistingContext` | You already ran `Connect-AzAccount` in this session and want the wrapper to reuse it. | Whatever your existing context produced | An existing Az context |
| `Interactive` | Laptop / workstation, one-off runs, browser-capable. | User-scale (can be large with heavy group membership) | Browser or device flow |
| `EntraUser` | Non-default user, still interactive, UPN known. | User-scale | Browser or device flow |
| `ServicePrincipal` | CI/CD and unattended runs from any host. | Small (SPN tokens have no user group claims) | App registration with secret or cert |
| `UserAssignedManagedIdentity` | Recommended for Azure VM / Arc / container hosts. Smallest, most stable, secret-free. | Small | UAMI attached to the host and its client ID |
| `SystemAssignedManagedIdentity` | Quick one-off from an Azure VM that already has SAMI. | Small | SAMI enabled on the host |

## License

This project is released under the [MIT License](LICENSE) and is free to use, modify, and distribute.

**Use at your own risk.** The software is provided "AS IS", without warranty of any kind, express or implied. The authors and copyright holders accept no liability for any data loss, downtime, migration failure, or other damages arising from use of these scripts. Always validate against non-production environments before running against production Azure SQL Managed Instance workloads, and ensure you have verified backups before initiating any cutover.
