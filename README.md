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

## License

This project is released under the [MIT License](LICENSE) and is free to use, modify, and distribute.

**Use at your own risk.** The software is provided "AS IS", without warranty of any kind, express or implied. The authors and copyright holders accept no liability for any data loss, downtime, migration failure, or other damages arising from use of these scripts. Always validate against non-production environments before running against production Azure SQL Managed Instance workloads, and ensure you have verified backups before initiating any cutover.
