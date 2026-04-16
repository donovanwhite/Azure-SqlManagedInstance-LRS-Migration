# Sample execution commands for the Log Replay Service wrapper scripts.
# Defaults below reflect the current environment values already built into the wrappers.

# Default values
$TenantId = $null
$SubscriptionId = $null
$AutoReauthenticate = $true
$ResourceGroupName = 'rg_sql_dev_zan'
$ManagedInstanceName = 'dev-sql-mi-001'
$StorageAccountName = 'adlssqlbackups'
$BackupRootPath = 'C:\SqlBackups'
$StorageContainerSasToken = $null

# End-to-end login and execution example

# 1. Sign in to Azure CLI
az login

# 2. Select the subscription you want the wrappers to use
az account set --subscription ''

# 3. Optional but recommended: establish Az PowerShell context too
Connect-AzAccount -Subscription ''

# If the tenant later requires MFA or a claims challenge, the wrappers can now fall back
# to device-code authentication automatically. Leave AutoReauthenticate enabled unless you
# want the wrapper to fail instead of prompting for interactive sign-in.
# The wrappers now also perform an Az PowerShell control-plane MFA preflight before any backup
# transfer starts, so claims-challenge failures are surfaced earlier in the run.
# AzCopy upload targets use the DFS endpoint (dfs.core.windows.net) so the hierarchical folder
# layout remains intact. LRS-side URI construction converts DFS-style input to the Blob endpoint
# (blob.core.windows.net) while preserving the same container and per-database folder path.
# SAS mode bypasses Entra auth for AzCopy upload only. The wrappers still use the SQL MI
# managed identity for LRS restore access and still need Az PowerShell authentication for
# SQL Managed Instance control-plane operations.

# 4. Move to the wrappers folder
Set-Location 'C:\AzureDataMigrationAssessments\LogReplayService\wrappers'

# 5. Run the offline wrapper by using the current Azure context
# Use Instance\Database or Instance::Database when the same database name exists on multiple source instances
# and you want to restore only a specific copy. When every selected database is fully qualified,
# you do not need -SelectedInstanceNames in the same command.
.\wrapper-execution-multi-offline.ps1 `
    -AutoReauthenticate $true `
    -ResourceGroupName 'rg_sql_dev_zan' `
    -ManagedInstanceName 'dev-sql-mi-001' `
    -StorageAccountName 'adlssqlbackups' `
    -BackupRootPath 'C:\SqlBackups' `
    -SelectedDatabaseNames 'HP865-DONWHITE$SQL2022\2008DW', 'HP865-DONWHITE$SQL2022\TenantDataDb', 'HP865-DONWHITE$SQL2025\2008DW_1'

# 6. Run the online wrapper by using the current Azure context
# For Entra-backed upload and LRS start, ensure the SQL MI managed identity can read the backup container.
# Online mode may still prompt for an AzCopy device login up front. The uploader runs in a
# separate long-lived background process, so the wrapper establishes an AzCopy session the child
# process can reuse instead of depending on the current Azure CLI token alone.
# Direct copy/paste commands with relative wrapper paths

# Offline: all discovered instances and databases under C:\SqlBackups
.\wrapper-execution-multi-offline.ps1 `
    -ResourceGroupName 'rg_sql_dev_zan' `
    -ManagedInstanceName 'dev-sql-mi-001' `
    -StorageAccountName 'adlssqlbackups' `
    -BackupRootPath 'C:\SqlBackups'

# Offline: qualify a same-named database so only one source instance copy is restored
.\wrapper-execution-multi-offline.ps1 `
  -ResourceGroupName 'rg_sql_dev_zan' `
  -ManagedInstanceName 'dev-sql-mi-001' `
  -StorageAccountName 'adlssqlbackups' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedDatabaseNames 'HP865-DONWHITE$SQL2022\2008DW', 'TenantDataDb'

# Online: selected instance and databases only
.\wrapper-execution-multi-online.ps1 `
  -ResourceGroupName 'rg_sql_dev_zan' `
  -ManagedInstanceName 'dev-sql-mi-001' `
  -StorageAccountName 'adlssqlbackups' `
  -BackupRootPath 'C:\SqlBackups' `
  -SelectedInstanceNames 'HP865-DONWHITE$SQL2022' `
  -SelectedDatabaseNames '2008DW', 'TenantDataDb' `
  -TransferPollSeconds 60 `
  -StatusIntervalMinutes 2 `
  -ScheduledCutoverLocalTime '2026-04-15 15:15'


# Notes
# - Offline wrapper performs upload and guided restore completion.
# - Offline SelectedDatabaseNames accepts either Database or Instance\Database (or Instance::Database).
# - If every offline selected database is already instance-qualified, -SelectedInstanceNames is optional.
# - Online wrapper starts the background uploader, starts online LRS, shows status snapshots,
#   and waits for operator actions S, C, or Q.
# - Online mode still supports exactly one source instance, so database names there remain unambiguous.
# - the SQL Managed Instance managed identity must be able to read the backup container.
# - Each wrapper run writes reports under C:\AzureDataMigrationAssessments\LogReplayService\reports.
