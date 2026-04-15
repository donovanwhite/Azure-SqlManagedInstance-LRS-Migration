# Log Replay Service Guided Migration

This guide uses PowerShell to start and monitor Log Replay Service (LRS) migrations to Azure SQL Managed Instance.
It supports two modes:

- Offline: autocomplete mode (all backups uploaded in advance, auto cutover)
- Online: continuous mode (apply new logs/diffs until manual cutover)

## Prerequisites

- Az.Sql PowerShell module 4.0.0+.
- Azure CLI 2.42.0+.
- SQL Managed Instance managed identity with `Storage Blob Data Reader` or equivalent Read/List access on the target container.
- Backups in a flat-folder structure per database (no nested folders).
- Container or folder names must not include backup or Backup.
- When migrating multiple databases, each database must be in its own folder.

## Compliant folder structure

Use a flat folder per database. No nested subfolders like full or tran.

```
https://<storageaccount>.dfs.core.windows.net/<container>/db1/
	db1_full_20240301.bak
	db1_diff_20240302.bak
	db1_log_20240302_001.trn
	db1_log_20240302_002.trn

https://<storageaccount>.dfs.core.windows.net/<container>/db2/
	db2_full_20240301.bak
	db2_log_20240301_001.trn
```

## Configuration

Edit the config file to set defaults:

- LogReplayService\lrs-guided.config.json

Required values:

- ResourceGroupName
- InstanceName
- DatabaseName or DatabaseNames
- StorageContainerUri (base URI or database folder)
- StorageContainerIdentity (`ManagedIdentity`)

Optional values:

- Collation
- StorageContainerSasToken (ignored by the managed-identity restore workflow)
- LastBackupName (required for Offline mode or Online cutover)
- LastBackupNames (map of database name to last backup file)
- LogPath (optional; defaults to LogReplayService\\lrs-guided-<timestamp>.log)
- MonitorMinutes
- PollSeconds
- StorageContainerUriTemplate (optional, use {db})

## Start migration

Offline (autocomplete):

- Requires LastBackupName

Online (continuous):

- Allows new backups to be added until cutover

### Examples

PowerShell:

```
# Offline (autocomplete)
.\lrs-guided.ps1 -Mode Offline

# Online (continuous)
.\lrs-guided.ps1 -Mode Online

# Online with cutover at the end (requires LastBackupName)
.\lrs-guided.ps1 -Mode Online -CompleteOnlineCutover

# Multiple databases using a template
.\lrs-guided.ps1 -Mode Online -DatabaseNames db1,db2 -StorageContainerUriTemplate "https://storage.dfs.core.windows.net/container/{db}"
```

### Example config for multiple databases

```
{
  "ResourceGroupName": "rg-migration",
  "InstanceName": "mi-prod",
  "DatabaseNames": ["db1", "db2"],
  "StorageContainerUriTemplate": "https://storage.dfs.core.windows.net/container/{db}",
  "StorageContainerIdentity": "ManagedIdentity",
  "LastBackupNames": {
    "db1": "db1_cutover.bak",
    "db2": "db2_cutover.bak"
  }
}
```

## Copy backups to storage (AzCopy)

Use the helper script to copy backups without changing your production backup location.
It copies full/diff/log chains for Offline mode, and then continues copying new log files for Online mode.

When you launch the online wrapper, it may prompt for an AzCopy device login even if `az login` is already active. That behavior is intentional: the online uploader runs as a separate long-lived background process, so the wrapper establishes an AzCopy session that the child process can reuse. Offline transfers run inline and usually continue with the current Azure CLI session.

PowerShell:

```
# Offline: copy a specific full backup plus optional diff and logs from a start file
.\lrs-backup-transfer.ps1 -Mode Offline -SourcePath "D:\SqlBackups\db1" -FullBackupPath "D:\SqlBackups\db1\full" -FullBackupFile "db1_full_20240301.bak" -DiffPath "D:\SqlBackups\db1\diff" -TranPath "D:\SqlBackups\db1\tran" -TranStartFile "db1_log_20240302_001.trn" -StorageContainerUri "https://storage.dfs.core.windows.net/container/db1" -StorageAuthMode Sas -StorageContainerSasToken "<sas-token>" -IncludeDiffs

# Online: initial copy then continuous log copy every 5 minutes
.\lrs-backup-transfer.ps1 -Mode Online -SourcePath "D:\SqlBackups\db1" -FullBackupPath "D:\SqlBackups\db1\full" -FullBackupFile "db1_full_20240301.bak" -DiffPath "D:\SqlBackups\db1\diff" -TranPath "D:\SqlBackups\db1\tran" -TranStartFile "db1_log_20240302_001.trn" -StorageContainerUri "https://storage.dfs.core.windows.net/container/db1" -StorageAuthMode Sas -StorageContainerSasToken "<sas-token>" -IntervalSeconds 300 -IncludeDiffs

# Multiple databases using a template
.\lrs-backup-transfer.ps1 -Mode Online -SourcePathBase "D:\SqlBackups" -DatabaseNames db1,db2 -FullBackupPath "D:\SqlBackups\{db}\full" -FullBackupFile "{db}_full_20240301.bak" -DiffPath "D:\SqlBackups\{db}\diff" -TranPath "D:\SqlBackups\{db}\tran" -TranStartFile "{db}_log_20240302_001.trn" -StorageContainerUriTemplate "https://storage.dfs.core.windows.net/container/{db}" -StorageAuthMode Sas -StorageContainerSasToken "<sas-token>" -IntervalSeconds 300 -IncludeDiffs
```

Notes:

- The AzCopy SAS token must include write permissions for uploads.
- For large on-prem backups, consider using backup compression to improve transfer and storage behavior.
- Block blob limits (Backup to URL): ~200 GB per blob (50,000 blocks * 4 MB `MAXTRANSFERSIZE`), up to 64 striped URLs (~12.8 TB total).
- If a full backup exceeds block blob limits, use striping for full backups only (avoid striping for log backups).

Example (compression):

```
BACKUP DATABASE [YourDb]
TO DISK = 'D:\SQLBackups\YourDb\YourDb_FULL.bak'
WITH COMPRESSION, CHECKSUM, STATS = 10;
```

Example (full backup striping only when required):

```
BACKUP DATABASE [YourDb]
TO DISK = 'D:\SQLBackups\YourDb\YourDb_FULL_01.bak',
   DISK = 'D:\SQLBackups\YourDb\YourDb_FULL_02.bak',
   DISK = 'D:\SQLBackups\YourDb\YourDb_FULL_03.bak',
   DISK = 'D:\SQLBackups\YourDb\YourDb_FULL_04.bak'
WITH COMPRESSION, CHECKSUM, STATS = 10;
```
- The script tracks last sync time in a local state file next to the script.
- If DiffPath is omitted, differential backups are skipped.
- If TranStartFile is omitted, all log backups in TranPath are copied.

## Choose Offline vs Online

- Use Offline (autocomplete) when the full backup chain is available up front and no further log catch-up is needed.
- Use Online (continuous) when you need to keep applying new log backups until a planned cutover.

## Migration checklist

Offline (autocomplete):

- Verify SQL Server is using full recovery model and backups are valid (prefer CHECKSUM).
- For large on-prem backups, use compression to improve transfer and storage behavior.
- If full backups exceed blob size limits, use striping for full backups only.
- Ensure storage container and folders do not contain the word backup.
- Place each database backup chain in a flat folder (no nested full or tran).
- Upload the entire backup chain before starting LRS.
- Confirm the SQL Managed Instance managed identity has Read/List access to the storage container.
- Identify the final backup file name per database.
- Start LRS in Offline mode and monitor until completion.

Online (continuous):

- Verify SQL Server is using full recovery model and backups are valid (prefer CHECKSUM).
- For large on-prem backups, use compression to improve transfer and storage behavior.
- If full backups exceed blob size limits, use striping for full backups only.
- Ensure storage container and folders do not contain the word backup.
- Place each database backup chain in a flat folder (no nested full or tran).
- Upload the initial backup chain before starting LRS.
- Confirm the SQL Managed Instance managed identity has Read/List access to the storage container.
- Start LRS in Online mode and monitor status while logs are applied.
- At cutover: stop workload, take final log-tail backup, upload it, then run cutover with the final backup name.

## Monitoring

The script polls LRS status for the configured duration and prints a health summary.
If it detects failure, it prints recent managed instance operations for troubleshooting.
All console output is also captured in the log file.

## Notes

- AzCopy SAS usage is upload-only in this workflow.
- The workflow always uses managed identity for LRS restore access.
- Databases are unavailable until cutover completes.
- Autocomplete mode requires the entire backup chain to be present before start.
- Continuous mode requires a manual cutover after the final log-tail backup is applied.
- LRS requires a flat folder per database and disallows 'backup' in container or folder names.
