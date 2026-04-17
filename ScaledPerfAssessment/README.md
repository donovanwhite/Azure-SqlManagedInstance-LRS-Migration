# Scaled Performance Assessment

This folder now reflects the current Azure CLI `datamigration` workflow instead of the older DMA and `SqlAssessment.exe` command set.

This workflow is intended for environments that cannot practically use online Azure-assisted assessment and migration tooling, such as Azure Migrate, Azure Arc-enabled experiences, or other connected discovery and assessment services. Where those online options are available and supported, they remain the recommended first choice because they provide a more current, integrated, and operationally simpler experience.

## Current workflow

1. Install or refresh the extension:

```powershell
az extension add --name datamigration --upgrade
```

2. Run SQL assessment:

```powershell
az datamigration get-assessment --config-file-path C:\AzureDataMigrationAssessments\Configs\Scaled-MetaData-Conf.json
```

3. Collect performance data:

```powershell
az datamigration performance-data-collection --config-file-path C:\AzureDataMigrationAssessments\Configs\Scaled-PerfData-Conf.json
```

4. Generate SKU recommendations:

```powershell
az datamigration get-sku-recommendation --config-file-path C:\AzureDataMigrationAssessments\Configs\Scaled-SKU-Conf.json
```

## Notes

- The `Scaled-ManagedInstance-Conf.xml` file is retained only as a deprecated marker for the old DMA-era flow.
- The `Assessment Commands` file contains direct command-line equivalents if you prefer not to use config files.
- `az datamigration` replaces the local DMA/SqlAssessment command sequence for assessment, performance capture, and SKU recommendation.
- The old `AzureMigrateUpload` DMA step is not part of the `az datamigration` CLI workflow.
- Use this folder when you need an offline or minimally connected assessment path; prefer Azure Migrate, Azure Arc-enabled options, and other online Azure tooling where applicable.

## Reference

- https://learn.microsoft.com/en-us/cli/azure/datamigration?view=azure-cli-latest