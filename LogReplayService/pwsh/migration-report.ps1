function Write-MigrationEvent {
    param(
        [string]$EventLogPath,
        [string]$RunId,
        [string]$Source,
        [string]$Level = 'Info',
        [string]$Phase,
        [string]$Action,
        [string]$Mode,
        [string]$InstanceName,
        [string]$DatabaseName,
        [string]$Message,
        [hashtable]$Data
    )

    if (-not $EventLogPath) {
        return
    }

    $parentPath = Split-Path -Parent $EventLogPath
    if ($parentPath -and -not (Test-Path -LiteralPath $parentPath)) {
        New-Item -ItemType Directory -Path $parentPath -Force | Out-Null
    }

    $eventRecord = [ordered]@{
        timestamp    = (Get-Date).ToString('o')
        runId        = $RunId
        source       = $Source
        level        = $Level
        phase        = $Phase
        action       = $Action
        mode         = $Mode
        instanceName = $InstanceName
        databaseName = $DatabaseName
        message      = $Message
        data         = $Data
    }

    Add-Content -LiteralPath $EventLogPath -Value ($eventRecord | ConvertTo-Json -Depth 10 -Compress)
}

function Read-MigrationEventLog {
    param([string[]]$Paths)

    $events = @()
    foreach ($path in $Paths) {
        if (-not $path -or -not (Test-Path -LiteralPath $path)) {
            continue
        }

        foreach ($line in Get-Content -LiteralPath $path) {
            if ([string]::IsNullOrWhiteSpace($line)) {
                continue
            }

            try {
                $events += ($line | ConvertFrom-Json)
            } catch {
            }
        }
    }

    return ,@($events | Sort-Object -Property @{ Expression = { [datetime]$_.timestamp } }, source, phase, action)
}

function ConvertTo-MigrationHtmlValue {
    param([object]$Value)

    if ($null -eq $Value) {
        return ''
    }

    return [System.Net.WebUtility]::HtmlEncode([string]$Value)
}

function ConvertTo-MigrationDateTime {
    param([object]$Value)

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return $null
    }

    if ($Value -is [datetime]) {
        return [datetime]$Value
    }

    if ($Value -is [datetimeoffset]) {
        return $Value.LocalDateTime
    }

    try {
        return [datetime]$Value
    } catch {
        return $null
    }
}

function ConvertTo-MigrationDurationText {
    param(
        [datetime]$StartTime,
        [datetime]$EndTime
    )

    if (-not $StartTime -or -not $EndTime) {
        return ''
    }

    $duration = $EndTime - $StartTime
    if ($duration.TotalSeconds -lt 0) {
        $duration = [timespan]::Zero
    }

    if ($duration.TotalHours -ge 1) {
        return ('{0}h {1}m' -f [int]$duration.TotalHours, $duration.Minutes)
    }

    if ($duration.TotalMinutes -ge 1) {
        return ('{0}m {1}s' -f [int]$duration.TotalMinutes, $duration.Seconds)
    }

    return ('{0}s' -f [Math]::Max(0, [int][Math]::Round($duration.TotalSeconds)))
}

function Get-MigrationTimelineRows {
    param(
        [object[]]$Events,
        [object[]]$DatabaseSummaries
    )

    $rows = @()
    foreach ($databaseSummary in $DatabaseSummaries) {
        $databaseEvents = @($Events | Where-Object { $_.databaseName -eq $databaseSummary.databaseName } | Sort-Object -Property @{ Expression = { ConvertTo-MigrationDateTime -Value $_.timestamp } }, source, phase, action)
        if (-not $databaseEvents -or $databaseEvents.Count -eq 0) {
            continue
        }

        $instanceEvents = @($Events | Where-Object { -not $_.databaseName -and $_.instanceName -eq $databaseSummary.instanceName } | Sort-Object -Property @{ Expression = { ConvertTo-MigrationDateTime -Value $_.timestamp } }, source, phase, action)
        $transferEvents = @($databaseEvents | Where-Object { $_.phase -eq 'Transfer' })
        $lrsEvents = @($databaseEvents | Where-Object { $_.phase -eq 'LRS' })

        $copyStart = if ($transferEvents.Count -gt 0) { ConvertTo-MigrationDateTime -Value $transferEvents[0].timestamp } else { $null }
        $copyEnd = if ($transferEvents.Count -gt 0) { ConvertTo-MigrationDateTime -Value $transferEvents[$transferEvents.Count - 1].timestamp } else { $null }

        $instanceRestoreStartEvents = @($instanceEvents | Where-Object { $_.phase -eq 'LRS' -and $_.action -in @('ScriptStart', 'InstanceRestoreStart') })
        $restoreStartCandidates = @((@($lrsEvents) + $instanceRestoreStartEvents) | Sort-Object -Property @{ Expression = { ConvertTo-MigrationDateTime -Value $_.timestamp } }, source, phase, action)
        $restoreStart = if ($restoreStartCandidates.Count -gt 0) { ConvertTo-MigrationDateTime -Value $restoreStartCandidates[0].timestamp } else { $null }

        $restoreEndCandidates = @($lrsEvents)
        if ($restoreEndCandidates.Count -eq 0) {
            $restoreEndCandidates = @($instanceEvents | Where-Object { $_.phase -eq 'LRS' })
        }
        $restoreEndCandidates = @($restoreEndCandidates)
        $restoreEnd = if ($restoreEndCandidates.Count -gt 0) { ConvertTo-MigrationDateTime -Value $restoreEndCandidates[$restoreEndCandidates.Count - 1].timestamp } else { $null }

        $segments = @()
        if ($copyStart -and $copyEnd) {
            $segments += [pscustomobject]@{
                label = 'Copy'
                cssClass = 'copy'
                startTime = $copyStart
                endTime = $copyEnd
                durationText = ConvertTo-MigrationDurationText -StartTime $copyStart -EndTime $copyEnd
            }
        }

        if ($restoreStart -and $restoreEnd) {
            $restoreCssClass = if ($databaseSummary.status -eq 'Failed') { 'restore failed' } elseif ($databaseSummary.status -eq 'Completed') { 'restore completed' } else { 'restore active' }
            $segments += [pscustomobject]@{
                label = 'Restore'
                cssClass = $restoreCssClass
                startTime = $restoreStart
                endTime = $restoreEnd
                durationText = ConvertTo-MigrationDurationText -StartTime $restoreStart -EndTime $restoreEnd
            }
        }

        if ($segments.Count -eq 0) {
            continue
        }

        $rows += [pscustomobject]@{
            databaseName = $databaseSummary.databaseName
            instanceName = $databaseSummary.instanceName
            status = $databaseSummary.status
            segments = @($segments)
        }
    }

    return ,@($rows)
}

function Get-MigrationDatabaseSummaries {
    param([object[]]$Events)

    $databaseEvents = @($Events | Where-Object { $_.databaseName })
    if (-not $databaseEvents -or $databaseEvents.Count -eq 0) {
        return ,@()
    }

    $summaries = foreach ($group in ($databaseEvents | Group-Object -Property databaseName | Sort-Object Name)) {
        $groupEvents = @($group.Group | Sort-Object -Property @{ Expression = { [datetime]$_.timestamp } }, source, phase, action)
        $latestEvent = $groupEvents[$groupEvents.Count - 1]
        $latestError = @($groupEvents | Where-Object { $_.level -eq 'Error' } | Select-Object -Last 1)
        $latestWarning = @($groupEvents | Where-Object { $_.level -eq 'Warning' } | Select-Object -Last 1)
        $latestSuccess = @($groupEvents | Where-Object { $_.level -eq 'Success' } | Select-Object -Last 1)
        $status = 'InProgress'

        if ($latestError.Count -gt 0) {
            $status = 'Failed'
        } elseif (@($groupEvents | Where-Object { $_.action -in @('StatusCompleted', 'CutoverSubmitted') }).Count -gt 0) {
            $status = 'Completed'
        } elseif ($latestWarning.Count -gt 0) {
            $status = 'Warning'
        } elseif ($latestSuccess.Count -gt 0) {
            $status = 'Healthy'
        }

        [pscustomobject]@{
            databaseName    = [string]$group.Name
            instanceName    = [string]$latestEvent.instanceName
            status          = $status
            totalEvents     = $groupEvents.Count
            errorCount      = @($groupEvents | Where-Object { $_.level -eq 'Error' }).Count
            warningCount    = @($groupEvents | Where-Object { $_.level -eq 'Warning' }).Count
            successCount    = @($groupEvents | Where-Object { $_.level -eq 'Success' }).Count
            lastTimestamp   = [string]$latestEvent.timestamp
            lastPhase       = [string]$latestEvent.phase
            lastAction      = [string]$latestEvent.action
            lastMessage     = [string]$latestEvent.message
            lastError       = if ($latestError.Count -gt 0) { [string]$latestError[0].message } else { $null }
            lastWarning     = if ($latestWarning.Count -gt 0) { [string]$latestWarning[0].message } else { $null }
        }
    }

    return ,@($summaries)
}

function Export-MigrationArtifacts {
    param(
        [string]$JsonPath,
        [string]$HtmlPath,
        [object[]]$Events,
        [hashtable]$Metadata
    )

    $normalizedEvents = @($Events)
    $databaseSummaries = Get-MigrationDatabaseSummaries -Events $normalizedEvents
    $timelineRows = Get-MigrationTimelineRows -Events $normalizedEvents -DatabaseSummaries $databaseSummaries
    $reportObject = [ordered]@{
        generatedAt = (Get-Date).ToString('o')
        metadata    = $Metadata
        summary     = [ordered]@{
            totalEvents      = $normalizedEvents.Count
            errorCount       = @($normalizedEvents | Where-Object { $_.level -eq 'Error' }).Count
            warningCount     = @($normalizedEvents | Where-Object { $_.level -eq 'Warning' }).Count
            successCount     = @($normalizedEvents | Where-Object { $_.level -eq 'Success' }).Count
            sources          = @($normalizedEvents | Select-Object -ExpandProperty source -Unique)
            databases        = @($normalizedEvents | Where-Object { $_.databaseName } | Select-Object -ExpandProperty databaseName -Unique)
            instances        = @($normalizedEvents | Where-Object { $_.instanceName } | Select-Object -ExpandProperty instanceName -Unique)
            databaseSummaries = $databaseSummaries
        }
        events      = $normalizedEvents
    }

    if ($JsonPath) {
        $jsonParent = Split-Path -Parent $JsonPath
        if ($jsonParent -and -not (Test-Path -LiteralPath $jsonParent)) {
            New-Item -ItemType Directory -Path $jsonParent -Force | Out-Null
        }

        $reportObject | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $JsonPath
    }

    if (-not $HtmlPath) {
        return
    }

    $htmlParent = Split-Path -Parent $HtmlPath
    if ($htmlParent -and -not (Test-Path -LiteralPath $htmlParent)) {
        New-Item -ItemType Directory -Path $htmlParent -Force | Out-Null
    }

    $metadataRows = @()
    foreach ($entry in $Metadata.GetEnumerator() | Sort-Object Name) {
        $metadataValue = if ($entry.Value -is [System.Collections.IEnumerable] -and -not ($entry.Value -is [string])) {
            ($entry.Value | ForEach-Object { [string]$_ }) -join ', '
        } else {
            [string]$entry.Value
        }

        $metadataRows += "<tr><th>$([System.Net.WebUtility]::HtmlEncode($entry.Key))</th><td>$([System.Net.WebUtility]::HtmlEncode($metadataValue))</td></tr>"
    }

    $databaseCards = foreach ($databaseSummary in $databaseSummaries) {
        $statusClass = switch ($databaseSummary.status) {
            'Failed' { 'db-card failed' }
            'Warning' { 'db-card warning' }
            'Completed' { 'db-card completed' }
            'Healthy' { 'db-card healthy' }
            default { 'db-card inprogress' }
        }

        $attentionMessage = if ($databaseSummary.lastError) {
            $databaseSummary.lastError
        } elseif ($databaseSummary.lastWarning) {
            $databaseSummary.lastWarning
        } else {
            $databaseSummary.lastMessage
        }

        @"
<div class="$statusClass">
  <div class="db-card-header">
    <div>
      <div class="db-name">$(ConvertTo-MigrationHtmlValue -Value $databaseSummary.databaseName)</div>
      <div class="db-instance">$(ConvertTo-MigrationHtmlValue -Value $databaseSummary.instanceName)</div>
    </div>
    <div class="db-status">$(ConvertTo-MigrationHtmlValue -Value $databaseSummary.status)</div>
  </div>
  <div class="db-meta">Last event: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.lastPhase) / $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.lastAction)</div>
  <div class="db-meta">Updated: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.lastTimestamp)</div>
  <div class="db-counts">
    <span>Events: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.totalEvents)</span>
    <span>Errors: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.errorCount)</span>
    <span>Warnings: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.warningCount)</span>
    <span>Successes: $(ConvertTo-MigrationHtmlValue -Value $databaseSummary.successCount)</span>
  </div>
  <div class="db-message">$(ConvertTo-MigrationHtmlValue -Value $attentionMessage)</div>
</div>
"@
    }

        $timelineMarkup = ''
        if ($timelineRows.Count -gt 0) {
                $allSegmentStarts = @($timelineRows | ForEach-Object { $_.segments } | ForEach-Object { $_.startTime })
                $allSegmentEnds = @($timelineRows | ForEach-Object { $_.segments } | ForEach-Object { $_.endTime })
                $overallStart = @($allSegmentStarts | Sort-Object | Select-Object -First 1)[0]
                $overallEnd = @($allSegmentEnds | Sort-Object | Select-Object -Last 1)[0]
                $totalSeconds = [Math]::Max(1, ($overallEnd - $overallStart).TotalSeconds)

                $timelineRowsMarkup = foreach ($timelineRow in $timelineRows) {
                        $segmentBadges = foreach ($segment in $timelineRow.segments) {
                                "<span class='timeline-badge $($segment.cssClass -replace ' ', '-')'>$([System.Net.WebUtility]::HtmlEncode($segment.label)): $([System.Net.WebUtility]::HtmlEncode($segment.durationText))</span>"
                        }

                        $segmentBars = foreach ($segment in $timelineRow.segments) {
                                $left = (($segment.startTime - $overallStart).TotalSeconds / $totalSeconds) * 100
                                $width = (($segment.endTime - $segment.startTime).TotalSeconds / $totalSeconds) * 100
                                if ($width -lt 1.8) {
                                        $width = 1.8
                                }

                                $segmentLabel = "$($segment.label) $($segment.durationText)".Trim()
                                "<div class='timeline-segment $($segment.cssClass -replace ' ', '-')' style='left: $([Math]::Round($left, 2))%; width: $([Math]::Round($width, 2))%;'><span>$([System.Net.WebUtility]::HtmlEncode($segmentLabel))</span></div>"
                        }

                        @"
<div class="timeline-row">
    <div class="timeline-row-header">
        <div>
            <div class="timeline-db-name">$(ConvertTo-MigrationHtmlValue -Value $timelineRow.databaseName)</div>
            <div class="timeline-db-instance">$(ConvertTo-MigrationHtmlValue -Value $timelineRow.instanceName)</div>
        </div>
        <div class="timeline-badges">$($segmentBadges -join '')</div>
    </div>
    <div class="timeline-track">
        $($segmentBars -join [Environment]::NewLine)
    </div>
</div>
"@
                }

                $timelineMarkup = @"
    <h2>Timeline</h2>
    <div class="timeline-legend">
        <span class="legend-item"><span class="legend-swatch copy"></span>Copy</span>
        <span class="legend-item"><span class="legend-swatch restore"></span>Restore</span>
    </div>
    <div class="timeline-range">$(ConvertTo-MigrationHtmlValue -Value $overallStart) to $(ConvertTo-MigrationHtmlValue -Value $overallEnd)</div>
    <div class="timeline-grid">
        $($timelineRowsMarkup -join [Environment]::NewLine)
    </div>
"@
        }

    $eventRows = foreach ($eventRecord in $normalizedEvents) {
        $dataText = ''
        if ($eventRecord.PSObject.Properties.Match('data').Count -gt 0 -and $eventRecord.data) {
            $dataText = ($eventRecord.data | ConvertTo-Json -Depth 8 -Compress)
        }

        $rowClass = switch ([string]$eventRecord.level) {
            'Error' { 'event-row error' }
            'Warning' { 'event-row warning' }
            'Success' { 'event-row success' }
            default { 'event-row info' }
        }

        "<tr class='$rowClass'><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.timestamp))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.source))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.level))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.phase))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.action))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.instanceName))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.databaseName))</td><td>$([System.Net.WebUtility]::HtmlEncode([string]$eventRecord.message))</td><td><pre>$([System.Net.WebUtility]::HtmlEncode($dataText))</pre></td></tr>"
    }

    $summary = $reportObject.summary
    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Migration Report</title>
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; margin: 24px; color: #1f2937; }
    h1, h2 { margin-bottom: 12px; }
    .summary { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
    .card { border: 1px solid #d1d5db; border-radius: 8px; padding: 12px 16px; min-width: 140px; background: #f9fafb; }
    .card .label { font-size: 12px; text-transform: uppercase; color: #6b7280; }
    .card .value { font-size: 24px; font-weight: 600; }
        .database-summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 24px; }
        .db-card { border-radius: 10px; padding: 14px; border: 1px solid #d1d5db; background: #f9fafb; }
        .db-card.failed { border-color: #dc2626; background: #fef2f2; }
        .db-card.warning { border-color: #d97706; background: #fffbeb; }
        .db-card.completed { border-color: #047857; background: #ecfdf5; }
        .db-card.healthy { border-color: #2563eb; background: #eff6ff; }
        .db-card.inprogress { border-color: #6b7280; background: #f3f4f6; }
        .db-card-header { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; margin-bottom: 8px; }
        .db-name { font-size: 18px; font-weight: 700; }
        .db-instance { color: #6b7280; font-size: 12px; }
        .db-status { font-weight: 700; text-transform: uppercase; font-size: 12px; }
        .db-meta { font-size: 12px; color: #4b5563; margin-top: 4px; }
        .db-counts { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; font-size: 12px; }
        .db-message { margin-top: 10px; font-size: 13px; font-weight: 600; }
    .timeline-legend { display: flex; gap: 16px; align-items: center; margin-bottom: 8px; font-size: 12px; color: #4b5563; flex-wrap: wrap; }
    .legend-item { display: inline-flex; gap: 8px; align-items: center; }
    .legend-swatch { width: 14px; height: 14px; border-radius: 4px; display: inline-block; }
    .legend-swatch.copy { background: linear-gradient(90deg, #2563eb, #60a5fa); }
    .legend-swatch.restore { background: linear-gradient(90deg, #047857, #34d399); }
    .timeline-range { font-size: 12px; color: #6b7280; margin-bottom: 12px; }
    .timeline-grid { display: grid; gap: 14px; margin-bottom: 24px; }
    .timeline-row { border: 1px solid #d1d5db; border-radius: 12px; background: #f9fafb; padding: 12px 14px; }
    .timeline-row-header { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; margin-bottom: 10px; }
    .timeline-db-name { font-size: 15px; font-weight: 700; }
    .timeline-db-instance { color: #6b7280; font-size: 12px; }
    .timeline-badges { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    .timeline-badge { display: inline-flex; align-items: center; border-radius: 999px; padding: 4px 8px; font-size: 11px; font-weight: 600; }
    .timeline-badge.copy { background: #dbeafe; color: #1d4ed8; }
    .timeline-badge.restore-active, .timeline-badge.restore-completed { background: #d1fae5; color: #065f46; }
    .timeline-badge.restore-failed { background: #fee2e2; color: #991b1b; }
    .timeline-track { position: relative; height: 34px; border-radius: 999px; background: linear-gradient(90deg, #e5e7eb, #f3f4f6); overflow: hidden; }
    .timeline-segment { position: absolute; top: 4px; bottom: 4px; border-radius: 999px; min-width: 14px; display: flex; align-items: center; justify-content: center; color: #ffffff; font-size: 11px; font-weight: 700; padding: 0 8px; white-space: nowrap; }
    .timeline-segment.copy { background: linear-gradient(90deg, #2563eb, #60a5fa); }
    .timeline-segment.restore-active, .timeline-segment.restore-completed { background: linear-gradient(90deg, #047857, #34d399); }
    .timeline-segment.restore-failed { background: linear-gradient(90deg, #b91c1c, #ef4444); }
    .timeline-segment span { overflow: hidden; text-overflow: ellipsis; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
    th, td { border: 1px solid #d1d5db; padding: 8px; text-align: left; vertical-align: top; }
    th { background: #f3f4f6; }
        .event-row.error td { background: #fef2f2; }
        .event-row.warning td { background: #fffbeb; }
        .event-row.success td { background: #f0fdf4; }
    pre { margin: 0; white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
  <h1>Migration Report</h1>
  <div class="summary">
    <div class="card"><div class="label">Events</div><div class="value">$($summary.totalEvents)</div></div>
    <div class="card"><div class="label">Errors</div><div class="value">$($summary.errorCount)</div></div>
    <div class="card"><div class="label">Warnings</div><div class="value">$($summary.warningCount)</div></div>
    <div class="card"><div class="label">Successes</div><div class="value">$($summary.successCount)</div></div>
  </div>
  <h2>Metadata</h2>
  <table>
    <tbody>
      $($metadataRows -join [Environment]::NewLine)
    </tbody>
  </table>
    <h2>Database Summary</h2>
    <div class="database-summary">
        $($databaseCards -join [Environment]::NewLine)
    </div>
    $timelineMarkup
  <h2>Events</h2>
  <table>
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>Source</th>
        <th>Level</th>
        <th>Phase</th>
        <th>Action</th>
        <th>Instance</th>
        <th>Database</th>
        <th>Message</th>
        <th>Data</th>
      </tr>
    </thead>
    <tbody>
      $($eventRows -join [Environment]::NewLine)
    </tbody>
  </table>
</body>
</html>
"@

    Set-Content -LiteralPath $HtmlPath -Value $html
}