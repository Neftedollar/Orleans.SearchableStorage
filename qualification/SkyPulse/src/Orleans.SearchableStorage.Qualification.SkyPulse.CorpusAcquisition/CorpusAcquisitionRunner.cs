namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public static class CorpusAcquisitionRunner
{
    public static async Task<AcquisitionResult> RunAsync(
        AcquisitionOptions options,
        IJetstreamLifecycleSource jetstream,
        IListReposSource listRepos,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(jetstream);
        ArgumentNullException.ThrowIfNull(listRepos);

        var outputDirectory = Path.GetFullPath(options.OutputDirectory);
        if (Directory.Exists(outputDirectory))
        {
            PrivateArtifactIO.EnsurePrivateDirectory(outputDirectory);
        }

        var checkpointPath = Path.Combine(outputDirectory, AcquisitionContract.CheckpointFileName);
        if (File.Exists(checkpointPath))
        {
            var existing = PrivateArtifactIO.ReadCanonical<AcquisitionCheckpoint>(checkpointPath);
            if (existing.Phase == AcquisitionPhase.Complete)
            {
                return AcquisitionWorkspace.ReadCompleted(options);
            }
        }

        await using var workspace = AcquisitionWorkspace.Open(options);
        var startCursor = workspace.Checkpoint.LastJetstreamCursor;
        await using var session = await jetstream.OpenAsync(
            new JetstreamOpenRequest(
                options.JetstreamEndpoint,
                options.JetstreamInstanceId,
                startCursor,
                options.MaximumJetstreamFrameBytes),
            cancellationToken).ConfigureAwait(false);
        if (!string.Equals(session.InstanceId, options.JetstreamInstanceId, StringComparison.Ordinal))
        {
            await workspace.PoisonAsync("jetstream-instance-switch", cancellationToken).ConfigureAwait(false);
            throw new AcquisitionContractException(
                "jetstream-instance-switch",
                "The opened Jetstream instance does not match the configured identity.");
        }

        using var pumpCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = PumpJetstreamAsync(workspace, session, pumpCancellation.Token);
        try
        {
            if (workspace.Checkpoint.Phase is AcquisitionPhase.Capturing or AcquisitionPhase.Sweeping)
            {
                await workspace.MarkSweepingAsync(cancellationToken).ConfigureAwait(false);
                await RunSweepsAsync(options, workspace, listRepos, pump, cancellationToken)
                    .ConfigureAwait(false);
            }

            ulong closeCursor;
            if (workspace.Checkpoint.CloseCursor is { } durableClose)
            {
                closeCursor = durableClose;
            }
            else
            {
                ThrowIfPumpFailedOrEnded(pump);
                closeCursor = await session.WaitForCloseCursorAsync(
                    options.CloseCursorWaitTimeout,
                    cancellationToken).ConfigureAwait(false);
                closeCursor = await workspace.RecordCloseCursorAsync(closeCursor, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (workspace.Checkpoint.LastJetstreamCursor < closeCursor)
            {
                await pump.WaitAsync(options.CloseCursorWaitTimeout, cancellationToken).ConfigureAwait(false);
            }

            if (pump.IsFaulted)
            {
                await pump.ConfigureAwait(false);
            }

            pumpCancellation.Cancel();
            try
            {
                await pump.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (pumpCancellation.IsCancellationRequested)
            {
            }

            return await workspace.FinalizeAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AcquisitionContractException exception)
        {
            await TryPoisonAsync(workspace, exception.ReasonCode).ConfigureAwait(false);
            throw;
        }
        catch (InvalidDataException exception)
        {
            await TryPoisonAsync(workspace, "malformed-source-contract").ConfigureAwait(false);
            throw new AcquisitionContractException(
                "malformed-source-contract",
                "A source violated the pinned acquisition contract.",
                exception);
        }
        finally
        {
            pumpCancellation.Cancel();
        }
    }

    private static async Task RunSweepsAsync(
        AcquisitionOptions options,
        AcquisitionWorkspace workspace,
        IListReposSource source,
        Task pump,
        CancellationToken cancellationToken)
    {
        while (workspace.Checkpoint.CurrentSweep <= options.FullSweepCount)
        {
            ThrowIfPumpFailedOrEnded(pump);
            var sweep = workspace.Checkpoint.CurrentSweep;
            var cursor = workspace.Checkpoint.NextPageCursor;
            var pageNumber = workspace.Checkpoint.CurrentSweepPageCount;
            while (true)
            {
                ThrowIfPumpFailedOrEnded(pump);
                var page = await source.GetPageAsync(
                    new ListReposRequest(
                        options.RelayEndpoint,
                        options.RelayInstanceId,
                        options.ListReposPageLimit,
                        cursor),
                    cancellationToken).ConfigureAwait(false);
                pageNumber = checked(pageNumber + 1);
                await workspace.CommitListReposPageAsync(
                    sweep,
                    pageNumber,
                    cursor,
                    page,
                    cancellationToken).ConfigureAwait(false);
                cursor = page.NextCursor;
                if (cursor is null)
                {
                    break;
                }
            }
        }
    }

    private static async Task PumpJetstreamAsync(
        AcquisitionWorkspace workspace,
        IJetstreamLifecycleSession session,
        CancellationToken cancellationToken)
    {
        await foreach (var observation in session.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!await workspace.CommitJetstreamAsync(observation, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private static void ThrowIfPumpFailedOrEnded(Task pump)
    {
        if (pump.IsFaulted)
        {
            pump.GetAwaiter().GetResult();
        }

        if (pump.IsCanceled)
        {
            pump.GetAwaiter().GetResult();
        }

        if (pump.IsCompleted)
        {
            throw new AcquisitionContractException(
                "jetstream-ended-before-close",
                "Jetstream lifecycle capture ended before a close cursor was recorded.");
        }
    }

    private static async Task TryPoisonAsync(AcquisitionWorkspace workspace, string reasonCode)
    {
        try
        {
            await workspace.PoisonAsync(reasonCode, CancellationToken.None).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
        }
    }
}
