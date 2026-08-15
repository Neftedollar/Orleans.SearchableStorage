using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

public static class CorpusFreezer
{
    private const long MinimumMemoryBudgetBytes = 4 * 1024;
    private const long MaximumMemoryBudgetBytes = 4L * 1024 * 1024 * 1024;
    private const int MaximumProfileCount = 64;

    public static CorpusFreezeResult Freeze(CorpusFreezeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var (journalPath, outputDirectory, profiles) = Validate(options);
        var outputParent = Path.GetDirectoryName(outputDirectory)
            ?? throw new ArgumentException("The output directory must have a parent.", nameof(options));
        Directory.CreateDirectory(outputParent);

        var outputName = Path.GetFileName(outputDirectory);
        var stagingDirectory = Path.Combine(outputParent, $".{outputName}.tmp-{Guid.NewGuid():N}");
        PrivateWorkspacePermissions.CreateDirectory(stagingDirectory);
        try
        {
            var workDirectory = Path.Combine(stagingDirectory, ".private-sort-work");
            PrivateWorkspacePermissions.CreateDirectory(workDirectory);
            var sorted = ExternalObservationSorter.Sort(
                journalPath,
                workDirectory,
                options.MemoryBudgetBytes,
                options.MergeFanIn);

            var binaryPath = Path.Combine(stagingDirectory, CorpusFormat.BinaryArtifactName);
            var humanPath = options.WriteHumanReadableHex
                ? Path.Combine(stagingDirectory, CorpusFormat.HumanArtifactName)
                : null;
            var accountCount = FoldLatestExplicitActive(sorted.SortedRunPath, binaryPath, humanPath);
            Directory.Delete(workDirectory, recursive: true);

            if (accountCount == 0)
            {
                throw new InvalidDataException(
                    "The observation journal does not select any latest explicit Active accounts.");
            }

            var resolvedProfiles = ResolveProfiles(profiles, accountCount);
            var parentEvidence = ArtifactEvidence.InspectParent(
                binaryPath,
                accountCount,
                verifyOrder: true);
            var profileManifests = resolvedProfiles
                .Select(profile => new CorpusProfileManifest(
                    profile.Name,
                    profile.AccountCount,
                    checked(profile.AccountCount * CorpusFormat.AccountKeyByteLength),
                    ArtifactEvidence.HashPrefix(
                        binaryPath,
                        checked(profile.AccountCount * CorpusFormat.AccountKeyByteLength))))
                .ToArray();

            TextArtifactManifest? humanManifest = null;
            if (humanPath is not null)
            {
                var humanEvidence = ArtifactEvidence.HashFile(humanPath);
                humanManifest = new TextArtifactManifest(
                    CorpusFormat.HumanArtifactName,
                    humanEvidence.ByteLength,
                    humanEvidence.Sha256);
            }

            var manifest = new CorpusManifest(
                CorpusFormat.ManifestFormat,
                new AccountKeyManifest(
                    CorpusFormat.AccountKeyAlgorithm,
                    CorpusFormat.AccountKeyByteLength),
                new SourceJournalManifest(
                    CorpusFormat.JournalFormat,
                    sorted.SourceByteLength,
                    sorted.SourceSha256),
                new ParentCorpusManifest(
                    CorpusFormat.BinaryArtifactName,
                    accountCount,
                    parentEvidence.ByteLength,
                    parentEvidence.Sha256,
                    parentEvidence.CorpusFingerprint),
                profileManifests,
                humanManifest);

            CorpusManifestCodec.WriteCanonical(
                Path.Combine(stagingDirectory, CorpusFormat.ManifestName),
                manifest);
            PublicArtifactPrivacy.VerifyDirectory(
                stagingDirectory,
                ExpectedArtifacts(options.WriteHumanReadableHex));

            Directory.Move(stagingDirectory, outputDirectory);
            return new CorpusFreezeResult(
                outputDirectory,
                Path.Combine(outputDirectory, CorpusFormat.ManifestName),
                accountCount,
                sorted.InitialRunCount,
                manifest);
        }
        catch
        {
            if (Directory.Exists(stagingDirectory))
            {
                Directory.Delete(stagingDirectory, recursive: true);
            }

            throw;
        }
    }

    private static (string JournalPath, string OutputDirectory, IReadOnlyList<CorpusProfileRequest> Profiles)
        Validate(CorpusFreezeOptions options)
    {
        var journalPath = Path.GetFullPath(
            string.IsNullOrWhiteSpace(options.JournalPath)
                ? throw new ArgumentException("A sanitized journal path is required.", nameof(options))
                : options.JournalPath);
        if (!File.Exists(journalPath))
        {
            throw new FileNotFoundException("The sanitized observation journal does not exist.", journalPath);
        }
        PrivateWorkspacePermissions.ValidateRegularFile(journalPath);

        var outputDirectory = Path.GetFullPath(
            string.IsNullOrWhiteSpace(options.OutputDirectory)
                ? throw new ArgumentException("An output directory is required.", nameof(options))
                : options.OutputDirectory);
        if (File.Exists(outputDirectory) || Directory.Exists(outputDirectory))
        {
            throw new IOException(
                "The frozen output directory must not already exist; corpus publication never overwrites.");
        }

        if (options.MemoryBudgetBytes is < MinimumMemoryBudgetBytes or > MaximumMemoryBudgetBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The memory budget must be between {MinimumMemoryBudgetBytes} and "
                + $"{MaximumMemoryBudgetBytes} bytes.");
        }

        if (options.MergeFanIn is < 2 or > 128)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The merge fan-in must be between 2 and 128.");
        }

        if (options.Profiles.Count > MaximumProfileCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"At most {MaximumProfileCount} exact-prefix profiles may be frozen.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var profile in options.Profiles)
        {
            ArgumentNullException.ThrowIfNull(profile);
            if (!names.Add(profile.Name))
            {
                throw new ArgumentException($"Duplicate corpus profile name '{profile.Name}'.", nameof(options));
            }
        }

        return (journalPath, outputDirectory, options.Profiles);
    }

    private static CorpusProfileRequest[] ResolveProfiles(
        IReadOnlyList<CorpusProfileRequest> requested,
        long accountCount)
    {
        var profiles = requested.Count == 0
            ? [new CorpusProfileRequest("parent", accountCount)]
            : requested.ToArray();
        foreach (var profile in profiles)
        {
            if (profile.AccountCount > accountCount)
            {
                throw new InvalidDataException(
                    $"Profile '{profile.Name}' requires {profile.AccountCount} accounts, but the frozen "
                    + $"parent contains only {accountCount}.");
            }
        }

        return profiles
            .OrderBy(static profile => profile.AccountCount)
            .ThenBy(static profile => profile.Name, StringComparer.Ordinal)
            .ToArray();
    }

    private static long FoldLatestExplicitActive(
        string sortedRunPath,
        string binaryPath,
        string? humanPath)
    {
        using var reader = new ExternalObservationSorter.ObservationRunReader(sortedRunPath);
        using var binary = new FileStream(
            binaryPath,
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                Options = FileOptions.SequentialScan,
            });
        using var humanStream = humanPath is null
            ? null
            : new FileStream(
                humanPath,
                new FileStreamOptions
                {
                    Mode = FileMode.CreateNew,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    Options = FileOptions.SequentialScan,
                });
        using var human = humanStream is null
            ? null
            : new StreamWriter(humanStream, new UTF8Encoding(false), bufferSize: 64 * 1024, leaveOpen: true)
            {
                NewLine = "\n",
            };

        ObservationSortRecord? current = null;
        ObservationSortRecord? priorSorted = null;
        long accountCount = 0;
        while (reader.TryRead(out var record))
        {
            if (priorSorted is not null
                && ExternalObservationSorter.ObservationSortRecordComparer.Instance.Compare(
                    priorSorted,
                    record) > 0)
            {
                throw new InvalidDataException("A private observation run is not sorted.");
            }

            priorSorted = record;
            if (current is null)
            {
                current = record;
                continue;
            }

            var keyComparison = current.AccountKey.AsSpan().SequenceCompareTo(record.AccountKey);
            if (keyComparison == 0)
            {
                if (!string.Equals(current.Did, record.Did, StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "A strict SHA-256 collision was detected between two distinct canonical DIDs.");
                }

                if (record.Ordinal <= current.Ordinal)
                {
                    throw new InvalidDataException(
                        "Observations for one canonical DID contain inconsistent local ordinals.");
                }

                current = record;
                continue;
            }

            accountCount = FlushAccount(current, binary, human, accountCount);
            current = record;
        }

        if (current is not null)
        {
            accountCount = FlushAccount(current, binary, human, accountCount);
        }

        human?.Flush();
        humanStream?.Flush(flushToDisk: true);
        binary.Flush(flushToDisk: true);
        return accountCount;
    }

    private static long FlushAccount(
        ObservationSortRecord observation,
        FileStream binary,
        StreamWriter? human,
        long accountCount)
    {
        if (observation.Status != ExplicitLifecycleStatus.Active)
        {
            return accountCount;
        }

        binary.Write(observation.AccountKey);
        human?.WriteLine(Convert.ToHexString(observation.AccountKey).ToLowerInvariant());
        return checked(accountCount + 1);
    }

    private static HashSet<string> ExpectedArtifacts(bool includeHuman)
        => includeHuman
            ? new HashSet<string>(
                [CorpusFormat.BinaryArtifactName, CorpusFormat.HumanArtifactName, CorpusFormat.ManifestName],
                StringComparer.Ordinal)
            : new HashSet<string>(
                [CorpusFormat.BinaryArtifactName, CorpusFormat.ManifestName],
                StringComparer.Ordinal);
}
