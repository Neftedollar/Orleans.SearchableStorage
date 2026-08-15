using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public sealed record PrivateRoutingExportOptions
{
    public required string AcquisitionManifestPath { get; init; }

    public required string CorpusManifestPath { get; init; }

    public required string ProfileName { get; init; }

    public required string OutputDirectory { get; init; }

    public long MemoryBudgetBytes { get; init; } = 64L * 1024 * 1024;

    public int MergeFanIn { get; init; } = 32;

    public int BatchRecordLimit { get; init; } = 500;
}

public sealed record RoutingProfileBinding(
    string Name,
    long AccountCount,
    long ByteLength,
    string PrefixSha256);

public sealed record RoutingArtifactEvidence(
    string Artifact,
    string Format,
    long AccountCount,
    long ByteLength,
    string Sha256,
    string AccountKeyProjectionSha256);

public sealed record RoutingBatchEvidence(
    int Batch,
    long FirstOrdinal,
    int RecordCount,
    long ByteOffset,
    long ByteLength,
    string Sha256);

public sealed record PrivateRoutingManifest(
    string Format,
    string AcquisitionManifestSha256,
    PrivateArtifactEvidence SourceJournal,
    string CorpusManifestSha256,
    string ParentCorpusSha256,
    string ParentCorpusFingerprint,
    RoutingProfileBinding Profile,
    int BatchRecordLimit,
    IReadOnlyList<RoutingBatchEvidence> Batches,
    RoutingArtifactEvidence Routing);

public sealed record PrivateRoutingExportResult(
    string OutputDirectory,
    string RoutingPath,
    string ManifestPath,
    PrivateRoutingManifest Manifest);

public sealed record PrivateRoutingExpectedProfile(
    string Name,
    long AccountCount,
    string PrefixSha256);

public static class PrivateRoutingExporter
{
    public static PrivateRoutingExportResult Export(PrivateRoutingExportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);
        var acquisitionPath = Path.GetFullPath(options.AcquisitionManifestPath);
        PrivateArtifactIO.EnsurePrivateDirectory(Path.GetDirectoryName(acquisitionPath)!);
        var acquisition = PrivateArtifactIO.ReadCanonical<AcquisitionManifest>(acquisitionPath);
        AcquisitionManifestValidation.Validate(acquisition);
        var journalPath = Path.Combine(
            Path.GetDirectoryName(acquisitionPath)!,
            AcquisitionContract.JournalFileName);
        var journalEvidence = PrivateArtifactIO.InspectPrivate(journalPath);
        if (journalEvidence != acquisition.Journal)
        {
            throw new InvalidDataException("The acquisition journal does not match its successful manifest.");
        }

        PrivateArtifactIO.EnsurePrivateMode(journalPath);
        var corpusManifestPath = Path.GetFullPath(options.CorpusManifestPath);
        _ = CorpusVerifier.Verify(corpusManifestPath, deep: true, journalPath);
        var corpus = CorpusManifestCodec.ReadCanonical(corpusManifestPath);
        var profile = corpus.Profiles.SingleOrDefault(
            profile => string.Equals(profile.Name, options.ProfileName, StringComparison.Ordinal))
            ?? throw new InvalidDataException("The requested exact-prefix corpus profile does not exist.");
        if (profile.AccountCount > int.MaxValue && options.BatchRecordLimit <= 0)
        {
            throw new InvalidDataException("The private routing batch contract is invalid.");
        }

        var output = Path.GetFullPath(options.OutputDirectory);
        if (File.Exists(output) || Directory.Exists(output))
        {
            throw new IOException("The private routing output directory must not already exist.");
        }

        var parent = Path.GetDirectoryName(output)
            ?? throw new ArgumentException("The output directory needs a parent.", nameof(options));
        Directory.CreateDirectory(parent);
        var staging = Path.Combine(parent, $".{Path.GetFileName(output)}.tmp-{Guid.NewGuid():N}");
        PrivateArtifactIO.EnsurePrivateDirectory(staging);
        try
        {
            var work = Path.Combine(staging, ".private-sort-work");
            PrivateArtifactIO.EnsurePrivateDirectory(work);
            var sorted = ExternalObservationSorter.Sort(
                journalPath,
                work,
                options.MemoryBudgetBytes,
                options.MergeFanIn);
            if (sorted.SourceByteLength != acquisition.Journal.ByteLength
                || !string.Equals(sorted.SourceSha256, acquisition.Journal.Sha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The routing sort did not consume the exact acquisition journal.");
            }

            var routePath = Path.Combine(staging, AcquisitionContract.RoutingFileName);
            var (routing, batches) = WriteExactPrefixRoute(
                sorted.SortedRunPath,
                corpusManifestPath,
                profile,
                routePath,
                options.BatchRecordLimit);
            Directory.Delete(work, recursive: true);

            var manifest = new PrivateRoutingManifest(
                AcquisitionContract.RoutingManifestFormat,
                PrivateArtifactIO.InspectPrivate(acquisitionPath).Sha256,
                acquisition.Journal,
                PrivateArtifactIO.Inspect(corpusManifestPath).Sha256,
                corpus.Parent.Sha256,
                corpus.Parent.CorpusFingerprint,
                new RoutingProfileBinding(
                    profile.Name,
                    profile.AccountCount,
                    profile.ByteLength,
                    profile.PrefixSha256),
                options.BatchRecordLimit,
                batches,
                routing);
            ValidateManifest(manifest);
            var routingManifestPath = Path.Combine(
                staging,
                AcquisitionContract.RoutingManifestFileName);
            PrivateArtifactIO.WriteNewPrivateFile(
                routingManifestPath,
                PrivateArtifactIO.SerializeCanonical(manifest));
            Directory.Move(staging, output);
            return new PrivateRoutingExportResult(
                output,
                Path.Combine(output, AcquisitionContract.RoutingFileName),
                Path.Combine(output, AcquisitionContract.RoutingManifestFileName),
                manifest);
        }
        catch
        {
            if (Directory.Exists(staging))
            {
                Directory.Delete(staging, recursive: true);
            }

            throw;
        }
    }

    public static PrivateRoutingManifest Verify(string manifestPath)
    {
        var fullManifestPath = Path.GetFullPath(manifestPath);
        PrivateArtifactIO.EnsurePrivateDirectory(Path.GetDirectoryName(fullManifestPath)!);
        var manifest = PrivateArtifactIO.ReadCanonical<PrivateRoutingManifest>(fullManifestPath);
        ValidateManifest(manifest);
        var directory = Path.GetDirectoryName(fullManifestPath)!;
        var routePath = Path.Combine(directory, AcquisitionContract.RoutingFileName);
        PrivateArtifactIO.EnsurePrivateMode(routePath);
        var evidence = PrivateArtifactIO.InspectPrivate(routePath);
        if (evidence.ByteLength != manifest.Routing.ByteLength
            || !string.Equals(evidence.Sha256, manifest.Routing.Sha256, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The private routing artifact hash or length changed.");
        }

        VerifyRouteContents(routePath, manifest);
        return manifest;
    }

    public static PrivateRoutingManifest Verify(
        string manifestPath,
        PrivateRoutingExpectedProfile expectedProfile)
    {
        ArgumentNullException.ThrowIfNull(expectedProfile);
        var manifest = Verify(manifestPath);
        if (!string.Equals(manifest.Profile.Name, expectedProfile.Name, StringComparison.Ordinal)
            || manifest.Profile.AccountCount != expectedProfile.AccountCount
            || !string.Equals(
                manifest.Profile.PrefixSha256,
                expectedProfile.PrefixSha256,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The private route does not match the independently configured exact profile identity.");
        }

        return manifest;
    }

    private static (RoutingArtifactEvidence Routing, IReadOnlyList<RoutingBatchEvidence> Batches)
        WriteExactPrefixRoute(
            string sortedRunPath,
            string corpusManifestPath,
            CorpusProfileManifest profile,
            string routePath,
            int batchRecordLimit)
    {
        var corpusDirectory = Path.GetDirectoryName(corpusManifestPath)!;
        var binaryPath = Path.Combine(corpusDirectory, CorpusFormat.BinaryArtifactName);
        using var keys = new FileStream(
            binaryPath,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });
        using var sortedRun = new ExternalObservationSorter.ObservationRunReader(sortedRunPath);
        var sorted = new GroupedObservationReader(sortedRun);
        var routeOptions = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.Read,
            BufferSize = 64 * 1024,
            Options = FileOptions.SequentialScan,
        };
        if (!OperatingSystem.IsWindows())
        {
            routeOptions.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        using var route = new FileStream(routePath, routeOptions);
        PrivateWorkspacePermissions.ValidateRegularFile(route);
        using var projectionHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var batches = new List<RoutingBatchEvidence>();
        var expectedKey = new byte[CorpusFormat.AccountKeyByteLength];
        var routeCount = 0L;
        var batchStartOffset = 0L;
        var batchFirstOrdinal = 1L;
        var batchCount = 0;
        using var batchHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var active = ReadNextActive(sorted);
        while (routeCount < profile.AccountCount)
        {
            keys.ReadExactly(expectedKey);
            if (active is null
                || active.AccountKey.AsSpan().SequenceCompareTo(expectedKey) != 0)
            {
                throw new InvalidDataException(
                    "The private DID fold does not reproduce the frozen profile's exact ordered prefix.");
            }

            var line = EncodeRouteLine(routeCount + 1, active.AccountKey, active.Did);
            route.Write(line);
            batchHash.AppendData(line);
            projectionHash.AppendData(active.AccountKey);
            routeCount++;
            batchCount++;
            if (batchCount == batchRecordLimit || routeCount == profile.AccountCount)
            {
                var endOffset = route.Position;
                batches.Add(
                    new RoutingBatchEvidence(
                        batches.Count + 1,
                        batchFirstOrdinal,
                        batchCount,
                        batchStartOffset,
                        checked(endOffset - batchStartOffset),
                        PrivateArtifactIO.LowerHex(batchHash.GetHashAndReset())));
                batchStartOffset = endOffset;
                batchFirstOrdinal = routeCount + 1;
                batchCount = 0;
            }

            active = ReadNextActive(sorted);
        }

        route.Flush(flushToDisk: true);
        PrivateArtifactIO.EnsurePrivateMode(routePath);
        var projectionSha = PrivateArtifactIO.LowerHex(projectionHash.GetHashAndReset());
        if (!string.Equals(projectionSha, profile.PrefixSha256, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The private route key projection does not match the profile prefix hash.");
        }

        var evidence = PrivateArtifactIO.InspectPrivate(routePath);
        return (
            new RoutingArtifactEvidence(
                AcquisitionContract.RoutingFileName,
                AcquisitionContract.RoutingArtifactFormat,
                routeCount,
                evidence.ByteLength,
                evidence.Sha256,
                projectionSha),
            batches);
    }

    private static ObservationSortRecord? ReadNextActive(GroupedObservationReader reader)
    {
        while (reader.TryRead(out var first))
        {
            var latest = first;
            while (reader.TryRead(out var next))
            {
                var comparison = first.AccountKey.AsSpan().SequenceCompareTo(next.AccountKey);
                if (comparison != 0)
                {
                    reader.PushBack(next);
                    break;
                }

                if (!string.Equals(first.Did, next.Did, StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "A strict SHA-256 collision exists between distinct canonical DIDs.");
                }

                if (next.Ordinal <= latest.Ordinal)
                {
                    throw new InvalidDataException("A DID contains non-increasing local ordinals.");
                }

                latest = next;
            }

            if (latest.Status == ExplicitLifecycleStatus.Active)
            {
                return latest;
            }
        }

        return null;
    }

    private static byte[] EncodeRouteLine(long ordinal, byte[] accountKey, string did)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            writer.WriteStartObject();
            writer.WriteNumber("ordinal", ordinal);
            writer.WriteString("accountKey", PrivateArtifactIO.LowerHex(accountKey));
            writer.WriteString("did", did);
            writer.WriteEndObject();
        }

        var line = new byte[buffer.WrittenCount + 1];
        buffer.WrittenSpan.CopyTo(line);
        line[^1] = (byte)'\n';
        return line;
    }

    private static void VerifyRouteContents(string routePath, PrivateRoutingManifest manifest)
    {
        using var stream = File.OpenRead(routePath);
        using var projectionHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var batchIndex = 0;
        var batchCount = 0;
        var batchStart = 0L;
        using var batchHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[]? priorKey = null;
        long ordinal = 0;
        foreach (var line in ReadBoundedLines(stream, 16 * 1024))
        {
            ordinal++;
            var (lineOrdinal, key, did) = ParseRouteLine(line);
            if (lineOrdinal != ordinal
                || !string.Equals(
                    AccountKey.FromDid(did).ToString(),
                    PrivateArtifactIO.LowerHex(key),
                    StringComparison.Ordinal)
                || (priorKey is not null && priorKey.AsSpan().SequenceCompareTo(key) >= 0))
            {
                throw new InvalidDataException("The private route is not the canonical ordered DID-to-key mapping.");
            }

            priorKey = key;
            projectionHash.AppendData(key);
            batchHash.AppendData(line.Span);
            batchHash.AppendData("\n"u8);
            batchCount++;
            if (batchCount == manifest.BatchRecordLimit || ordinal == manifest.Routing.AccountCount)
            {
                if (batchIndex >= manifest.Batches.Count)
                {
                    throw new InvalidDataException("The private route contains an unmanifested batch.");
                }

                var batch = manifest.Batches[batchIndex];
                var end = stream.Position;
                if (batch.Batch != batchIndex + 1
                    || batch.FirstOrdinal != ordinal - batchCount + 1
                    || batch.RecordCount != batchCount
                    || batch.ByteOffset != batchStart
                    || batch.ByteLength != end - batchStart
                    || !string.Equals(
                        batch.Sha256,
                        PrivateArtifactIO.LowerHex(batchHash.GetHashAndReset()),
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException("A private routing batch does not match its evidence.");
                }

                batchIndex++;
                batchCount = 0;
                batchStart = end;
            }
        }

        var projectionSha = PrivateArtifactIO.LowerHex(projectionHash.GetHashAndReset());
        if (ordinal != manifest.Routing.AccountCount
            || batchIndex != manifest.Batches.Count
            || !string.Equals(projectionSha, manifest.Profile.PrefixSha256, StringComparison.Ordinal)
            || !string.Equals(projectionSha, manifest.Routing.AccountKeyProjectionSha256, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The private route does not prove the exact frozen profile prefix.");
        }
    }

    private static (long Ordinal, byte[] Key, string Did) ParseRouteLine(ReadOnlyMemory<byte> line)
    {
        using var document = JsonDocument.Parse(line);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException("A private route line must be an object.");
        }

        var properties = root.EnumerateObject().ToArray();
        if (properties.Length != 3
            || properties[0].Name != "ordinal"
            || properties[1].Name != "accountKey"
            || properties[2].Name != "did"
            || !properties[0].Value.TryGetInt64(out var ordinal)
            || ordinal <= 0
            || properties[1].Value.ValueKind != JsonValueKind.String
            || properties[2].Value.ValueKind != JsonValueKind.String)
        {
            throw new InvalidDataException("A private route line is outside the canonical schema.");
        }

        var keyText = properties[1].Value.GetString()!;
        if (!AccountKey.TryParse(keyText, out _))
        {
            throw new InvalidDataException("A private route account key is not canonical lowercase hexadecimal.");
        }

        var key = Convert.FromHexString(keyText);

        var did = properties[2].Value.GetString()!;
        _ = AccountKey.FromDid(did);
        var canonical = EncodeRouteLine(ordinal, key, did);
        if (!canonical.AsSpan(0, canonical.Length - 1).SequenceEqual(line.Span))
        {
            throw new InvalidDataException("A private route line is not in its unique canonical encoding.");
        }

        return (ordinal, key, did);
    }

    private static IEnumerable<ReadOnlyMemory<byte>> ReadBoundedLines(Stream stream, int maximumLineBytes)
    {
        var line = new byte[maximumLineBytes];
        var length = 0;
        while (true)
        {
            var value = stream.ReadByte();
            if (value < 0)
            {
                if (length != 0)
                {
                    throw new InvalidDataException("The private route must end each record with LF.");
                }

                yield break;
            }

            if (value == '\n')
            {
                yield return line.AsMemory(0, length).ToArray();
                length = 0;
                continue;
            }

            if (length == line.Length)
            {
                throw new InvalidDataException("A private route line exceeds its byte bound.");
            }

            line[length++] = (byte)value;
        }
    }

    private static void ValidateManifest(PrivateRoutingManifest manifest)
    {
        if (!string.Equals(manifest.Format, AcquisitionContract.RoutingManifestFormat, StringComparison.Ordinal)
            || !string.Equals(manifest.Routing.Artifact, AcquisitionContract.RoutingFileName, StringComparison.Ordinal)
            || !string.Equals(manifest.Routing.Format, AcquisitionContract.RoutingArtifactFormat, StringComparison.Ordinal)
            || manifest.Profile.AccountCount <= 0
            || manifest.Profile.AccountCount != manifest.Routing.AccountCount
            || manifest.Profile.ByteLength != checked(manifest.Profile.AccountCount * CorpusFormat.AccountKeyByteLength)
            || manifest.BatchRecordLimit is < 1 or > 1000
            || manifest.Batches.Count != (manifest.Profile.AccountCount + manifest.BatchRecordLimit - 1)
                / manifest.BatchRecordLimit)
        {
            throw new InvalidDataException("The private routing manifest is inconsistent.");
        }

        PrivateArtifactIO.ValidateSha256(manifest.AcquisitionManifestSha256, "acquisition manifest SHA-256");
        PrivateArtifactIO.ValidateSha256(manifest.SourceJournal.Sha256, "source journal SHA-256");
        PrivateArtifactIO.ValidateSha256(manifest.CorpusManifestSha256, "corpus manifest SHA-256");
        PrivateArtifactIO.ValidateSha256(manifest.ParentCorpusSha256, "parent corpus SHA-256");
        PrivateArtifactIO.ValidateSha256(manifest.ParentCorpusFingerprint, "parent corpus fingerprint");
        PrivateArtifactIO.ValidateSha256(manifest.Profile.PrefixSha256, "profile prefix SHA-256");
        PrivateArtifactIO.ValidateSha256(manifest.Routing.Sha256, "routing artifact SHA-256");
        PrivateArtifactIO.ValidateSha256(
            manifest.Routing.AccountKeyProjectionSha256,
            "routing account-key projection SHA-256");
        if (!string.Equals(
                manifest.Profile.PrefixSha256,
                manifest.Routing.AccountKeyProjectionSha256,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException("The private route is not bound to the exact profile prefix.");
        }

        long expectedOrdinal = 1;
        long expectedOffset = 0;
        for (var index = 0; index < manifest.Batches.Count; index++)
        {
            var batch = manifest.Batches[index];
            PrivateArtifactIO.ValidateSha256(batch.Sha256, "routing batch SHA-256");
            if (batch.Batch != index + 1
                || batch.FirstOrdinal != expectedOrdinal
                || batch.RecordCount is < 1
                || batch.RecordCount > manifest.BatchRecordLimit
                || batch.ByteOffset != expectedOffset
                || batch.ByteLength <= 0)
            {
                throw new InvalidDataException("Private routing batches do not form a bounded contiguous partition.");
            }

            expectedOrdinal = checked(expectedOrdinal + batch.RecordCount);
            expectedOffset = checked(expectedOffset + batch.ByteLength);
        }

        if (expectedOrdinal != manifest.Routing.AccountCount + 1
            || expectedOffset != manifest.Routing.ByteLength)
        {
            throw new InvalidDataException("Private routing batches do not cover the exact routing artifact.");
        }
    }

    private static void ValidateOptions(PrivateRoutingExportOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.AcquisitionManifestPath)
            || string.IsNullOrWhiteSpace(options.CorpusManifestPath)
            || string.IsNullOrWhiteSpace(options.ProfileName)
            || string.IsNullOrWhiteSpace(options.OutputDirectory))
        {
            throw new ArgumentException("Acquisition, corpus, profile, and output identities are required.");
        }

        if (options.MemoryBudgetBytes is < 4096 or > 4L * 1024 * 1024 * 1024
            || options.MergeFanIn is < 2 or > 128
            || options.BatchRecordLimit is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Private routing bounds are invalid.");
        }
    }

    private sealed class GroupedObservationReader(
        ExternalObservationSorter.ObservationRunReader reader)
    {
        private ObservationSortRecord? _pending;

        public bool TryRead(out ObservationSortRecord record)
        {
            if (_pending is not null)
            {
                record = _pending;
                _pending = null;
                return true;
            }

            return reader.TryRead(out record);
        }

        public void PushBack(ObservationSortRecord record)
        {
            if (_pending is not null)
            {
                throw new InvalidOperationException("Only one sorted observation may be pushed back.");
            }

            _pending = record;
        }
    }
}
