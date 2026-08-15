using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

internal sealed record ObservationSortRecord(
    byte[] AccountKey,
    string Did,
    long Ordinal,
    ExplicitLifecycleStatus Status);

internal sealed record ObservationSortResult(
    string SortedRunPath,
    int InitialRunCount,
    long SourceByteLength,
    string SourceSha256);

internal static class ExternalObservationSorter
{
    private const int EstimatedManagedRecordOverhead = 96;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static ObservationSortResult Sort(
        string journalPath,
        string workDirectory,
        long memoryBudgetBytes,
        int mergeFanIn)
    {
        PrivateWorkspacePermissions.ValidateRegularFile(journalPath);
        PrivateWorkspacePermissions.ValidateDirectory(workDirectory);
        var runs = new SpillRunAccumulator(workDirectory, mergeFanIn);
        var batch = new List<ObservationSortRecord>();
        long batchBytes = 0;
        long lastOrdinal = 0;
        long lineNumber = 0;
        long sourceByteLength;
        string sourceSha256;

        using (var reader = new BoundedUtf8LineReader(
                   journalPath,
                   SanitizedObservationParser.MaximumLineBytes))
        {
            while (reader.TryReadLine(out var line))
            {
                lineNumber++;
                var observation = SanitizedObservationParser.Parse(line, lineNumber);
                if (observation.Ordinal <= lastOrdinal)
                {
                    throw new InvalidDataException(
                        $"Sanitized journal line {lineNumber}: 'ordinal' must increase strictly; "
                        + $"observed {observation.Ordinal} after {lastOrdinal}.");
                }

                lastOrdinal = observation.Ordinal;
                var record = CreateRecord(observation);
                var estimatedBytes = checked(
                    CorpusFormat.AccountKeyByteLength
                    + sizeof(long)
                    + sizeof(byte)
                    + sizeof(ushort)
                    + Encoding.UTF8.GetByteCount(record.Did)
                    + EstimatedManagedRecordOverhead);

                if (batch.Count > 0 && checked(batchBytes + estimatedBytes) > memoryBudgetBytes)
                {
                    runs.Add(WriteRun(workDirectory, runs.InitialRunCount, batch));
                    batch.Clear();
                    batchBytes = 0;
                }

                batch.Add(record);
                batchBytes = checked(batchBytes + estimatedBytes);
            }

            sourceByteLength = reader.ByteLength;
            sourceSha256 = reader.GetCompletedSha256();
        }

        if (batch.Count > 0)
        {
            runs.Add(WriteRun(workDirectory, runs.InitialRunCount, batch));
        }

        if (runs.InitialRunCount == 0)
        {
            throw new InvalidDataException("The sanitized observation journal is empty.");
        }

        var initialRunCount = runs.InitialRunCount;
        var sortedRun = runs.Complete();
        return new ObservationSortResult(
            sortedRun,
            initialRunCount,
            sourceByteLength,
            sourceSha256);
    }

    private static ObservationSortRecord CreateRecord(SanitizedLifecycleObservation observation)
    {
        var didBytes = Encoding.UTF8.GetBytes(observation.Did);
        var key = SHA256.HashData(didBytes);
        return new ObservationSortRecord(key, observation.Did, observation.Ordinal, observation.Status);
    }

    private static string WriteRun(
        string workDirectory,
        int runNumber,
        List<ObservationSortRecord> records)
    {
        records.Sort(ObservationSortRecordComparer.Instance);
        var path = Path.Combine(workDirectory, $"run-0-{runNumber:D8}.bin");
        using var writer = new ObservationRunWriter(path);
        foreach (var record in records)
        {
            writer.Write(record);
        }

        writer.FlushToDisk();
        return path;
    }

    private static string MergePasses(
        List<string> initialRuns,
        string workDirectory,
        int mergeFanIn)
    {
        var current = initialRuns;
        var pass = 1;
        while (current.Count > 1)
        {
            var next = new List<string>((current.Count + mergeFanIn - 1) / mergeFanIn);
            for (var offset = 0; offset < current.Count; offset += mergeFanIn)
            {
                var count = Math.Min(mergeFanIn, current.Count - offset);
                if (count == 1)
                {
                    next.Add(current[offset]);
                    continue;
                }

                var inputs = current.GetRange(offset, count);
                var output = Path.Combine(workDirectory, $"run-{pass}-{next.Count:D8}.bin");
                MergeRuns(inputs, output);
                foreach (var input in inputs)
                {
                    File.Delete(input);
                }

                next.Add(output);
            }

            current = next;
            pass++;
        }

        return current[0];
    }

    private static void MergeRuns(IReadOnlyList<string> inputPaths, string outputPath)
    {
        var readers = inputPaths.Select(static path => new ObservationRunReader(path)).ToArray();
        try
        {
            var queue = new PriorityQueue<MergeCursor, MergePriority>(MergePriorityComparer.Instance);
            for (var index = 0; index < readers.Length; index++)
            {
                if (readers[index].TryRead(out var record))
                {
                    queue.Enqueue(new MergeCursor(index, readers[index], record), new MergePriority(record, index));
                }
            }

            using var writer = new ObservationRunWriter(outputPath);
            while (queue.TryDequeue(out var cursor, out _))
            {
                writer.Write(cursor.Current);
                if (cursor.Reader.TryRead(out var next))
                {
                    cursor.Current = next;
                    queue.Enqueue(cursor, new MergePriority(next, cursor.RunIndex));
                }
            }

            writer.FlushToDisk();
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader.Dispose();
            }
        }
    }

    /// <summary>
    /// Compacts spill runs while the journal is still being read. At most fan-in minus one paths
    /// are retained per logarithmic level, instead of one managed path for every source chunk.
    /// </summary>
    private sealed class SpillRunAccumulator(string workDirectory, int mergeFanIn)
    {
        private readonly List<List<string>> _levels = [];
        private long _mergeNumber;

        public int InitialRunCount { get; private set; }

        public void Add(string path)
        {
            InitialRunCount = checked(InitialRunCount + 1);
            AddAtLevel(path, 0);
        }

        public string Complete()
        {
            var remaining = _levels.SelectMany(static level => level).ToList();
            return MergePasses(remaining, workDirectory, mergeFanIn);
        }

        private void AddAtLevel(string path, int level)
        {
            while (_levels.Count <= level)
            {
                _levels.Add([]);
            }

            var paths = _levels[level];
            paths.Add(path);
            if (paths.Count < mergeFanIn)
            {
                return;
            }

            var merged = Path.Combine(
                workDirectory,
                $"run-online-{level + 1:D4}-{_mergeNumber++:D12}.bin");
            MergeRuns(paths, merged);
            foreach (var input in paths)
            {
                File.Delete(input);
            }

            paths.Clear();
            AddAtLevel(merged, level + 1);
        }
    }

    private sealed class MergeCursor(
        int runIndex,
        ObservationRunReader reader,
        ObservationSortRecord current)
    {
        public int RunIndex { get; } = runIndex;

        public ObservationRunReader Reader { get; } = reader;

        public ObservationSortRecord Current { get; set; } = current;
    }

    private readonly record struct MergePriority(ObservationSortRecord Record, int RunIndex);

    private sealed class MergePriorityComparer : IComparer<MergePriority>
    {
        public static MergePriorityComparer Instance { get; } = new();

        public int Compare(MergePriority left, MergePriority right)
        {
            var comparison = ObservationSortRecordComparer.Instance.Compare(left.Record, right.Record);
            return comparison != 0 ? comparison : left.RunIndex.CompareTo(right.RunIndex);
        }
    }

    internal sealed class ObservationSortRecordComparer : IComparer<ObservationSortRecord>
    {
        public static ObservationSortRecordComparer Instance { get; } = new();

        public int Compare(ObservationSortRecord? left, ObservationSortRecord? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }

            if (left is null)
            {
                return -1;
            }

            if (right is null)
            {
                return 1;
            }

            var comparison = left.AccountKey.AsSpan().SequenceCompareTo(right.AccountKey);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = string.CompareOrdinal(left.Did, right.Did);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = left.Ordinal.CompareTo(right.Ordinal);
            return comparison != 0 ? comparison : left.Status.CompareTo(right.Status);
        }
    }

    internal sealed class ObservationRunReader : IDisposable
    {
        private const int HeaderBytes = CorpusFormat.AccountKeyByteLength + sizeof(long) + sizeof(byte) + sizeof(ushort);
        private readonly FileStream _stream;
        private readonly byte[] _header = new byte[HeaderBytes];

        public ObservationRunReader(string path)
        {
            PrivateWorkspacePermissions.ValidateRegularFile(path);
            _stream = new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    Options = FileOptions.SequentialScan,
                });
            PrivateWorkspacePermissions.ValidateRegularFile(_stream);
        }

        public bool TryRead(out ObservationSortRecord record)
        {
            var first = _stream.ReadByte();
            if (first < 0)
            {
                record = null!;
                return false;
            }

            _header[0] = (byte)first;
            _stream.ReadExactly(_header.AsSpan(1));
            var key = _header.AsSpan(0, CorpusFormat.AccountKeyByteLength).ToArray();
            var ordinal = BinaryPrimitives.ReadInt64BigEndian(
                _header.AsSpan(CorpusFormat.AccountKeyByteLength, sizeof(long)));
            var statusByte = _header[CorpusFormat.AccountKeyByteLength + sizeof(long)];
            if (statusByte > (byte)ExplicitLifecycleStatus.Active)
            {
                throw new InvalidDataException("A private sort run contains an invalid lifecycle status.");
            }

            var didLength = BinaryPrimitives.ReadUInt16BigEndian(_header.AsSpan(HeaderBytes - sizeof(ushort)));
            if (didLength == 0)
            {
                throw new InvalidDataException("A private sort run contains an empty DID.");
            }

            var didBytes = new byte[didLength];
            _stream.ReadExactly(didBytes);
            string did;
            try
            {
                did = StrictUtf8.GetString(didBytes);
            }
            catch (DecoderFallbackException exception)
            {
                throw new InvalidDataException("A private sort run contains invalid UTF-8.", exception);
            }

            record = new ObservationSortRecord(
                key,
                did,
                ordinal,
                (ExplicitLifecycleStatus)statusByte);
            return true;
        }

        public void Dispose() => _stream.Dispose();
    }

    private sealed class ObservationRunWriter : IDisposable
    {
        private const int HeaderBytes = CorpusFormat.AccountKeyByteLength + sizeof(long) + sizeof(byte) + sizeof(ushort);
        private readonly FileStream _stream;

        public ObservationRunWriter(string path)
        {
            var options = new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                Options = FileOptions.SequentialScan,
            };
            PrivateWorkspacePermissions.ApplyPrivateCreateMode(options);
            _stream = new FileStream(
                path,
                options);
            PrivateWorkspacePermissions.ValidateRegularFile(_stream);
        }

        public void Write(ObservationSortRecord record)
        {
            var didBytes = Encoding.UTF8.GetBytes(record.Did);
            if (didBytes.Length > ushort.MaxValue)
            {
                throw new InvalidDataException("A DID is too large for the private sort-run format.");
            }

            Span<byte> header = stackalloc byte[HeaderBytes];
            record.AccountKey.CopyTo(header);
            BinaryPrimitives.WriteInt64BigEndian(
                header.Slice(CorpusFormat.AccountKeyByteLength, sizeof(long)),
                record.Ordinal);
            header[CorpusFormat.AccountKeyByteLength + sizeof(long)] = (byte)record.Status;
            BinaryPrimitives.WriteUInt16BigEndian(header[^sizeof(ushort)..], (ushort)didBytes.Length);
            _stream.Write(header);
            _stream.Write(didBytes);
        }

        public void FlushToDisk() => _stream.Flush(flushToDisk: true);

        public void Dispose() => _stream.Dispose();
    }
}
