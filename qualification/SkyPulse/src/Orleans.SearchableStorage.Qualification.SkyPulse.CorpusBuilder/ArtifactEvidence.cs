using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

internal sealed record ParentArtifactEvidence(
    long ByteLength,
    string Sha256,
    string CorpusFingerprint);

internal static class ArtifactEvidence
{
    private const int BufferBytes = 1024 * 1024;

    public static ParentArtifactEvidence InspectParent(string path, long accountCount, bool verifyOrder)
    {
        var expectedLength = checked(accountCount * CorpusFormat.AccountKeyByteLength);
        var file = new FileInfo(path);
        if (!file.Exists)
        {
            throw new InvalidDataException("The canonical account artifact is missing.");
        }

        if (file.Length != expectedLength)
        {
            throw new InvalidDataException(
                $"The canonical account artifact is {file.Length} bytes; expected exactly {expectedLength}.");
        }

        using var artifactHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var fingerprint = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        fingerprint.AppendData(Encoding.ASCII.GetBytes(CorpusFormat.FingerprintDomain));

        var buffer = ArrayPool<byte>.Shared.Rent(BufferBytes);
        var previous = new byte[CorpusFormat.AccountKeyByteLength];
        var hasPrevious = false;
        try
        {
            using var stream = new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    Options = FileOptions.SequentialScan,
                });
            long remaining = expectedLength;
            while (remaining > 0)
            {
                var requested = (int)Math.Min(buffer.Length, remaining);
                requested -= requested % CorpusFormat.AccountKeyByteLength;
                stream.ReadExactly(buffer.AsSpan(0, requested));
                var bytes = buffer.AsSpan(0, requested);
                artifactHash.AppendData(bytes);
                fingerprint.AppendData(bytes);

                if (verifyOrder)
                {
                    for (var offset = 0; offset < requested; offset += CorpusFormat.AccountKeyByteLength)
                    {
                        var current = bytes.Slice(offset, CorpusFormat.AccountKeyByteLength);
                        if (current.IndexOfAnyExcept((byte)0) < 0)
                        {
                            throw new InvalidDataException("The canonical corpus contains an all-zero account key.");
                        }

                        if (hasPrevious && previous.AsSpan().SequenceCompareTo(current) >= 0)
                        {
                            throw new InvalidDataException(
                                "Canonical corpus keys must be unique and in strict unsigned lexicographic order.");
                        }

                        current.CopyTo(previous);
                        hasPrevious = true;
                    }
                }

                remaining -= requested;
            }

            if (stream.ReadByte() != -1)
            {
                throw new InvalidDataException("The canonical account artifact grew during verification.");
            }
        }
        finally
        {
            Array.Clear(previous);
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }

        return new ParentArtifactEvidence(
            expectedLength,
            Convert.ToHexString(artifactHash.GetHashAndReset()).ToLowerInvariant(),
            Convert.ToHexString(fingerprint.GetHashAndReset()).ToLowerInvariant());
    }

    public static string HashPrefix(string path, long byteLength)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });
        var buffer = ArrayPool<byte>.Shared.Rent(BufferBytes);
        try
        {
            long remaining = byteLength;
            while (remaining > 0)
            {
                var requested = (int)Math.Min(buffer.Length, remaining);
                stream.ReadExactly(buffer.AsSpan(0, requested));
                hash.AppendData(buffer.AsSpan(0, requested));
                remaining -= requested;
            }

            return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    public static (long ByteLength, string Sha256) HashFile(string path)
    {
        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });
        var length = stream.Length;
        var hash = SHA256.HashData(stream);
        if (stream.Length != length)
        {
            throw new InvalidDataException("A hashed artifact changed length during verification.");
        }

        return (length, Convert.ToHexString(hash).ToLowerInvariant());
    }

    public static void VerifyHumanReadable(string binaryPath, string humanPath, long accountCount)
    {
        var expectedHumanLength = checked(accountCount * 65);
        var humanInfo = new FileInfo(humanPath);
        if (!humanInfo.Exists || humanInfo.Length != expectedHumanLength)
        {
            throw new InvalidDataException(
                "The human-readable artifact is not exactly 65 bytes per account key.");
        }

        using var binary = new FileStream(
            binaryPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            FileOptions.SequentialScan);
        using var human = new FileStream(
            humanPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            FileOptions.SequentialScan);
        Span<byte> key = stackalloc byte[CorpusFormat.AccountKeyByteLength];
        Span<byte> line = stackalloc byte[65];
        for (long index = 0; index < accountCount; index++)
        {
            binary.ReadExactly(key);
            human.ReadExactly(line);
            for (var offset = 0; offset < key.Length; offset++)
            {
                if (line[offset * 2] != LowerHexNibble(key[offset] >> 4)
                    || line[(offset * 2) + 1] != LowerHexNibble(key[offset] & 0x0f))
                {
                    throw new InvalidDataException(
                        "The human-readable artifact is not the canonical rendering of accounts.ak32.");
                }
            }

            if (line[^1] != (byte)'\n')
            {
                throw new InvalidDataException(
                    "The human-readable artifact must use one LF-terminated key per line.");
            }
        }
    }

    private static byte LowerHexNibble(int value)
        => (byte)(value < 10 ? (byte)'0' + value : (byte)'a' + value - 10);
}
