using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Exposes a verified prefix of a large canonical account-key file without materializing the
/// parent corpus as a managed object graph.
/// </summary>
/// <remarks>
/// The canonical file is exactly <c>N * 32</c> bytes: one non-zero SHA-256 account key after
/// another in strict unsigned lexicographic order, with no header or delimiter. Opening performs
/// a full sequential hash and ordering verification once. Membership then uses bounded random
/// reads over the frozen prefix. The artifact must remain read-only for the lifetime of this
/// instance.
/// </remarks>
public sealed class FileBackedCorpusAdmission : IDisposable
{
    private const int VerificationBufferBytes = 1024 * 1024;
    public const int MaximumReadPageSize = 10_000;
    private readonly FileStream _stream;
    private int _disposed;

    private FileBackedCorpusAdmission(
        FileStream stream,
        int parentAccountCount,
        string artifactSha256,
        string allowlistFingerprint,
        string profilePrefixSha256,
        CappedCorpusProfile profile)
    {
        _stream = stream;
        ParentAccountCount = parentAccountCount;
        ArtifactSha256 = artifactSha256;
        AllowlistFingerprint = allowlistFingerprint;
        ProfilePrefixSha256 = profilePrefixSha256;
        Profile = profile;
    }

    public CappedCorpusProfile Profile { get; }

    public int Count => Profile.MaximumAccounts;

    public int ParentAccountCount { get; }

    public string ArtifactSha256 { get; }

    public string AllowlistFingerprint { get; }

    /// <summary>
    /// Gets the raw SHA-256 of the exact admitted prefix (<c>Count * 32</c> bytes).
    /// </summary>
    public string ProfilePrefixSha256 { get; }

    public AccountKey this[int index]
    {
        get
        {
            if ((uint)index >= (uint)Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return ReadKey(index);
        }
    }

    /// <summary>
    /// Reads one bounded contiguous page from the verified prefix. This is intended for durable
    /// startup bootstrap and avoids one random file operation per account.
    /// </summary>
    public IReadOnlyList<AccountKey> ReadPage(int startIndex, int pageSize)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (startIndex < 0 || startIndex > Count)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex));
        }

        if (pageSize is < 1 or > MaximumReadPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"The corpus read page must be between 1 and {MaximumReadPageSize} accounts.");
        }

        var count = Math.Min(pageSize, Count - startIndex);
        if (count == 0)
        {
            return [];
        }

        var bytes = new byte[checked(count * AccountKey.ByteLength)];
        ReadExactly(
            _stream.SafeFileHandle,
            bytes,
            checked((long)startIndex * AccountKey.ByteLength));
        var keys = new AccountKey[count];
        for (var index = 0; index < count; index++)
        {
            keys[index] = AccountKey.FromBytes(
                bytes.AsSpan(index * AccountKey.ByteLength, AccountKey.ByteLength));
        }

        CryptographicOperations.ZeroMemory(bytes);
        return keys;
    }

    /// <summary>
    /// Opens a canonical binary parent corpus only after its count, raw SHA-256, domain-separated
    /// corpus fingerprint, key validity, uniqueness, and ordering all match the frozen manifest.
    /// </summary>
    public static FileBackedCorpusAdmission OpenVerified(
        string path,
        int expectedParentAccountCount,
        string expectedArtifactSha256,
        string expectedAllowlistFingerprint,
        CappedCorpusProfile profile,
        string? expectedProfilePrefixSha256 = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(profile);
        if (expectedParentAccountCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedParentAccountCount),
                expectedParentAccountCount,
                "The frozen parent corpus must contain at least one account.");
        }

        ValidateSha256(expectedArtifactSha256, nameof(expectedArtifactSha256));
        ValidateSha256(expectedAllowlistFingerprint, nameof(expectedAllowlistFingerprint));
        if (expectedProfilePrefixSha256 is not null)
        {
            ValidateSha256(expectedProfilePrefixSha256, nameof(expectedProfilePrefixSha256));
        }

        if (profile.MaximumAccounts > expectedParentAccountCount)
        {
            throw new InvalidOperationException(
                $"Profile '{profile.Name}' requires {profile.MaximumAccounts} accounts, but the "
                + $"frozen parent contains only {expectedParentAccountCount}.");
        }

        var expectedLength = checked((long)expectedParentAccountCount * AccountKey.ByteLength);
        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.RandomAccess,
                BufferSize = 1,
            });

        try
        {
            if (stream.Length != expectedLength)
            {
                throw new InvalidDataException(
                    $"The canonical corpus is {stream.Length} bytes; expected exactly {expectedLength}.");
            }

            var profileLength = checked((long)profile.MaximumAccounts * AccountKey.ByteLength);
            var (artifactSha256, allowlistFingerprint, profilePrefixSha256) = Verify(
                stream.SafeFileHandle,
                expectedLength,
                profileLength);
            if (!string.Equals(artifactSha256, expectedArtifactSha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The canonical corpus artifact SHA-256 does not match its manifest.");
            }

            if (!string.Equals(allowlistFingerprint, expectedAllowlistFingerprint, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The domain-separated corpus fingerprint does not match its manifest.");
            }

            if (expectedProfilePrefixSha256 is not null
                && !string.Equals(profilePrefixSha256, expectedProfilePrefixSha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The exact corpus profile prefix SHA-256 does not match its manifest.");
            }

            return new FileBackedCorpusAdmission(
                stream,
                expectedParentAccountCount,
                artifactSha256,
                allowlistFingerprint,
                profilePrefixSha256,
                profile);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public bool IsAdmitted(AccountKey accountKey)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        var lower = 0;
        var upper = Count - 1;
        while (lower <= upper)
        {
            var middle = lower + ((upper - lower) / 2);
            var comparison = ReadKey(middle).CompareTo(accountKey);
            if (comparison == 0)
            {
                return true;
            }

            if (comparison < 0)
            {
                lower = middle + 1;
            }
            else
            {
                upper = middle - 1;
            }
        }

        return false;
    }

    public AccountProjection CreateProjection(
        AccountKey accountKey,
        long lastActivityMinuteUtc,
        RollingWindowCounts createdRecordCounts,
        RollingWindowCounts updatedRecordCounts,
        RollingWindowCounts deletedRecordCounts,
        long currentPostCount,
        long currentFollowingCount,
        long currentFollowerCount,
        RollingWindowCounts postCreateCounts,
        long receivedEngagementCreates30Days)
    {
        if (!IsAdmitted(accountKey))
        {
            throw new InvalidOperationException(
                $"Account '{accountKey}' is outside profile '{Profile.Name}'.");
        }

        return new AccountProjection(
            accountKey,
            lastActivityMinuteUtc,
            createdRecordCounts,
            updatedRecordCounts,
            deletedRecordCounts,
            currentPostCount,
            currentFollowingCount,
            currentFollowerCount,
            postCreateCounts,
            receivedEngagementCreates30Days);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _stream.Dispose();
        }
    }

    private static (string ArtifactSha256, string AllowlistFingerprint, string ProfilePrefixSha256) Verify(
        SafeFileHandle handle,
        long expectedLength,
        long profileLength)
    {
        using var artifactHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var corpusHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var profileHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        corpusHash.AppendData(Encoding.ASCII.GetBytes(FrozenCorpusAllowlist.FingerprintDomain));

        var buffer = ArrayPool<byte>.Shared.Rent(VerificationBufferBytes);
        try
        {
            AccountKey? previous = null;
            long offset = 0;
            while (offset < expectedLength)
            {
                var requested = (int)Math.Min(buffer.Length, expectedLength - offset);
                requested -= requested % AccountKey.ByteLength;
                ReadExactly(handle, buffer.AsSpan(0, requested), offset);
                var bytes = buffer.AsSpan(0, requested);
                artifactHash.AppendData(bytes);
                corpusHash.AppendData(bytes);
                if (offset < profileLength)
                {
                    var profileBytes = checked((int)Math.Min(requested, profileLength - offset));
                    profileHash.AppendData(bytes[..profileBytes]);
                }

                for (var position = 0; position < bytes.Length; position += AccountKey.ByteLength)
                {
                    var current = AccountKey.FromBytes(bytes.Slice(position, AccountKey.ByteLength));
                    if (previous is { } prior && prior.CompareTo(current) >= 0)
                    {
                        throw new InvalidDataException(
                            "Canonical corpus keys must be unique and in strict ascending order.");
                    }

                    previous = current;
                }

                offset += requested;
            }

            return (
                Convert.ToHexString(artifactHash.GetHashAndReset()).ToLowerInvariant(),
                Convert.ToHexString(corpusHash.GetHashAndReset()).ToLowerInvariant(),
                Convert.ToHexString(profileHash.GetHashAndReset()).ToLowerInvariant());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private AccountKey ReadKey(int index)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        Span<byte> bytes = stackalloc byte[AccountKey.ByteLength];
        ReadExactly(_stream.SafeFileHandle, bytes, checked((long)index * AccountKey.ByteLength));
        return AccountKey.FromBytes(bytes);
    }

    private static void ReadExactly(SafeFileHandle handle, Span<byte> destination, long offset)
    {
        var read = 0;
        while (read < destination.Length)
        {
            var count = RandomAccess.Read(handle, destination[read..], offset + read);
            if (count == 0)
            {
                throw new EndOfStreamException("The canonical corpus ended during a verified read.");
            }

            read += count;
        }
    }

    private static void ValidateSha256(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length != AccountKey.TextLength
            || value.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new ArgumentException(
                "A SHA-256 value must contain 64 lowercase hexadecimal characters.",
                parameterName);
        }
    }
}
