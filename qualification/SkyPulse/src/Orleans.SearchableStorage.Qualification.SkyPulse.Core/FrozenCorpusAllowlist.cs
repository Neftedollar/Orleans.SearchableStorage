using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Holds one immutable, canonically ordered account allowlist shared by all capped profiles.
/// </summary>
public sealed class FrozenCorpusAllowlist
{
    internal const string FingerprintDomain = "orleans-searchable-storage-skypulse-corpus-v1\0";
    private readonly AccountKey[] _orderedKeys;

    private FrozenCorpusAllowlist(AccountKey[] orderedKeys, string fingerprint)
    {
        _orderedKeys = orderedKeys;
        Fingerprint = fingerprint;
    }

    public int Count => _orderedKeys.Length;

    /// <summary>
    /// Gets the SHA-256 identity of the complete ordered allowlist.
    /// </summary>
    public string Fingerprint { get; }

    /// <summary>
    /// Loads an allowlist that is already in strict ascending <see cref="AccountKey"/> order.
    /// </summary>
    /// <remarks>
    /// Ordering and duplicates are rejected rather than repaired so that a malformed or altered
    /// frozen manifest cannot silently select a different qualification corpus.
    /// </remarks>
    public static FrozenCorpusAllowlist FromCanonicalOrder(IEnumerable<AccountKey> orderedKeys)
    {
        ArgumentNullException.ThrowIfNull(orderedKeys);

        var keys = orderedKeys.ToArray();
        if (keys.Length == 0)
        {
            throw new ArgumentException("A frozen allowlist cannot be empty.", nameof(orderedKeys));
        }

        for (var index = 0; index < keys.Length; index++)
        {
            if (!keys[index].IsValid)
            {
                throw new ArgumentException(
                    $"The account key at position {index} is invalid.",
                    nameof(orderedKeys));
            }

            if (index > 0 && keys[index - 1].CompareTo(keys[index]) >= 0)
            {
                throw new ArgumentException(
                    "Account keys must be unique and in strict ascending canonical order.",
                    nameof(orderedKeys));
            }
        }

        return new FrozenCorpusAllowlist(keys, ComputeFingerprint(keys));
    }

    /// <summary>
    /// Creates a bounded admission view over this exact frozen allowlist.
    /// </summary>
    public CappedCorpusAdmission CreateAdmission(CappedCorpusProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        if (profile.MaximumAccounts > Count)
        {
            throw new InvalidOperationException(
                $"Profile '{profile.Name}' requires {profile.MaximumAccounts} accounts, "
                + $"but the frozen allowlist contains only {Count}.");
        }

        return new CappedCorpusAdmission(this, profile);
    }

    internal AccountKey GetKey(int index) => _orderedKeys[index];

    private static string ComputeFingerprint(IEnumerable<AccountKey> keys)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.ASCII.GetBytes(FingerprintDomain));

        Span<byte> keyBytes = stackalloc byte[AccountKey.ByteLength];
        foreach (var key in keys)
        {
            key.WriteBytes(keyBytes);
            hash.AppendData(keyBytes);
        }

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }
}
