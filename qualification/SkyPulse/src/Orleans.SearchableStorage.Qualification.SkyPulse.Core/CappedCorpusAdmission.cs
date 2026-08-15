using System.Collections;

namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Exposes exactly one bounded prefix of a frozen account allowlist.
/// </summary>
/// <remarks>
/// Admission is membership-based rather than arrival-based. New accounts outside the frozen
/// prefix are never created, while existing admitted accounts continue to receive updates.
/// </remarks>
public sealed class CappedCorpusAdmission : IReadOnlyList<AccountKey>
{
    private readonly FrozenCorpusAllowlist _allowlist;

    internal CappedCorpusAdmission(FrozenCorpusAllowlist allowlist, CappedCorpusProfile profile)
    {
        _allowlist = allowlist;
        Profile = profile;
    }

    public CappedCorpusProfile Profile { get; }

    public int Count => Profile.MaximumAccounts;

    public string AllowlistFingerprint => _allowlist.Fingerprint;

    public AccountKey this[int index]
    {
        get
        {
            if ((uint)index >= (uint)Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return _allowlist.GetKey(index);
        }
    }

    /// <summary>
    /// Returns whether an account belongs to this exact frozen prefix.
    /// </summary>
    public bool IsAdmitted(AccountKey accountKey)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        var lower = 0;
        var upper = Count - 1;
        while (lower <= upper)
        {
            var middle = lower + ((upper - lower) / 2);
            var comparison = _allowlist.GetKey(middle).CompareTo(accountKey);
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

    /// <summary>
    /// Creates a validated projection only when its account belongs to this frozen prefix.
    /// </summary>
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
        EnsureAdmitted(accountKey);

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

    /// <summary>
    /// Fails closed when a caller attempts to create a projection outside this profile.
    /// </summary>
    public void EnsureAdmitted(AccountKey accountKey)
    {
        if (!IsAdmitted(accountKey))
        {
            throw new InvalidOperationException(
                $"Account '{accountKey}' is outside profile '{Profile.Name}'.");
        }
    }

    /// <summary>
    /// Returns whether every account admitted here is also admitted by <paramref name="other"/>.
    /// </summary>
    public bool IsPrefixOf(CappedCorpusAdmission other)
    {
        ArgumentNullException.ThrowIfNull(other);

        return Count <= other.Count
            && string.Equals(
                AllowlistFingerprint,
                other.AllowlistFingerprint,
                StringComparison.Ordinal);
    }

    public IEnumerator<AccountKey> GetEnumerator()
    {
        for (var index = 0; index < Count; index++)
        {
            yield return _allowlist.GetKey(index);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
