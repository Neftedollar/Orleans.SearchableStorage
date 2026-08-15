namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Exposes one process-wide admission view which can move only to a larger verified prefix of the
/// same immutable parent corpus. Older file handles remain alive until shutdown so concurrent
/// membership checks cannot race disposal during a swap.
/// </summary>
public sealed class MonotonicCorpusAdmission : IAccountAdmission, IDisposable
{
    private readonly object _gate = new();
    private readonly List<VerifiedCorpusAdmission> _ownedAdmissions = [];
    private VerifiedCorpusAdmission? _current;
    private int _disposed;

    public bool IsInitialized => Volatile.Read(ref _current) is not null;

    public int Count => Current.Count;

    public string ProfileId => Current.Profile.Name;

    public string ProfilePrefixSha256 => Current.ProfilePrefixSha256;

    public bool IsAdmitted(AccountKey accountKey) => Current.IsAdmitted(accountKey);

    public void Initialize(VerifiedCorpusAdmission admission)
    {
        ArgumentNullException.ThrowIfNull(admission);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        lock (_gate)
        {
            if (_current is not null)
            {
                throw new InvalidOperationException("The monotonic corpus admission is already initialized.");
            }

            _ownedAdmissions.Add(admission);
            Volatile.Write(ref _current, admission);
        }
    }

    public void Advance(VerifiedCorpusAdmission admission)
    {
        ArgumentNullException.ThrowIfNull(admission);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        lock (_gate)
        {
            var current = _current
                ?? throw new InvalidOperationException("The monotonic corpus admission is not initialized.");
            if (admission.Count <= current.Count)
            {
                throw new InvalidOperationException("A runtime corpus admission can move only to a larger prefix.");
            }

            if (admission.ParentAccountCount != current.ParentAccountCount
                || !string.Equals(
                    admission.ParentArtifactSha256,
                    current.ParentArtifactSha256,
                    StringComparison.Ordinal)
                || !string.Equals(
                    admission.ParentCorpusFingerprint,
                    current.ParentCorpusFingerprint,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "A runtime corpus admission cannot move to a different parent corpus.");
            }

            _ownedAdmissions.Add(admission);
            Volatile.Write(ref _current, admission);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        lock (_gate)
        {
            foreach (var admission in _ownedAdmissions)
            {
                admission.Dispose();
            }

            _ownedAdmissions.Clear();
            Volatile.Write(ref _current, null);
        }
    }

    private VerifiedCorpusAdmission Current
        => Volatile.Read(ref _current)
            ?? throw new InvalidOperationException("The monotonic corpus admission is not initialized.");
}
