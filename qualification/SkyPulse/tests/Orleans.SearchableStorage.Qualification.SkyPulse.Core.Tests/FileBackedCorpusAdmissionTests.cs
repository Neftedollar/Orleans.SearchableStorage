using System.Security.Cryptography;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tests;

public sealed class FileBackedCorpusAdmissionTests
{
    [Fact]
    public void VerifiedFileUsesExactPrefixWithoutMaterializingParent()
    {
        var keys = OrderedKeys(6);
        using var fixture = CreateCorpus(keys, new CappedCorpusProfile("file-3", 3));
        var corpus = fixture.Admission;

        Assert.Equal(3, corpus.Count);
        Assert.Equal(6, corpus.ParentAccountCount);
        Assert.Equal(keys.Take(3), Enumerable.Range(0, corpus.Count).Select(index => corpus[index]));
        Assert.All(keys.Take(3), key => Assert.True(corpus.IsAdmitted(key)));
        Assert.All(keys.Skip(3), key => Assert.False(corpus.IsAdmitted(key)));
    }

    [Fact]
    public void ProjectionCreationStillRequiresFrozenPrefixMembership()
    {
        var keys = OrderedKeys(2);
        using var fixture = CreateCorpus(keys, new CappedCorpusProfile("file-1", 1));
        var corpus = fixture.Admission;

        var projection = corpus.CreateProjection(
            keys[0],
            1,
            new RollingWindowCounts(0, 0, 0),
            new RollingWindowCounts(0, 0, 0),
            new RollingWindowCounts(0, 0, 0),
            0,
            0,
            0,
            new RollingWindowCounts(0, 0, 0),
            0);

        Assert.Equal(keys[0], projection.AccountKey);
        Assert.Throws<InvalidOperationException>(
            () => corpus.CreateProjection(
                keys[1],
                0,
                new RollingWindowCounts(0, 0, 0),
                new RollingWindowCounts(0, 0, 0),
                new RollingWindowCounts(0, 0, 0),
                0,
                0,
                0,
                new RollingWindowCounts(0, 0, 0),
                0));
    }

    [Fact]
    public void WrongHashTruncationAndUnsortedKeysFailClosed()
    {
        var keys = OrderedKeys(3);
        var path = WriteKeys(keys);
        try
        {
            var rawHash = RawHash(path);
            var fingerprint = FrozenCorpusAllowlist.FromCanonicalOrder(keys).Fingerprint;
            Assert.Throws<InvalidDataException>(
                () => FileBackedCorpusAdmission.OpenVerified(
                    path,
                    keys.Length,
                    new string('0', 64),
                    fingerprint,
                    new CappedCorpusProfile("hash", 1)));
            Assert.Throws<InvalidDataException>(
                () => FileBackedCorpusAdmission.OpenVerified(
                    path,
                    keys.Length + 1,
                    rawHash,
                    fingerprint,
                    new CappedCorpusProfile("length", 1)));

            WriteKeys(path, [keys[1], keys[0], keys[2]]);
            Assert.Throws<InvalidDataException>(
                () => FileBackedCorpusAdmission.OpenVerified(
                    path,
                    keys.Length,
                    RawHash(path),
                    fingerprint,
                    new CappedCorpusProfile("order", 1)));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void DisposedFileCannotBeRead()
    {
        var keys = OrderedKeys(2);
        using var fixture = CreateCorpus(keys, new CappedCorpusProfile("disposed", 1));
        var corpus = fixture.Admission;
        corpus.Dispose();

        Assert.Throws<ObjectDisposedException>(() => corpus.IsAdmitted(keys[0]));
        Assert.Throws<ObjectDisposedException>(() => _ = corpus[0]);
    }

    private static TestCorpus CreateCorpus(
        AccountKey[] keys,
        CappedCorpusProfile profile)
    {
        var path = WriteKeys(keys);
        try
        {
            var admission = FileBackedCorpusAdmission.OpenVerified(
                    path,
                    keys.Length,
                    RawHash(path),
                    FrozenCorpusAllowlist.FromCanonicalOrder(keys).Fingerprint,
                    profile);
            return new TestCorpus(path, admission);
        }
        catch
        {
            File.Delete(path);
            throw;
        }
    }

    private static AccountKey[] OrderedKeys(int count)
        => Enumerable.Range(0, count)
            .Select(index => AccountKey.FromDid($"did:plc:file-account-{index}"))
            .Order()
            .ToArray();

    private static string WriteKeys(IEnumerable<AccountKey> keys)
    {
        var path = Path.GetTempFileName();
        WriteKeys(path, keys);
        return path;
    }

    private static void WriteKeys(string path, IEnumerable<AccountKey> keys)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        Span<byte> bytes = stackalloc byte[AccountKey.ByteLength];
        foreach (var key in keys)
        {
            Convert.FromHexString(key.ToString(), bytes, out _, out _);
            stream.Write(bytes);
        }
    }

    private static string RawHash(string path)
    {
        using var stream = File.OpenRead(path);
        return Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
    }

    private sealed class TestCorpus(string path, FileBackedCorpusAdmission admission) : IDisposable
    {
        public FileBackedCorpusAdmission Admission { get; } = admission;

        public void Dispose()
        {
            Admission.Dispose();
            File.Delete(path);
        }
    }
}
