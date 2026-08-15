using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Thrown when a database is already bound to a different immutable runtime identity.
/// </summary>
public sealed class RuntimeManifestMismatchException : InvalidOperationException
{
    public RuntimeManifestMismatchException(string expectedFingerprint, string actualFingerprint)
        : base($"The PostgreSQL runtime manifest is {actualFingerprint}, but this process requires {expectedFingerprint}.")
    {
        ExpectedFingerprint = expectedFingerprint;
        ActualFingerprint = actualFingerprint;
    }

    public string ExpectedFingerprint { get; }

    public string ActualFingerprint { get; }
}

/// <summary>
/// Binds and verifies the exact package, base profile, source, namespace, and schema runtime identity.
/// </summary>
public sealed class PostgreSqlRuntimeManifestStore
{
    internal const string InsertSql = """
        INSERT INTO skypulse.runtime_manifest (
            manifest_id, profile_id, profile_version, corpus_cap, allowlist_sha256,
            source_instance_id, index_namespace, index_provider_name, index_schema_id,
            index_schema_version, index_schema_fingerprint, package_id, package_version,
            package_nupkg_sha256, package_canonical_manifest_sha256,
            package_repository_url, package_repository_commit, package_build_sdk_version,
            manifest_fingerprint)
        VALUES (
            1, @profile_id, @profile_version, @corpus_cap, @allowlist_sha256,
            @source_instance_id, @index_namespace, @index_provider_name, @index_schema_id,
            @index_schema_version, @index_schema_fingerprint, @package_id, @package_version,
            @package_nupkg_sha256, @package_canonical_manifest_sha256,
            @package_repository_url, @package_repository_commit, @package_build_sdk_version,
            @manifest_fingerprint)
        ON CONFLICT (manifest_id) DO NOTHING;
        """;

    internal const string ReadSql = """
        SELECT profile_id, profile_version, corpus_cap, allowlist_sha256,
            source_instance_id, index_namespace, index_provider_name, index_schema_id,
            index_schema_version, index_schema_fingerprint, package_id, package_version,
            package_nupkg_sha256, package_canonical_manifest_sha256,
            package_repository_url, package_repository_commit, package_build_sdk_version,
            manifest_fingerprint
        FROM skypulse.runtime_manifest
        WHERE manifest_id = 1;
        """;

    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlRuntimeManifestStore(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    public async Task BindAsync(RuntimeManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = InsertSql;
            AddParameters(insert, manifest);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var actual = await ReadAsync(connection, transaction, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("The runtime manifest could not be read after binding.");
        if (!string.Equals(manifest.Fingerprint, actual.Fingerprint, StringComparison.Ordinal))
        {
            throw new RuntimeManifestMismatchException(manifest.Fingerprint, actual.Fingerprint);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<RuntimeManifest?> ReadAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await ReadAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<RuntimeManifest?> ReadAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadSql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var profile = new RuntimeProfileIdentity(
            reader.GetString(0),
            reader.GetInt32(1),
            reader.GetInt64(2),
            ToHex(reader.GetFieldValue<byte[]>(3)));
        var index = new RuntimeIndexIdentity(
            reader.GetString(5),
            reader.GetString(6),
            reader.GetString(7),
            reader.GetInt32(8),
            ToHex(reader.GetFieldValue<byte[]>(9)));
        var package = new RuntimePackageIdentity(
            reader.GetString(10),
            reader.GetString(11),
            ToHex(reader.GetFieldValue<byte[]>(12)),
            ToHex(reader.GetFieldValue<byte[]>(13)),
            reader.GetString(14),
            reader.GetString(15),
            reader.GetString(16));
        var manifest = new RuntimeManifest(profile, reader.GetGuid(4), index, package);
        var storedFingerprint = ToHex(reader.GetFieldValue<byte[]>(17));
        if (!string.Equals(manifest.Fingerprint, storedFingerprint, StringComparison.Ordinal))
        {
            throw new RuntimeManifestMismatchException(manifest.Fingerprint, storedFingerprint);
        }

        return manifest;
    }

    private static void AddParameters(NpgsqlCommand command, RuntimeManifest manifest)
    {
        command.Parameters.AddWithValue("profile_id", NpgsqlDbType.Text, manifest.Profile.ProfileId);
        command.Parameters.AddWithValue("profile_version", NpgsqlDbType.Integer, manifest.Profile.ProfileVersion);
        command.Parameters.AddWithValue("corpus_cap", NpgsqlDbType.Bigint, manifest.Profile.CorpusCap);
        command.Parameters.AddWithValue("allowlist_sha256", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(manifest.Profile.AllowlistSha256));
        command.Parameters.AddWithValue("source_instance_id", NpgsqlDbType.Uuid, manifest.SourceInstanceId);
        command.Parameters.AddWithValue("index_namespace", NpgsqlDbType.Text, manifest.Index.IndexNamespace);
        command.Parameters.AddWithValue("index_provider_name", NpgsqlDbType.Text, manifest.Index.ProviderName);
        command.Parameters.AddWithValue("index_schema_id", NpgsqlDbType.Text, manifest.Index.SchemaId);
        command.Parameters.AddWithValue("index_schema_version", NpgsqlDbType.Integer, manifest.Index.SchemaVersion);
        command.Parameters.AddWithValue("index_schema_fingerprint", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(manifest.Index.SchemaFingerprint));
        command.Parameters.AddWithValue("package_id", NpgsqlDbType.Text, manifest.Package.PackageId);
        command.Parameters.AddWithValue("package_version", NpgsqlDbType.Text, manifest.Package.PackageVersion);
        command.Parameters.AddWithValue("package_nupkg_sha256", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(manifest.Package.NupkgSha256));
        command.Parameters.AddWithValue("package_canonical_manifest_sha256", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(manifest.Package.CanonicalManifestSha256));
        command.Parameters.AddWithValue("package_repository_url", NpgsqlDbType.Text, manifest.Package.RepositoryUrl);
        command.Parameters.AddWithValue("package_repository_commit", NpgsqlDbType.Text, manifest.Package.RepositoryCommit);
        command.Parameters.AddWithValue("package_build_sdk_version", NpgsqlDbType.Text, manifest.Package.BuildSdkVersion);
        command.Parameters.AddWithValue("manifest_fingerprint", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(manifest.Fingerprint));
    }

    private static string ToHex(byte[] value) => Convert.ToHexString(value).ToLowerInvariant();
}
