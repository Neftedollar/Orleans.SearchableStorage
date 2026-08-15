using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

internal static class CorpusManifestCodec
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        NumberHandling = JsonNumberHandling.Strict,
        RespectNullableAnnotations = true,
        WriteIndented = false,
    };

    public static byte[] SerializeCanonical(CorpusManifest manifest)
    {
        Validate(manifest);
        var json = JsonSerializer.SerializeToUtf8Bytes(manifest, Options);
        var result = new byte[json.Length + 1];
        json.CopyTo(result, 0);
        result[^1] = (byte)'\n';
        return result;
    }

    public static CorpusManifest ReadCanonical(string path)
    {
        var bytes = File.ReadAllBytes(path);
        CorpusManifest manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<CorpusManifest>(bytes, Options)
                ?? throw new InvalidDataException("The corpus manifest is empty.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The corpus manifest is not valid canonical JSON.", exception);
        }

        Validate(manifest);
        if (!bytes.AsSpan().SequenceEqual(SerializeCanonical(manifest)))
        {
            throw new InvalidDataException(
                "The corpus manifest is not in its unique canonical JSON representation.");
        }

        return manifest;
    }

    public static void WriteCanonical(string path, CorpusManifest manifest)
    {
        var bytes = SerializeCanonical(manifest);
        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                Options = FileOptions.SequentialScan,
            });
        stream.Write(bytes);
        stream.Flush(flushToDisk: true);
    }

    private static void Validate(CorpusManifest manifest)
    {
        if (!string.Equals(manifest.Format, CorpusFormat.ManifestFormat, StringComparison.Ordinal))
        {
            throw new InvalidDataException("The corpus manifest format is unknown.");
        }

        if (!string.Equals(
                manifest.AccountKey.Algorithm,
                CorpusFormat.AccountKeyAlgorithm,
                StringComparison.Ordinal)
            || manifest.AccountKey.ByteLength != CorpusFormat.AccountKeyByteLength)
        {
            throw new InvalidDataException("The corpus account-key contract is unknown.");
        }

        if (!string.Equals(
                manifest.SourceJournal.Format,
                CorpusFormat.JournalFormat,
                StringComparison.Ordinal)
            || manifest.SourceJournal.ByteLength <= 0)
        {
            throw new InvalidDataException("The source-journal identity is invalid.");
        }

        ValidateSha256(manifest.SourceJournal.Sha256, "source journal SHA-256");

        if (!string.Equals(
                manifest.Parent.Artifact,
                CorpusFormat.BinaryArtifactName,
                StringComparison.Ordinal)
            || manifest.Parent.AccountCount <= 0
            || manifest.Parent.ByteLength
                != checked(manifest.Parent.AccountCount * CorpusFormat.AccountKeyByteLength))
        {
            throw new InvalidDataException("The parent corpus identity or byte length is invalid.");
        }

        ValidateSha256(manifest.Parent.Sha256, "parent artifact SHA-256");
        ValidateSha256(manifest.Parent.CorpusFingerprint, "parent corpus fingerprint");

        if (manifest.Profiles.Count == 0)
        {
            throw new InvalidDataException("At least one exact-prefix profile is required.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        CorpusProfileManifest? previous = null;
        foreach (var profile in manifest.Profiles)
        {
            _ = new CorpusProfileRequest(profile.Name, profile.AccountCount);
            if (!names.Add(profile.Name))
            {
                throw new InvalidDataException($"Duplicate corpus profile name '{profile.Name}'.");
            }

            if (profile.AccountCount > manifest.Parent.AccountCount
                || profile.ByteLength
                    != checked(profile.AccountCount * CorpusFormat.AccountKeyByteLength))
            {
                throw new InvalidDataException($"Corpus profile '{profile.Name}' is not a valid parent prefix.");
            }

            ValidateSha256(profile.PrefixSha256, $"profile '{profile.Name}' prefix SHA-256");
            if (previous is not null
                && (previous.AccountCount > profile.AccountCount
                    || (previous.AccountCount == profile.AccountCount
                        && string.CompareOrdinal(previous.Name, profile.Name) >= 0)))
            {
                throw new InvalidDataException(
                    "Corpus profiles must be ordered by account count and then by name.");
            }

            previous = profile;
        }

        if (manifest.HumanReadableArtifact is { } human)
        {
            if (!string.Equals(human.Artifact, CorpusFormat.HumanArtifactName, StringComparison.Ordinal)
                || human.ByteLength != checked(manifest.Parent.AccountCount * 65))
            {
                throw new InvalidDataException("The human-readable artifact identity is invalid.");
            }

            ValidateSha256(human.Sha256, "human-readable artifact SHA-256");
        }
    }

    private static void ValidateSha256(string value, string name)
    {
        if (value.Length != 64
            || value.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new InvalidDataException($"The {name} is not canonical lowercase hexadecimal SHA-256.");
        }
    }
}
