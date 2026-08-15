using System.Security.Cryptography;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

internal static partial class PrivateArtifactIO
{
    internal static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        NumberHandling = JsonNumberHandling.Strict,
        RespectNullableAnnotations = true,
        WriteIndented = false,
    };

    public static byte[] SerializeCanonical<T>(T value)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(value, CanonicalJson);
        var result = new byte[json.Length + 1];
        json.CopyTo(result, 0);
        result[^1] = (byte)'\n';
        return result;
    }

    public static T ReadCanonical<T>(string path)
    {
        PrivateWorkspacePermissions.ValidateRegularFile(path);
        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });
        PrivateWorkspacePermissions.ValidateRegularFile(stream);
        if (stream.Length > int.MaxValue)
        {
            throw new InvalidDataException("The canonical private JSON artifact is too large.");
        }

        var bytes = new byte[checked((int)stream.Length)];
        stream.ReadExactly(bytes);
        try
        {
            var value = JsonSerializer.Deserialize<T>(bytes, CanonicalJson)
                ?? throw new InvalidDataException("The canonical JSON artifact is empty.");
            var canonical = SerializeCanonical(value);
            try
            {
                if (!bytes.AsSpan().SequenceEqual(canonical))
                {
                    throw new InvalidDataException(
                        "The JSON artifact is not in its unique canonical representation.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(canonical);
            }

            return value;
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The canonical JSON artifact is malformed.", exception);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }

    public static void AtomicWriteCanonical<T>(string path, T value)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The artifact path must have a parent directory.", nameof(path));
        PrivateWorkspacePermissions.CreateDirectory(directory);
        var temporary = Path.Combine(directory, $".{Path.GetFileName(path)}.tmp-{Guid.NewGuid():N}");
        try
        {
            WriteNewPrivateFile(temporary, SerializeCanonical(value));
            File.Move(temporary, path, overwrite: true);
            EnsurePrivateMode(path);
            FlushDirectory(directory);
        }
        finally
        {
            if (File.Exists(temporary))
            {
                File.Delete(temporary);
            }
        }
    }

    public static FileStream OpenPrivateAppend(string path)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The artifact path must have a parent directory.", nameof(path));
        PrivateWorkspacePermissions.ValidateDirectory(directory);
        var file = new FileInfo(path);
        if (file.LinkTarget is not null
            || (file.Exists && (file.Attributes & FileAttributes.ReparsePoint) != 0))
        {
            throw new IOException("A private append artifact must not be a link.");
        }

        var existed = file.Exists;
        if (existed)
        {
            PrivateWorkspacePermissions.ValidateRegularFile(path);
        }

        var options = new FileStreamOptions
        {
            Mode = FileMode.OpenOrCreate,
            Access = FileAccess.ReadWrite,
            Share = FileShare.Read,
            BufferSize = 64 * 1024,
            Options = FileOptions.SequentialScan,
        };
        PrivateWorkspacePermissions.ApplyPrivateCreateMode(options);

        var stream = new FileStream(path, options);
        PrivateWorkspacePermissions.ValidateRegularFile(stream);

        stream.Seek(0, SeekOrigin.End);
        return stream;
    }

    public static void WriteNewPrivateFile(string path, ReadOnlySpan<byte> bytes)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The artifact path must have a parent directory.", nameof(path));
        PrivateWorkspacePermissions.ValidateDirectory(directory);
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = 64 * 1024,
            Options = FileOptions.SequentialScan,
        };
        PrivateWorkspacePermissions.ApplyPrivateCreateMode(options);

        using var stream = new FileStream(path, options);
        PrivateWorkspacePermissions.ValidateRegularFile(stream);
        stream.Write(bytes);
        stream.Flush(flushToDisk: true);
        EnsurePrivateMode(path);
    }

    public static void EnsurePrivateMode(string path)
        => PrivateWorkspacePermissions.ValidateRegularFile(path);

    public static void EnsurePrivateDirectory(string path)
        => PrivateWorkspacePermissions.CreateDirectory(path);

    public static PrivateArtifactEvidence Inspect(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return new PrivateArtifactEvidence(stream.Length, LowerHex(hash));
    }

    public static PrivateArtifactEvidence InspectPrivate(string path)
    {
        PrivateWorkspacePermissions.ValidateRegularFile(path);
        using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });
        PrivateWorkspacePermissions.ValidateRegularFile(stream);
        var hash = SHA256.HashData(stream);
        return new PrivateArtifactEvidence(stream.Length, LowerHex(hash));
    }

    public static string Sha256Text(string value)
        => LowerHex(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(value)));

    public static string LowerHex(ReadOnlySpan<byte> bytes)
        => Convert.ToHexString(bytes).ToLowerInvariant();

    public static void ValidateSha256(string value, string name)
    {
        if (value.Length != 64
            || value.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new InvalidDataException($"The {name} is not lowercase hexadecimal SHA-256.");
        }
    }

    private static void FlushDirectory(string directory)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        if (!OperatingSystem.IsLinux())
        {
            // The qualification deployment is Linux. Other Unix platforms still receive atomic
            // rename semantics, but their directory-open flags differ and are not claimed here.
            return;
        }

        const int openReadOnly = 0;
        const int openDirectory = 0x00010000;
        var descriptor = NativeMethods.Open(directory, openReadOnly | openDirectory);
        if (descriptor < 0)
        {
            throw new IOException("Opening the private artifact directory for fsync failed.", new Win32Exception());
        }

        try
        {
            if (NativeMethods.Fsync(descriptor) != 0)
            {
                throw new IOException("Fsync of the private artifact directory failed.", new Win32Exception());
            }
        }
        finally
        {
            _ = NativeMethods.Close(descriptor);
        }
    }

    private static partial class NativeMethods
    {
        [LibraryImport("libc", EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int Open(string path, int flags);

        [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
        internal static partial int Fsync(int descriptor);

        [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
        internal static partial int Close(int descriptor);
    }
}
