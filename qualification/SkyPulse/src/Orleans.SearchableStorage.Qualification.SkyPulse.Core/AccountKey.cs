using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Identifies an AT Protocol account without retaining the source DID.
/// </summary>
/// <remarks>
/// The key is the SHA-256 digest of the exact UTF-8 representation of a syntactically valid DID.
/// DID normalization is deliberately not performed: the source adapter must supply the canonical
/// repository DID. This is a stable identifier, not a privacy boundary for public DIDs.
/// </remarks>
public readonly struct AccountKey : IComparable<AccountKey>, IEquatable<AccountKey>
{
    public const int ByteLength = 32;
    public const int TextLength = ByteLength * 2;

    private readonly ulong _part0;
    private readonly ulong _part1;
    private readonly ulong _part2;
    private readonly ulong _part3;

    private AccountKey(ReadOnlySpan<byte> digest)
    {
        _part0 = BinaryPrimitives.ReadUInt64BigEndian(digest);
        _part1 = BinaryPrimitives.ReadUInt64BigEndian(digest[8..]);
        _part2 = BinaryPrimitives.ReadUInt64BigEndian(digest[16..]);
        _part3 = BinaryPrimitives.ReadUInt64BigEndian(digest[24..]);
    }

    /// <summary>
    /// Gets whether this value contains a usable account identifier.
    /// </summary>
    public bool IsValid => (_part0 | _part1 | _part2 | _part3) != 0;

    /// <summary>
    /// Produces a stable account key from the exact canonical repository DID.
    /// </summary>
    public static AccountKey FromDid(string did)
    {
        ValidateDid(did);

        var utf8Did = Encoding.UTF8.GetBytes(did);
        Span<byte> digest = stackalloc byte[ByteLength];
        SHA256.HashData(utf8Did, digest);
        return new AccountKey(digest);
    }

    internal static AccountKey FromBytes(ReadOnlySpan<byte> digest)
    {
        if (digest.Length != ByteLength)
        {
            throw new ArgumentException(
                $"An account-key digest must contain exactly {ByteLength} bytes.",
                nameof(digest));
        }

        var result = new AccountKey(digest);
        if (!result.IsValid)
        {
            throw new ArgumentException("An account-key digest cannot be all zeroes.", nameof(digest));
        }

        return result;
    }

    /// <summary>
    /// Parses a canonical lowercase hexadecimal account key.
    /// </summary>
    public static AccountKey Parse(string value)
    {
        if (!TryParse(value, out var key))
        {
            throw new FormatException(
                $"An account key must be {TextLength} lowercase hexadecimal characters and must not be all zeroes.");
        }

        return key;
    }

    /// <summary>
    /// Attempts to parse a canonical lowercase hexadecimal account key.
    /// </summary>
    public static bool TryParse(string? value, out AccountKey key)
    {
        key = default;
        if (value is null || value.Length != TextLength)
        {
            return false;
        }

        Span<byte> digest = stackalloc byte[ByteLength];
        for (var index = 0; index < digest.Length; index++)
        {
            var high = DecodeLowerHex(value[index * 2]);
            var low = DecodeLowerHex(value[(index * 2) + 1]);
            if (high < 0 || low < 0)
            {
                return false;
            }

            digest[index] = (byte)((high << 4) | low);
        }

        var candidate = new AccountKey(digest);
        if (!candidate.IsValid)
        {
            return false;
        }

        key = candidate;
        return true;
    }

    public int CompareTo(AccountKey other)
    {
        var comparison = _part0.CompareTo(other._part0);
        if (comparison != 0)
        {
            return comparison;
        }

        comparison = _part1.CompareTo(other._part1);
        if (comparison != 0)
        {
            return comparison;
        }

        comparison = _part2.CompareTo(other._part2);
        if (comparison != 0)
        {
            return comparison;
        }

        return _part3.CompareTo(other._part3);
    }

    public bool Equals(AccountKey other)
    {
        return _part0 == other._part0
            && _part1 == other._part1
            && _part2 == other._part2
            && _part3 == other._part3;
    }

    public override bool Equals(object? obj) => obj is AccountKey other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_part0, _part1, _part2, _part3);

    public override string ToString()
    {
        Span<byte> digest = stackalloc byte[ByteLength];
        WriteBytes(digest);
        return Convert.ToHexString(digest).ToLowerInvariant();
    }

    public static bool operator ==(AccountKey left, AccountKey right) => left.Equals(right);

    public static bool operator !=(AccountKey left, AccountKey right) => !left.Equals(right);

    public static bool operator <(AccountKey left, AccountKey right) => left.CompareTo(right) < 0;

    public static bool operator >(AccountKey left, AccountKey right) => left.CompareTo(right) > 0;

    public static bool operator <=(AccountKey left, AccountKey right) => left.CompareTo(right) <= 0;

    public static bool operator >=(AccountKey left, AccountKey right) => left.CompareTo(right) >= 0;

    internal void WriteBytes(Span<byte> destination)
    {
        if (destination.Length < ByteLength)
        {
            throw new ArgumentException(
                $"The destination must contain at least {ByteLength} bytes.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt64BigEndian(destination, _part0);
        BinaryPrimitives.WriteUInt64BigEndian(destination[8..], _part1);
        BinaryPrimitives.WriteUInt64BigEndian(destination[16..], _part2);
        BinaryPrimitives.WriteUInt64BigEndian(destination[24..], _part3);
    }

    private static void ValidateDid(string did)
    {
        ArgumentNullException.ThrowIfNull(did);

        if (!did.StartsWith("did:", StringComparison.Ordinal))
        {
            throw new ArgumentException("The account identifier must be a DID.", nameof(did));
        }

        var methodEnd = did.IndexOf(':', 4);
        if (methodEnd <= 4 || methodEnd == did.Length - 1)
        {
            throw new ArgumentException("The account DID must contain a method and method-specific identifier.", nameof(did));
        }

        for (var index = 4; index < methodEnd; index++)
        {
            var character = did[index];
            if (!IsLowerLetter(character) && !IsDigit(character))
            {
                throw new ArgumentException("The DID method must contain only lowercase letters and digits.", nameof(did));
            }
        }

        for (var index = methodEnd + 1; index < did.Length; index++)
        {
            var character = did[index];
            if (IsAsciiLetter(character)
                || IsDigit(character)
                || character is '.' or '-' or '_' or ':')
            {
                continue;
            }

            if (character == '%'
                && index + 2 < did.Length
                && IsHex(did[index + 1])
                && IsHex(did[index + 2]))
            {
                index += 2;
                continue;
            }

            throw new ArgumentException("The account DID contains an invalid method-specific identifier.", nameof(did));
        }
    }

    private static int DecodeLowerHex(char value)
    {
        if (value is >= '0' and <= '9')
        {
            return value - '0';
        }

        if (value is >= 'a' and <= 'f')
        {
            return value - 'a' + 10;
        }

        return -1;
    }

    private static bool IsAsciiLetter(char value)
        => IsLowerLetter(value) || value is >= 'A' and <= 'Z';

    private static bool IsLowerLetter(char value) => value is >= 'a' and <= 'z';

    private static bool IsDigit(char value) => value is >= '0' and <= '9';

    private static bool IsHex(char value)
        => IsDigit(value)
            || value is >= 'a' and <= 'f'
            || value is >= 'A' and <= 'F';
}
