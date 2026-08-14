using System.Globalization;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Defines the physical layout and arithmetic used by journaled partition persistence.
/// </summary>
internal static class StoragePersistence
{
    public const int LegacyPersistenceFormatVersion = 3;
    public const int MovementPersistenceFormatVersion = 4;
    public const int CurrentPersistenceFormatVersion = 5;
    public const int IndexOnlyPersistenceFormatVersion = 6;

    // Kept as an explicit alias because the version-3 format is still the format used by a
    // newly-created partition until an operator enables a newer capability.
    public const int PreviousPersistenceFormatVersion = LegacyPersistenceFormatVersion;
    public const int DefaultJournalSegmentCapacity = 64;
    public const int DefaultMaximumJournalReplayEntries = 4_096;
    public const int DefaultCompactionThreshold = 1_024;
    public const int SnapshotSlotCount = 2;

    public static bool IsSupportedFormat(int formatVersion)
    {
        return formatVersion is LegacyPersistenceFormatVersion
            or MovementPersistenceFormatVersion
            or CurrentPersistenceFormatVersion
            or IndexOnlyPersistenceFormatVersion;
    }

    public static bool SupportsMovement(int formatVersion)
    {
        return formatVersion is MovementPersistenceFormatVersion
            or CurrentPersistenceFormatVersion
            or IndexOnlyPersistenceFormatVersion;
    }

    public static bool SupportsIndexSchemas(int formatVersion)
    {
        return formatVersion is CurrentPersistenceFormatVersion
            or IndexOnlyPersistenceFormatVersion;
    }

    public static bool UsesLosslessSnapshots(int formatVersion)
    {
        return SupportsMovement(formatVersion);
    }

    public static int GetJournalSlotCount(int maxReplayEntries, int segmentCapacity)
    {
        ValidateOptions(segmentCapacity, maxReplayEntries);

        var replaySegmentCount = checked(
            (maxReplayEntries + (long)segmentCapacity - 1) / segmentCapacity);
        return checked((int)replaySegmentCount + 2);
    }

    public static long GetAbsoluteSegmentIndex(long sequence, int segmentCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sequence);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);
        return (sequence - 1) / segmentCapacity;
    }

    public static long GetSegmentStartSequence(long absoluteSegmentIndex, int segmentCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(absoluteSegmentIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);
        return checked((absoluteSegmentIndex * segmentCapacity) + 1);
    }

    public static long GetSegmentEndSequence(long absoluteSegmentIndex, int segmentCapacity)
    {
        return checked(GetSegmentStartSequence(absoluteSegmentIndex, segmentCapacity) + segmentCapacity - 1);
    }

    public static int GetJournalSlotIndex(
        long absoluteSegmentIndex,
        int maxReplayEntries,
        int segmentCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(absoluteSegmentIndex);
        var slotCount = GetJournalSlotCount(maxReplayEntries, segmentCapacity);
        return (int)(absoluteSegmentIndex % slotCount);
    }

    public static long GetPrunableSequence(long snapshotSequence, int segmentCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(snapshotSequence);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);
        return (snapshotSequence / segmentCapacity) * segmentCapacity;
    }

    public static string CreateJournalSlotKey(string partitionKey, int slotIndex, int slotCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ArgumentOutOfRangeException.ThrowIfNegative(slotIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(slotCount);
        if (slotIndex >= slotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slotIndex),
                slotIndex,
                "A journal slot index must be less than the configured slot count.");
        }

        return string.Concat(
            partitionKey,
            ":journal-slot:",
            slotIndex.ToString("D8", CultureInfo.InvariantCulture));
    }

    public static string CreateSnapshotSlotKey(string partitionKey, int slotIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ValidateSnapshotSlot(slotIndex, nameof(slotIndex));
        return string.Concat(
            partitionKey,
            ":snapshot-slot:",
            slotIndex.ToString(CultureInfo.InvariantCulture));
    }

    public static void ValidateOptions(int journalSegmentCapacity, int maxReplayEntries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(journalSegmentCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxReplayEntries);

        var replaySegmentCount =
            (maxReplayEntries + (long)journalSegmentCapacity - 1) / journalSegmentCapacity;
        if (replaySegmentCount > int.MaxValue - 2L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxReplayEntries),
                maxReplayEntries,
                "The replay limit and segment capacity produce more journal slots than can be addressed.");
        }
    }

    public static void ValidateSnapshotSlot(int slot, string parameterName)
    {
        if ((uint)slot >= SnapshotSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                slot,
                $"A snapshot slot must be between 0 and {SnapshotSlotCount - 1}.");
        }
    }
}
