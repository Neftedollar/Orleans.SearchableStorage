namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Freezes the first local-functional SkyPulse index identity.
/// </summary>
public static class SkyPulseIndexContract
{
    public const string ProviderName = "SkyPulse.AccountIndex.v1";

    public const string StateName = "account-projection";

    public const string GrainType = "skypulse-account";

    public const int ApplicationSchemaVersion = 1;

    // This is intentionally conservative for the 17-entry schema. A later provider identity is
    // required if qualification evidence says the owner-size limit needs a different layout.
    public const int PartitionCount = 256;

    public const int VirtualSlotTargetCount = 8_192;
}
