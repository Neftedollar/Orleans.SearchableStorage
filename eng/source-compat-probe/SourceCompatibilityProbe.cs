namespace Orleans.SearchableStorage.SourceCompatibility;

#if OSS_SOURCE_COMPAT_NEGATIVE
public sealed class SourceCompatibilityProbe<T>
#else
public sealed class SourceCompatibilityProbe<T>
    where T : notnull
#endif
{
}
