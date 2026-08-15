namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

internal static class PublicArtifactPrivacy
{
    private static ReadOnlySpan<byte> ForbiddenDidPrefix => "did:"u8;

    public static void VerifyDirectory(string directory, IReadOnlySet<string> expectedFileNames)
    {
        if (Directory.EnumerateDirectories(directory).Any())
        {
            throw new InvalidDataException("A published corpus directory cannot contain subdirectories.");
        }

        var observed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var path in Directory.EnumerateFiles(directory))
        {
            var name = Path.GetFileName(path);
            if (!expectedFileNames.Contains(name))
            {
                throw new InvalidDataException($"Unexpected public corpus artifact '{name}'.");
            }

            observed.Add(name);
            VerifyFile(path);
        }

        if (!observed.SetEquals(expectedFileNames))
        {
            throw new InvalidDataException("The published corpus directory is missing an expected artifact.");
        }
    }

    private static void VerifyFile(string path)
    {
        var matched = 0;
        using var stream = File.OpenRead(path);
        Span<byte> buffer = stackalloc byte[32 * 1024];
        while (true)
        {
            var read = stream.Read(buffer);
            if (read == 0)
            {
                return;
            }

            foreach (var value in buffer[..read])
            {
                if (value == ForbiddenDidPrefix[matched])
                {
                    matched++;
                    if (matched == ForbiddenDidPrefix.Length)
                    {
                        throw new InvalidDataException(
                            $"Public artifact '{Path.GetFileName(path)}' contains forbidden DID text.");
                    }
                }
                else
                {
                    matched = value == ForbiddenDidPrefix[0] ? 1 : 0;
                }
            }
        }
    }
}
