namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

public static class CorpusVerifier
{
    public static CorpusVerificationResult Verify(
        string manifestPath,
        bool deep,
        string? sourceJournalPath = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        var canonicalManifestPath = Path.GetFullPath(manifestPath);
        if (!File.Exists(canonicalManifestPath))
        {
            throw new FileNotFoundException("The corpus manifest does not exist.", canonicalManifestPath);
        }

        if (!string.Equals(
                Path.GetFileName(canonicalManifestPath),
                CorpusFormat.ManifestName,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException($"The manifest file must be named '{CorpusFormat.ManifestName}'.");
        }

        var manifest = CorpusManifestCodec.ReadCanonical(canonicalManifestPath);
        var directory = Path.GetDirectoryName(canonicalManifestPath)!;
        var binaryPath = Path.Combine(directory, CorpusFormat.BinaryArtifactName);
        var parentEvidence = ArtifactEvidence.InspectParent(
            binaryPath,
            manifest.Parent.AccountCount,
            verifyOrder: deep);
        if (!string.Equals(parentEvidence.Sha256, manifest.Parent.Sha256, StringComparison.Ordinal)
            || !string.Equals(
                parentEvidence.CorpusFingerprint,
                manifest.Parent.CorpusFingerprint,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException("The parent corpus hashes do not match the canonical manifest.");
        }

        foreach (var profile in manifest.Profiles)
        {
            var observed = ArtifactEvidence.HashPrefix(binaryPath, profile.ByteLength);
            if (!string.Equals(observed, profile.PrefixSha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Exact-prefix profile '{profile.Name}' does not match its SHA-256.");
            }
        }

        if (manifest.HumanReadableArtifact is { } human)
        {
            var humanPath = Path.Combine(directory, human.Artifact);
            var observed = ArtifactEvidence.HashFile(humanPath);
            if (observed.ByteLength != human.ByteLength
                || !string.Equals(observed.Sha256, human.Sha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The human-readable artifact does not match its manifest.");
            }

            ArtifactEvidence.VerifyHumanReadable(binaryPath, humanPath, manifest.Parent.AccountCount);
        }

        PublicArtifactPrivacy.VerifyDirectory(
            directory,
            manifest.HumanReadableArtifact is null
                ? new HashSet<string>(
                    [CorpusFormat.BinaryArtifactName, CorpusFormat.ManifestName],
                    StringComparer.Ordinal)
                : new HashSet<string>(
                    [CorpusFormat.BinaryArtifactName, CorpusFormat.HumanArtifactName, CorpusFormat.ManifestName],
                    StringComparer.Ordinal));

        var sourceVerified = false;
        if (sourceJournalPath is not null)
        {
            var canonicalSourceJournalPath = Path.GetFullPath(sourceJournalPath);
            PrivateWorkspacePermissions.ValidateRegularFile(canonicalSourceJournalPath);
            var sourceEvidence = ArtifactEvidence.HashFile(canonicalSourceJournalPath);
            if (sourceEvidence.ByteLength != manifest.SourceJournal.ByteLength
                || !string.Equals(sourceEvidence.Sha256, manifest.SourceJournal.Sha256, StringComparison.Ordinal))
            {
                throw new InvalidDataException("The supplied source journal does not match the manifest.");
            }

            sourceVerified = true;
        }

        return new CorpusVerificationResult(
            canonicalManifestPath,
            manifest.Parent.AccountCount,
            deep,
            sourceVerified);
    }
}
