namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

/// <summary>
/// Enforces the local-filesystem boundary for intermediate files which contain canonical DIDs.
/// The qualification deployment is Linux; Windows keeps the structural regular-file checks but
/// relies on its deployment ACL instead of Unix modes.
/// </summary>
internal static class PrivateWorkspacePermissions
{
    internal const UnixFileMode PrivateFileMode =
        UnixFileMode.UserRead | UnixFileMode.UserWrite;

    internal const UnixFileMode PrivateDirectoryMode =
        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;

    public static void CreateDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            ValidateDirectory(path);
            return;
        }

        if (File.Exists(path))
        {
            throw new IOException("A private workspace path is already a file.");
        }

        if (OperatingSystem.IsWindows())
        {
            Directory.CreateDirectory(path);
        }
        else
        {
            Directory.CreateDirectory(path, PrivateDirectoryMode);
        }

        ValidateDirectory(path);
    }

    public static void ValidateDirectory(string path)
    {
        var directory = new DirectoryInfo(path);
        if (!directory.Exists
            || directory.LinkTarget is not null
            || (directory.Attributes & FileAttributes.ReparsePoint) != 0)
        {
            throw new IOException("A private workspace must be a real directory, not a link.");
        }

        if (!OperatingSystem.IsWindows()
            && File.GetUnixFileMode(path) != PrivateDirectoryMode)
        {
            throw new IOException("A private workspace directory must have Unix mode 0700.");
        }
    }

    public static void ApplyPrivateCreateMode(FileStreamOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = PrivateFileMode;
        }
    }

    public static void ValidateRegularFile(string path)
    {
        var file = new FileInfo(path);
        if (!file.Exists
            || file.LinkTarget is not null
            || (file.Attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != 0)
        {
            throw new IOException("A private artifact must be a regular file, not a link.");
        }

        if (!OperatingSystem.IsWindows()
            && File.GetUnixFileMode(path) != PrivateFileMode)
        {
            throw new IOException("A private artifact must have Unix mode 0600.");
        }
    }

    public static void ValidateRegularFile(FileStream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var attributes = File.GetAttributes(stream.SafeFileHandle);
        if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != 0)
        {
            throw new IOException("An opened private artifact is not a regular file.");
        }

        if (!OperatingSystem.IsWindows()
            && File.GetUnixFileMode(stream.SafeFileHandle) != PrivateFileMode)
        {
            throw new IOException("An opened private artifact must have Unix mode 0600.");
        }
    }
}
