using System.Runtime.InteropServices;

namespace Tourbillon.Log;

/// <summary>
/// Makes the creation, renaming and deletion of files in a directory durable.
/// </summary>
static partial class DirectorySync {
    /// <summary>
    /// Flushes the directory at <paramref name="path"/> to disk; on Windows the file
    /// system already does so, and this does nothing.
    /// </summary>
    public static void Flush(string path) {
        if (OperatingSystem.IsWindows())
            return;

        // .NET cannot open a directory as a SafeFileHandle (it throws
        // UnauthorizedAccessException), so there is no managed way. O_RDONLY is 0 on
        // Linux and macOS, and a directory opens read-only without O_DIRECTORY,
        // whose value differs per architecture.
        var descriptor = open(path, 0);
        if (descriptor < 0)
            throw new IOException($"Could not open the directory '{path}' to flush it.", Marshal.GetLastPInvokeError());

        try {
            if (fsync(descriptor) != 0)
                throw new IOException($"Could not flush the directory '{path}'.", Marshal.GetLastPInvokeError());
        } finally {
            close(descriptor);
        }
    }

    [LibraryImport("libc", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int open(string path, int flags);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int fsync(int descriptor);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int close(int descriptor);
}
