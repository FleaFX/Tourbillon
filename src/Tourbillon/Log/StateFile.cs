using System.Buffers.Binary;

namespace Tourbillon.Log;

/// <summary>
/// Small files next to a log, written so that after a crash each is the old version or
/// the new one, never half of each.
/// </summary>
static class StateFile {
    /// <summary>
    /// Writes <paramref name="content"/> as the file <paramref name="name"/> in
    /// <paramref name="directory"/> through <paramref name="fileSystem"/>: to a temp
    /// file, fsynced, renamed over the old one, and the directory fsynced.
    /// </summary>
    public static async ValueTask WriteAsync(
        IFileSystem fileSystem,
        string directory,
        string name,
        ReadOnlyMemory<byte> content,
        CancellationToken cancellationToken = default
    ) {
        var path = Path.Combine(directory, name);
        var temp = path + ".tmp";

        // On disk: the length of the content u32, its crc32c u32, then the content.
        var buffer = new byte[8 + content.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0, 4), (uint)content.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4, 4), Crc32C.ComputeHash(content.Span));
        content.Span.CopyTo(buffer.AsSpan(8));

        // A leftover temp file of a crashed write is overwritten: opening a file
        // does not cut it, so the write starts by cutting it to zero.
        using (var file = fileSystem.OpenFile(temp)) {
            file.SetLength(0);
            await file.WriteAsync(buffer, 0).ConfigureAwait(false);
            file.FlushToDisk();
        }

        // The rename makes the new version appear in one step: after a crash the file
        // is the old version or the new one, never half of each.
        fileSystem.MoveFile(temp, path);
        fileSystem.FlushDirectory(directory);
    }

    /// <summary>
    /// Reads the file <paramref name="name"/> in <paramref name="directory"/> through
    /// <paramref name="fileSystem"/>, or <see langword="null"/> when it does not exist.
    /// </summary>
    /// <exception cref="InvalidDataException">The file is damaged.</exception>
    public static async ValueTask<byte[]?> ReadAsync(
        IFileSystem fileSystem,
        string directory,
        string name,
        CancellationToken cancellationToken = default
    ) {
        var path = Path.Combine(directory, name);
        if (!fileSystem.FileExists(path))
            return null;

        using var file = fileSystem.OpenFile(path);
        var bytes = new byte[file.Length];
        var read = 0;
        while (read < bytes.Length) {
            var readNow = await file.ReadAsync(bytes.AsMemory(read), read, cancellationToken).ConfigureAwait(false);
            if (readNow == 0)
                break;

            read += readNow;
        }

        if (bytes.Length < 8)
            throw new InvalidDataException($"The state file '{path}' is damaged: it holds less than a header.");

        var length = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4));
        if (length != (uint)(bytes.Length - 8))
            throw new InvalidDataException($"The state file '{path}' is damaged: its length does not match its content.");

        var crc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4, 4));
        if (crc != Crc32C.ComputeHash(bytes.AsSpan(8)))
            throw new InvalidDataException($"The state file '{path}' is damaged: its check number does not match its content.");

        return bytes.AsSpan(8).ToArray();
    }
}
