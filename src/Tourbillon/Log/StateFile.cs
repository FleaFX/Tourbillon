using System.Buffers.Binary;

namespace Tourbillon.Log;

/// <summary>
/// Small files next to a log, written so that after a crash each is the old version or
/// the new one, never half of each.
/// </summary>
static class StateFile {
    /// <summary>
    /// Writes <paramref name="content"/> as the file <paramref name="name"/> in
    /// <paramref name="directory"/>: to a temp file, fsynced, renamed over the old one,
    /// and the directory fsynced.
    /// </summary>
    public static async ValueTask WriteAsync(
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

        // A leftover temp file of a crashed write is overwritten.
        using (var file = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 0, FileOptions.Asynchronous)) {
            await file.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            file.Flush(flushToDisk: true);
        }

        // The rename makes the new version appear in one step: after a crash the file
        // is the old version or the new one, never half of each.
        File.Move(temp, path, overwrite: true);
        DirectorySync.Flush(directory);
    }

    /// <summary>
    /// Reads the file <paramref name="name"/> in <paramref name="directory"/>, or
    /// <see langword="null"/> when it does not exist.
    /// </summary>
    /// <exception cref="InvalidDataException">The file is damaged.</exception>
    public static async ValueTask<byte[]?> ReadAsync(
        string directory,
        string name,
        CancellationToken cancellationToken = default
    ) {
        var path = Path.Combine(directory, name);
        if (!File.Exists(path))
            return null;

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
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
