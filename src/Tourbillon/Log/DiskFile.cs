using Microsoft.Win32.SafeHandles;

namespace Tourbillon.Log;

/// <summary>
/// A file on disk: one handle, opened for reading and writing, written and read
/// at explicit positions.
/// </summary>
sealed class DiskFile : IFile {
    readonly SafeFileHandle _handle;
    readonly string _path;

    /// <summary>
    /// The file on the handle <paramref name="handle"/>, opened as
    /// <paramref name="path"/>.
    /// </summary>
    public DiskFile(SafeFileHandle handle, string path) {
        _handle = handle;
        _path = path;
    }

    /// <summary>
    /// Opens or creates the file at <paramref name="path"/> for reading and
    /// writing; others may read it.
    /// </summary>
    public static DiskFile Open(string path) =>
        new(File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous), path);

    /// <inheritdoc/>
    public string Path => _path;

    /// <inheritdoc/>
    public long Length => RandomAccess.GetLength(_handle);

    /// <inheritdoc/>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position) =>
        RandomAccess.WriteAsync(_handle, data, position);

    /// <inheritdoc/>
    public void FlushToDisk() =>
        RandomAccess.FlushToDisk(_handle);

    /// <inheritdoc/>
    public void SetLength(long length) =>
        RandomAccess.SetLength(_handle, length);

    /// <inheritdoc/>
    public ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default) =>
        RandomAccess.ReadAsync(_handle, buffer, position, cancellationToken);

    /// <inheritdoc/>
    public void Dispose() => _handle.Dispose();
}
