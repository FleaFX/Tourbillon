namespace Tourbillon.Log;

/// <summary>
/// One open file, written and read at explicit positions.
/// </summary>
interface IFile : IDisposable {
    /// <summary>
    /// The path the file was opened with.
    /// </summary>
    string Path { get; }

    /// <summary>
    /// The number of bytes the file holds.
    /// </summary>
    long Length { get; }

    /// <summary>
    /// Writes <paramref name="data"/> at <paramref name="position"/>; the bytes are
    /// in the file until <see cref="FlushToDisk"/> makes them durable.
    /// </summary>
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position);

    /// <summary>
    /// Makes the bytes written durable.
    /// </summary>
    void FlushToDisk();

    /// <summary>
    /// Sets the length of the file, cutting it or extending it with zeros.
    /// </summary>
    void SetLength(long length);

    /// <summary>
    /// Reads into <paramref name="buffer"/> from <paramref name="position"/>.
    /// </summary>
    /// <returns>The number of bytes read; less than the buffer holds when the file
    /// ends.</returns>
    ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default);
}
