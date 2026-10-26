namespace Tourbillon.Log;

// A file a segment can open its file with, whose flush to disk is held back until
// the test opens the gate: the append must not complete before the fsync.
sealed class GatedFlushFile : IFile {
    readonly IFile _file;

    public GatedFlushFile(string path) {
        _file = DiskFile.Open(path);
    }

    public string Path => _file.Path;

    public long Length => _file.Length;

    public TaskCompletionSource FlushStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource FlushGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position) =>
        _file.WriteAsync(data, position);

    public void FlushToDisk() {
        FlushStarted.TrySetResult();
        FlushGate.Task.GetAwaiter().GetResult();
        _file.FlushToDisk();
    }

    public void SetLength(long length) => _file.SetLength(length);

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default) =>
        _file.ReadAsync(buffer, position, cancellationToken);

    public void Dispose() => _file.Dispose();
}

// Counts the write calls and the flushes to disk, so a test can see that one batch
// costs one write and one fsync.
sealed class CountingFile : IFile {
    readonly IFile _file;

    public CountingFile(string path) {
        _file = DiskFile.Open(path);
    }

    public string Path => _file.Path;

    public long Length => _file.Length;

    public int WriteCount { get; private set; }

    public int FlushToDiskCount { get; private set; }

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position) {
        WriteCount++;
        return _file.WriteAsync(data, position);
    }

    public void FlushToDisk() {
        FlushToDiskCount++;
        _file.FlushToDisk();
    }

    public void SetLength(long length) => _file.SetLength(length);

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default) =>
        _file.ReadAsync(buffer, position, cancellationToken);

    public void Dispose() => _file.Dispose();
}

// Fails the next write, the next flush to disk, the next truncate, or the flush after
// a truncate, each on a switch of its own.
sealed class FaultyFile : IFile {
    readonly IFile _file;

    public FaultyFile(string path) {
        _file = DiskFile.Open(path);
    }

    public string Path => _file.Path;

    public long Length => _file.Length;

    public bool FailNextWrite { get; set; }

    public bool FailNextFlushToDisk { get; set; }

    public bool FailNextSetLength { get; set; }

    public bool FailFlushAfterSetLength { get; set; }

    bool _lengthChanged;

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position) {
        if (FailNextWrite) {
            FailNextWrite = false;
            throw new IOException("Simulated write failure.");
        }

        return _file.WriteAsync(data, position);
    }

    public void SetLength(long length) {
        if (FailNextSetLength) {
            FailNextSetLength = false;
            throw new IOException("Simulated truncate failure.");
        }

        _file.SetLength(length);
        _lengthChanged = true;
    }

    public void FlushToDisk() {
        if (FailFlushAfterSetLength && _lengthChanged) {
            FailFlushAfterSetLength = false;
            _lengthChanged = false;
            throw new IOException("Simulated flush failure after a truncate.");
        }

        if (FailNextFlushToDisk) {
            FailNextFlushToDisk = false;
            throw new IOException("Simulated flush failure.");
        }

        _file.FlushToDisk();
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default) =>
        _file.ReadAsync(buffer, position, cancellationToken);

    public void Dispose() => _file.Dispose();
}
