namespace Tourbillon.Log;

// A file stream a segment can open its file with, whose flush to disk is held back
// until the test opens the gate: the append must not complete before the fsync.
sealed class GatedFlushFileStream(string path)
    : FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, FileOptions.Asynchronous) {
    public TaskCompletionSource FlushStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource FlushGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override void Flush(bool flushToDisk) {
        if (flushToDisk) {
            FlushStarted.TrySetResult();
            FlushGate.Task.GetAwaiter().GetResult();
        }

        base.Flush(flushToDisk);
    }
}

// Counts the write calls and the flushes to disk, so a test can see that one batch
// costs one write and one fsync.
sealed class CountingFileStream(string path)
    : FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, FileOptions.Asynchronous) {
    public int WriteCount { get; private set; }

    public int FlushToDiskCount { get; private set; }

    public override void Write(ReadOnlySpan<byte> buffer) {
        WriteCount++;
        base.Write(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) {
        WriteCount++;
        return base.WriteAsync(buffer, cancellationToken);
    }

    public override void Flush(bool flushToDisk) {
        if (flushToDisk)
            FlushToDiskCount++;

        base.Flush(flushToDisk);
    }
}

// Fails the next write, the next flush to disk, the next truncate, or the flush after
// a truncate, each on a switch of its own.
sealed class FaultyFileStream(string path)
    : FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, bufferSize: 0, FileOptions.Asynchronous) {
    public bool FailNextWrite { get; set; }

    public bool FailNextFlushToDisk { get; set; }

    public bool FailNextSetLength { get; set; }

    public bool FailFlushAfterSetLength { get; set; }

    bool _lengthChanged;

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) {
        if (FailNextWrite) {
            FailNextWrite = false;
            throw new IOException("Simulated write failure.");
        }

        return base.WriteAsync(buffer, cancellationToken);
    }

    public override void SetLength(long value) {
        if (FailNextSetLength) {
            FailNextSetLength = false;
            throw new IOException("Simulated truncate failure.");
        }

        base.SetLength(value);
        _lengthChanged = true;
    }

    public override void Flush(bool flushToDisk) {
        if (flushToDisk) {
            if (FailFlushAfterSetLength && _lengthChanged) {
                FailFlushAfterSetLength = false;
                _lengthChanged = false;
                throw new IOException("Simulated flush failure after a truncate.");
            }

            if (FailNextFlushToDisk) {
                FailNextFlushToDisk = false;
                throw new IOException("Simulated flush failure.");
            }
        }

        base.Flush(flushToDisk);
    }
}
