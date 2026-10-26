namespace Tourbillon.Log;

/// <summary>
/// The storage the logs of one instance may use: the free space of the disk, and an
/// optional maximum on top of it, with a reserve that only entries marked
/// <see cref="EntryFlags.MayUseReserve"/> may use.
/// </summary>
sealed class StorageLimit {
    readonly Func<long> _freeSpace;
    readonly long? _maximumSize;
    readonly long _reserveSize;
    readonly object _lock = new();
    long _used;
    long _inFlight;

    /// <summary>
    /// A limit for the logs under <paramref name="dataDirectory"/>.
    /// </summary>
    public StorageLimit(string dataDirectory, long? maximumSize, long reserveSize) {
        // One DriveInfo per limit, made once: a measurement is one statfs or
        // GetDiskFreeSpaceEx, no allocation. On Windows the drive is made from the
        // root of the path; on Unix statfs takes any path in the file system.
        var fullPath = Path.GetFullPath(dataDirectory);
        var drive = new DriveInfo(OperatingSystem.IsWindows() ? Path.GetPathRoot(fullPath)! : fullPath);
        _freeSpace = () => drive.AvailableFreeSpace;
        _maximumSize = maximumSize;
        _reserveSize = reserveSize;
    }

    /// <summary>
    /// A limit that measures free space with <paramref name="freeSpace"/>; for tests.
    /// </summary>
    public StorageLimit(Func<long> freeSpace, long? maximumSize, long reserveSize) {
        _freeSpace = freeSpace;
        _maximumSize = maximumSize;
        _reserveSize = reserveSize;
    }

    /// <summary>
    /// The number of bytes of all logs on disk.
    /// </summary>
    public long Used {
        get {
            lock (_lock)
                return _used;
        }
    }

    /// <summary>
    /// Takes room for the longest start of <paramref name="entries"/> that fits, and
    /// holds it until <see cref="Settle"/>.
    /// </summary>
    /// <returns>The number of entries that fit.</returns>
    public int TryTake(ReadOnlySpan<Entry> entries, out long bytes) {
        lock (_lock) {
            // The free disk space always counts, the maximum on top of it: a disk
            // that fills up for other reasons refuses early instead of failing a
            // write.
            var room = _freeSpace() - _inFlight;
            if (_maximumSize is { } maximum)
                room = Math.Min(room, maximum - _used - _inFlight);

            // Per entry in order: an unflagged one fits while the bytes of the batch
            // up to and including it stay within the room minus the reserve, a
            // flagged one within the room. The first that does not fit ends the
            // batch: a log cannot skip an index.
            var count = 0;
            long taken = 0;
            foreach (var entry in entries) {
                var length = RecordFormat.GetLength(entry.Payload.Length);
                var allowed = (entry.Flags & EntryFlags.MayUseReserve) != 0 ? room : room - _reserveSize;
                if (taken + length > allowed)
                    break;

                taken += length;
                count++;
            }

            _inFlight += taken;
            bytes = taken;
            return count;
        }
    }

    /// <summary>
    /// Ends a <see cref="TryTake"/>: the bytes count as used when they were written,
    /// and are given back when not.
    /// </summary>
    public void Settle(long bytes, bool written) {
        lock (_lock) {
            _inFlight -= bytes;
            if (written)
                _used += bytes;
        }
    }

    /// <summary>
    /// Counts bytes a log already has on disk when it opens.
    /// </summary>
    public void Add(long bytes) {
        lock (_lock)
            _used += bytes;
    }

    /// <summary>
    /// Gives back bytes a log removed or truncated, or holds no more because it closed.
    /// </summary>
    public void Release(long bytes) {
        lock (_lock)
            _used -= bytes;
    }
}
