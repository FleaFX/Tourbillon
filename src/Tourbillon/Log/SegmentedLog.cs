using Tourbillon.Raft;

namespace Tourbillon.Log;

/// <summary>
/// A log on disk: a directory of segments, appended to at the end, truncated from an
/// index and removed from the start a whole segment at a time. One writer at a time;
/// the entries a reader takes are published, so a reader beside the writer sees the
/// entries a commit index points at.
/// </summary>
sealed class SegmentedLog : IRaftLog, IDisposable {
    readonly IFileSystem _fs;
    readonly string _directory;
    readonly long _segmentSize;
    readonly StorageLimit _storage;
    readonly object _segmentsLock = new();
    readonly List<Segment> _segments = new();
    IOException? _fault;
    bool _disposed;

    SegmentedLog(IFileSystem fs, string directory, long segmentSize, StorageLimit storage) {
        _fs = fs;
        _directory = directory;
        _segmentSize = segmentSize;
        _storage = storage;
    }

    /// <summary>
    /// Opens or creates the log in <paramref name="directory"/> through
    /// <paramref name="fileSystem"/> and recovers it: every segment is read, only the
    /// last one may have a torn tail, and damage in a sealed segment or a gap
    /// between segments refuses to open.
    /// </summary>
    public static async ValueTask<SegmentedLog> OpenAsync(
        IFileSystem fileSystem,
        string directory,
        long segmentSize,
        StorageLimit storage,
        CancellationToken cancellationToken = default
    ) {
        var existed = fileSystem.DirectoryExists(directory);
        fileSystem.CreateDirectory(directory);
        if (!existed) {
            // The directory itself is new: its entry in the parent must be durable.
            var parent = Path.GetDirectoryName(Path.GetFullPath(directory));
            if (parent is { Length: > 0 })
                fileSystem.FlushDirectory(parent);
        }

        var files = new List<(string Path, ulong FirstIndex)>();
        foreach (var file in fileSystem.GetFiles(directory)) {
            // Other files (state files, *.tmp) are not segments and are left alone.
            if (Path.GetExtension(file) != ".seg")
                continue;

            files.Add((file, ParseFirstIndex(file)));
        }

        // The names are 20 digits, so sorting by first index puts the segments in order.
        files.Sort((x, y) => x.FirstIndex.CompareTo(y.FirstIndex));

        var log = new SegmentedLog(fileSystem, directory, segmentSize, storage);
        try {
            for (var i = 0; i < files.Count; i++) {
                // Only the last segment may have a torn tail.
                var segment = await Segment.OpenAsync(fileSystem, files[i].Path, files[i].FirstIndex, isSealed: i < files.Count - 1, cancellationToken).ConfigureAwait(false);
                if (i > 0) {
                    var previous = log._segments[^1];
                    if (segment.FirstIndex != previous.LastIndex + 1)
                        throw new InvalidDataException($"The log in '{directory}' has a gap between the segment '{previous.Path}' (up to {previous.LastIndex}) and the segment '{segment.Path}' (from {segment.FirstIndex}).");
                }

                log._segments.Add(segment);
            }

            // A new log starts with one empty segment.
            if (log._segments.Count == 0) {
                var segment = await Segment.OpenAsync(fileSystem, Path.Combine(directory, $"{1UL:D20}.seg"), 1, isSealed: false, cancellationToken).ConfigureAwait(false);
                log._segments.Add(segment);
                fileSystem.FlushDirectory(directory);
            }
        } catch {
            foreach (var segment in log._segments)
                segment.Dispose();

            throw;
        }

        // The bytes the log already has on disk count from now on.
        storage.Add(log.Length);

        return log;
    }

    /// <summary>
    /// The index of the first entry still kept, also when the log is empty.
    /// </summary>
    public ulong FirstIndex {
        get {
            lock (_segmentsLock)
                return _segments[0].FirstIndex;
        }
    }

    /// <summary>
    /// The index of the last entry, or <see cref="FirstIndex"/> - 1 when the log is empty.
    /// </summary>
    public ulong LastIndex {
        get {
            lock (_segmentsLock)
                return _segments[^1].LastIndex;
        }
    }

    /// <summary>
    /// The number of bytes of the log's records on disk.
    /// </summary>
    public long Length {
        get {
            lock (_segmentsLock) {
                long total = 0;
                foreach (var segment in _segments)
                    total += segment.Length;

                return total;
            }
        }
    }

    /// <summary>
    /// Whether the log is faulted and refuses everything.
    /// </summary>
    public bool IsFaulted => _fault is not null;

    /// <summary>
    /// The term of the entry at <paramref name="index"/>, from memory; 0 for the place
    /// before the first entry.
    /// </summary>
    public ulong GetTerm(ulong index) {
        if (index == 0)
            return 0;

        lock (_segmentsLock)
            return FindSegment(index).GetTerm(index);
    }

    /// <summary>
    /// The number of bytes the record of the entry at <paramref name="index"/> takes.
    /// </summary>
    public int GetRecordLength(ulong index) {
        lock (_segmentsLock)
            return FindSegment(index).GetRecordLength(index);
    }

    /// <summary>
    /// Appends the longest start of <paramref name="entries"/> that fits in the storage
    /// limit, with one write and one fsync; the entries after it are refused because
    /// storage is full and touch nothing.
    /// </summary>
    /// <returns>The number of entries appended; 0 when the first one does not fit.</returns>
    public async ValueTask<int> AppendAsync(ReadOnlyMemory<Entry> entries) {
        if (_fault is { } fault)
            throw fault;

        if (entries.Length == 0)
            return 0;

        var count = _storage.TryTake(entries.Span, out var bytes);
        if (count == 0)
            return 0;

        var written = false;
        Segment active;
        lock (_segmentsLock)
            active = _segments[^1];
        try {
            // Rotation before the batch: an idle log keeps no empty file.
            if (active.LastIndex >= active.FirstIndex && active.Length >= _segmentSize) {
                try {
                    active = await OpenNextSegmentAsync(active.LastIndex).ConfigureAwait(false);
                } catch (Exception exception) {
                    // The new file may be on disk without the log using it: a later
                    // truncate would let the old segment grow over its first index,
                    // and the next open would find an overlap.
                    Fault(exception);
                    throw;
                }
            }

            await active.AppendAsync(entries.Slice(0, count)).ConfigureAwait(false);
            written = true;
            return count;
        } catch (Exception exception) {
            if (active.IsFaulted)
                Fault(exception);

            throw;
        } finally {
            _storage.Settle(bytes, written);
        }
    }

    /// <summary>
    /// Removes the entry at <paramref name="index"/> and every entry after it.
    /// </summary>
    public void TruncateFrom(ulong index) {
        if (_fault is { } fault)
            throw fault;

        if (index < FirstIndex || index > LastIndex + 1)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within {FirstIndex} .. {LastIndex + 1}.");

        if (index == LastIndex + 1)
            return;

        var lengthBefore = Length;
        try {
            // Delete every segment that starts above the index, the newest first:
            // after every step the log on disk is the old log cut at some index at
            // or above the given one, contiguous, also when a crash comes in between.
            // The lock guards the list only; the file operations run beside it.
            while (true) {
                Segment segment;
                lock (_segmentsLock) {
                    if (_segments[^1].FirstIndex <= index)
                        break;

                    segment = _segments[^1];
                    _segments.RemoveAt(_segments.Count - 1);
                }

                segment.Dispose();
                _fs.DeleteFile(segment.Path);
                _fs.FlushDirectory(_directory);
            }

            // Truncate the segment that holds the index; it is now the active one,
            // also when that leaves it empty.
            Segment active;
            lock (_segmentsLock)
                active = _segments[^1];

            active.TruncateFrom(index);
        } catch (Exception exception) {
            Fault(exception);
            throw;
        }

        _storage.Release(lengthBefore - Length);
    }

    /// <summary>
    /// Removes every segment whose entries all lie at or below <paramref name="index"/>;
    /// never part of a segment.
    /// </summary>
    public async ValueTask RemoveUpToAsync(ulong index) {
        if (_fault is { } fault)
            throw fault;

        if (index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be at or below {LastIndex}.");

        if (index < FirstIndex)
            return;

        var lengthBefore = Length;
        try {
            // Rotate first when the active segment lies completely at or below the
            // index, so that the log always has a file that says where it starts.
            Segment active;
            lock (_segmentsLock)
                active = _segments[^1];

            if (active.LastIndex >= active.FirstIndex && active.LastIndex <= index)
                await OpenNextSegmentAsync(active.LastIndex).ConfigureAwait(false);

            // Delete, oldest first, every segment that holds at least one entry and
            // whose last index is at or below the index: the remaining segments stay
            // a contiguous suffix after a crash at any point. The lock guards the
            // list only; the file operations run beside it.
            while (true) {
                Segment segment;
                lock (_segmentsLock) {
                    if (_segments.Count <= 1 || _segments[0].LastIndex < _segments[0].FirstIndex || _segments[0].LastIndex > index)
                        break;

                    segment = _segments[0];
                    _segments.RemoveAt(0);
                }

                segment.Dispose();
                _fs.DeleteFile(segment.Path);
                _fs.FlushDirectory(_directory);
            }
        } catch (Exception exception) {
            Fault(exception);
            throw;
        }

        _storage.Release(lengthBefore - Length);
    }

    /// <summary>
    /// Reads the entry at <paramref name="index"/> into <paramref name="buffer"/>; the
    /// payload of the returned entry points into it.
    /// </summary>
    public ValueTask<Entry> ReadAsync(ulong index, Memory<byte> buffer, CancellationToken cancellationToken = default) {
        if (_fault is { } fault)
            throw fault;

        Segment segment;
        lock (_segmentsLock)
            segment = FindSegment(index);

        return segment.ReadAsync(index, buffer, cancellationToken);
    }

    /// <summary>
    /// Reads the entries from <paramref name="firstIndex"/> up to at most
    /// <paramref name="lastIndex"/>, as many whole records as fit in the buffer and
    /// the entry array, with one read; the payloads point into the buffer. One call
    /// reads within one segment: the batch may end at a segment boundary.
    /// </summary>
    /// <returns>The number of entries read; at least 1, the buffer must hold the first record.</returns>
    public async ValueTask<int> ReadAsync(ulong firstIndex, ulong lastIndex, Memory<byte> buffer, Memory<Entry> entries, CancellationToken cancellationToken = default) {
        if (_fault is { } fault)
            throw fault;

        Segment segment;
        lock (_segmentsLock) {
            segment = FindSegment(firstIndex);
            if (lastIndex < firstIndex)
                throw new ArgumentOutOfRangeException(nameof(lastIndex), lastIndex, $"The last index must be at or above the first one ({firstIndex}).");
        }

        return await segment.ReadAsync(firstIndex, lastIndex, buffer, entries, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Dispose() {
        if (_disposed)
            return;

        _disposed = true;
        Segment[] segments;
        lock (_segmentsLock)
            segments = _segments.ToArray();

        foreach (var segment in segments)
            segment.Dispose();

        // The log holds no more.
        _storage.Release(Length);
    }

    /// <summary>
    /// Opens the next segment, an empty file named after the first index it will hold,
    /// fsyncs the directory and makes it the active one.
    /// </summary>
    async ValueTask<Segment> OpenNextSegmentAsync(ulong lastIndex) {
        var path = Path.Combine(_directory, $"{(lastIndex + 1):D20}.seg");
        var segment = await Segment.OpenAsync(_fs, path, lastIndex + 1, isSealed: false).ConfigureAwait(false);
        try {
            _fs.FlushDirectory(_directory);
        } catch {
            segment.Dispose();
            throw;
        }

        lock (_segmentsLock)
            _segments.Add(segment);

        return segment;
    }

    /// <summary>
    /// The first index of the segment file <paramref name="path"/>, from its name: 20
    /// digits, not all zero (indexes start at 1). Any other name is damage.
    /// </summary>
    static ulong ParseFirstIndex(string path) {
        var name = Path.GetFileNameWithoutExtension(path);
        if (name.Length != 20 || !name.All(char.IsAsciiDigit) || !ulong.TryParse(name, out var firstIndex) || firstIndex == 0)
            throw new InvalidDataException($"The file '{path}' is not a segment of a log: its name is not 20 digits of an index from 1.");

        return firstIndex;
    }

    /// <summary>
    /// The segment that holds <paramref name="index"/>: the indexes of a log have no
    /// holes, so it is the last segment whose first index is at or below it. The lock
    /// on the list of segments must be held.
    /// </summary>
    Segment FindSegment(ulong index) {
        if (index < _segments[0].FirstIndex || index > _segments[^1].LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within {_segments[0].FirstIndex} .. {_segments[^1].LastIndex}.");

        var i = _segments.Count - 1;
        while (index < _segments[i].FirstIndex)
            i--;

        return _segments[i];
    }

    /// <summary>
    /// Faults the log: the log in memory no longer matches what is on disk, and every
    /// later call throws the fault.
    /// </summary>
    void Fault(Exception cause) =>
        _fault ??= new IOException($"The log in '{_directory}' is faulted and refuses everything.", cause);
}
