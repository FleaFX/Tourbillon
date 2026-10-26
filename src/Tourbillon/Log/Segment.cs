using System.Buffers;

namespace Tourbillon.Log;

/// <summary>
/// One segment file of a log: appends batches of entries with one write and one fsync,
/// recovers after a crash and reads entries by index. One writer at a time; readers
/// take the published state, so they see the entries a commit index points at.
/// </summary>
sealed class Segment : IDisposable {
    const int InitialCapacity = 1024;

    // The size of the buffer the recovery reads the file with.
    const int RecoveryBufferSize = 64 * 1024;

    readonly IFile _file;
    readonly ulong _firstIndex;
    readonly bool _isSealed;
    RecordLocation[] _locations;
    int _count;
    long _length;
    SegmentState _published;
    IOException? _fault;

    Segment(IFile file, ulong firstIndex, bool isSealed) {
        _file = file;
        _firstIndex = firstIndex;
        _isSealed = isSealed;
        _locations = new RecordLocation[InitialCapacity];
        _published = new SegmentState(_locations, 0, 0);
    }

    /// <summary>
    /// Opens or creates the segment file at <paramref name="path"/> through
    /// <paramref name="fileSystem"/>, whose first entry has index
    /// <paramref name="firstIndex"/>, and recovers it; a sealed segment refuses any
    /// bad record instead of cutting it.
    /// </summary>
    public static async ValueTask<Segment> OpenAsync(IFileSystem fileSystem, string path, ulong firstIndex, bool isSealed, CancellationToken cancellationToken = default) {
        ArgumentOutOfRangeException.ThrowIfZero(firstIndex);

        var file = fileSystem.OpenFile(path);
        try {
            return await OpenAsync(file, firstIndex, isSealed, cancellationToken).ConfigureAwait(false);
        } catch {
            file.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Recovers the segment in <paramref name="file"/>, which the segment owns from now on;
    /// a sealed segment refuses any bad record instead of cutting it.
    /// </summary>
    public static async ValueTask<Segment> OpenAsync(IFile file, ulong firstIndex, bool isSealed, CancellationToken cancellationToken = default) {
        ArgumentOutOfRangeException.ThrowIfZero(firstIndex);

        var segment = new Segment(file, firstIndex, isSealed);
        try {
            await segment.RecoverAsync(cancellationToken).ConfigureAwait(false);
        } catch {
            file.Dispose();
            throw;
        }

        return segment;
    }

    /// <summary>
    /// The index of the first entry of the segment, also when it is still empty.
    /// </summary>
    public ulong FirstIndex => _firstIndex;

    /// <summary>
    /// The index of the last entry, or <see cref="FirstIndex"/> - 1 when the segment is empty.
    /// </summary>
    public ulong LastIndex {
        get {
            var count = Published.Count;
            return count == 0 ? _firstIndex - 1 : _firstIndex + (ulong)count - 1;
        }
    }

    /// <summary>
    /// The number of bytes of the segment's records on disk.
    /// </summary>
    public long Length => Published.Length;

    /// <summary>
    /// The path of the segment file.
    /// </summary>
    public string Path => _file.Path;

    /// <summary>
    /// Whether the segment is faulted and refuses everything.
    /// </summary>
    public bool IsFaulted => _fault is not null;

    /// <summary>
    /// The published state of the segment: what a reader beside the writer sees.
    /// </summary>
    SegmentState Published => Volatile.Read(ref _published);

    /// <summary>
    /// Publishes the working state, so that a reader beside the writer sees it whole.
    /// </summary>
    void Publish() =>
        Volatile.Write(ref _published, new SegmentState(_locations, _count, _length));

    /// <summary>
    /// The term of the entry at <paramref name="index"/>, from memory.
    /// </summary>
    public ulong GetTerm(ulong index) {
        var state = Published;
        CheckIndex(index, state);
        return state.Locations[(int)(index - _firstIndex)].Term;
    }

    /// <summary>
    /// The number of bytes the record of the entry at <paramref name="index"/> takes.
    /// </summary>
    public int GetRecordLength(ulong index) {
        var state = Published;
        CheckIndex(index, state);
        return RecordLength(state, (int)(index - _firstIndex));
    }

    /// <summary>
    /// Appends <paramref name="entries"/> with one write and one fsync; completes only after
    /// the fsync.
    /// </summary>
    public async ValueTask AppendAsync(ReadOnlyMemory<Entry> entries) {
        if (_fault is { } fault)
            throw fault;

        if (entries.Length == 0)
            return;

        CheckBatch(entries.Span);

        long total = 0;
        foreach (var entry in entries.Span)
            total += RecordFormat.GetLength(entry.Payload.Length);

        if (total > Array.MaxLength)
            throw new ArgumentException("The batch is too long.", nameof(entries));

        var buffer = ArrayPool<byte>.Shared.Rent((int)total);
        try {
            var offset = 0;
            for (var i = 0; i < entries.Length; i++)
                offset += RecordFormat.Write(entries.Span[i], buffer.AsSpan(offset), firstOfWrite: i == 0);

            var lengthBeforeBatch = _length;
            try {
                await _file.WriteAsync(buffer.AsMemory(0, (int)total), _length).ConfigureAwait(false);
                _file.FlushToDisk();
            } catch (Exception writeException) {
                CutTo(lengthBeforeBatch, writeException, "The segment could not cut back a failed write and refuses everything.");
                throw;
            }
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var position = _length;
        for (var i = 0; i < entries.Length; i++) {
            AddLocation(new RecordLocation(position, entries.Span[i].Term));
            position += RecordFormat.GetLength(entries.Span[i].Payload.Length);
        }

        _length = position;
        Publish();
    }

    /// <summary>
    /// Removes the entry at <paramref name="index"/> and every entry after it, and fsyncs
    /// the cut; when that fails, the segment is faulted.
    /// </summary>
    public void TruncateFrom(ulong index) {
        if (_fault is { } fault)
            throw fault;

        if (index < _firstIndex || index > LastIndex + 1)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within {_firstIndex} .. {LastIndex + 1}.");

        if (index == LastIndex + 1)
            return;

        var position = _locations[(int)(index - _firstIndex)].Position;
        CutTo(position, null, "The segment could not truncate and refuses everything.");
        _count = (int)(index - _firstIndex);
        _length = position;
        Publish();
    }

    /// <summary>
    /// Reads the entry at <paramref name="index"/> into <paramref name="buffer"/>; the
    /// payload of the returned entry points into it.
    /// </summary>
    public async ValueTask<Entry> ReadAsync(ulong index, Memory<byte> buffer, CancellationToken cancellationToken = default) {
        if (_fault is { } fault)
            throw fault;

        var state = Published;
        CheckIndex(index, state);

        var i = (int)(index - _firstIndex);
        var recordLength = RecordLength(state, i);
        if (buffer.Length < recordLength)
            throw new ArgumentException($"The buffer must hold at least {recordLength} bytes.", nameof(buffer));

        var location = state.Locations[i];
        var read = 0;
        while (read < recordLength) {
            var readNow = await _file.ReadAsync(buffer.Slice(read, recordLength - read), location.Position + read, cancellationToken).ConfigureAwait(false);
            if (readNow == 0)
                throw new InvalidDataException($"The segment '{_file.Path}' ends before the record of entry {index} at position {location.Position}.");

            read += readNow;
        }

        var status = RecordFormat.TryRead(buffer.Span.Slice(0, recordLength), out var header, out var length);
        if (status != RecordStatus.Complete || length != recordLength || header.Index != index || header.Term != location.Term)
            throw new InvalidDataException($"The record of entry {index} at position {location.Position} in the segment '{_file.Path}' is damaged.");

        return new Entry(header.Index, header.Term, header.Kind, header.Flags, buffer.Slice(RecordFormat.HeaderLength, header.PayloadLength));
    }

    /// <summary>
    /// Reads the entries from <paramref name="firstIndex"/> up to at most
    /// <paramref name="lastIndex"/>, as many whole records as fit in the buffer and
    /// the entry array, with one read; the payloads point into the buffer. Every
    /// record is checked as in the single read.
    /// </summary>
    /// <returns>The number of entries read; at least 1, the buffer must hold the first record.</returns>
    public async ValueTask<int> ReadAsync(ulong firstIndex, ulong lastIndex, Memory<byte> buffer, Memory<Entry> entries, CancellationToken cancellationToken = default) {
        if (_fault is { } fault)
            throw fault;

        var state = Published;
        if (entries.Length == 0)
            throw new ArgumentException("The entry array must hold at least one entry.", nameof(entries));
        if (firstIndex < _firstIndex || firstIndex >= _firstIndex + (ulong)state.Count)
            throw new ArgumentOutOfRangeException(nameof(firstIndex), firstIndex, $"The index must be within {_firstIndex} .. {LastIndex}.");
        if (lastIndex < firstIndex)
            throw new ArgumentOutOfRangeException(nameof(lastIndex), lastIndex, $"The last index must be at or above the first one ({firstIndex}).");

        // The last entry this call may read: the given one, or the segment's last.
        var last = Math.Min(lastIndex, _firstIndex + (ulong)state.Count - 1);
        var locations = state.Locations;
        var start = locations[firstIndex - _firstIndex].Position;

        // The buffer must hold the first record.
        var firstRecordLength = RecordLength(state, (int)(firstIndex - _firstIndex));
        if (buffer.Length < firstRecordLength)
            throw new ArgumentException($"The buffer must hold at least {firstRecordLength} bytes.", nameof(buffer));

        // How many whole records fit in the buffer and the entry array.
        var position = start;
        var count = 0;
        while (count < entries.Length && firstIndex + (ulong)count <= last) {
            var recordLength = RecordLength(state, (int)(firstIndex + (ulong)count - _firstIndex));
            if (position + recordLength > start + buffer.Length)
                break;

            position += recordLength;
            count++;
        }

        // One read for the whole batch.
        var total = (int)(position - start);
        var read = 0;
        while (read < total) {
            var readNow = await _file.ReadAsync(buffer.Slice(read, total - read), start + read, cancellationToken).ConfigureAwait(false);
            if (readNow == 0)
                throw new InvalidDataException($"The segment '{_file.Path}' ends before the records of the entries {firstIndex} .. {firstIndex + (ulong)count - 1} at position {start}.");

            read += readNow;
        }

        // Every record is checked as in the single read; the payloads point into the buffer.
        var offset = 0;
        for (var i = 0; i < count; i++) {
            var index = firstIndex + (ulong)i;
            var location = locations[index - _firstIndex];
            var recordLength = RecordLength(state, (int)(index - _firstIndex));
            var status = RecordFormat.TryRead(buffer.Span.Slice(offset, recordLength), out var header, out var length);
            if (status != RecordStatus.Complete || length != recordLength || header.Index != index || header.Term != location.Term)
                throw new InvalidDataException($"The record of entry {index} at position {location.Position} in the segment '{_file.Path}' is damaged.");

            entries.Span[i] = new Entry(header.Index, header.Term, header.Kind, header.Flags, buffer.Slice(offset + RecordFormat.HeaderLength, header.PayloadLength));
            offset += recordLength;
        }

        return count;
    }

    /// <inheritdoc/>
    public void Dispose() => _file.Dispose();

    /// <summary>
    /// Recovers the segment: reads the file from the start, rebuilds the in-memory
    /// array of (position, term) per entry, and cuts off a torn tail at the first
    /// record that is short, damaged or not where it belongs, with everything after
    /// it. A bad record with the start of a later write behind it was already on
    /// disk: that is damage, the segment refuses to open and nothing is cut.
    /// </summary>
    async ValueTask RecoverAsync(CancellationToken cancellationToken) {
        var fileLength = _file.Length;
        if (fileLength == 0)
            return;

        var buffer = ArrayPool<byte>.Shared.Rent(RecoveryBufferSize);
        try {
            var position = 0L;
            var windowStart = 0L;
            var filled = 0;
            var expectedIndex = _firstIndex;
            var previousTerm = 0ul;
            var stopped = false;

            while (position < fileLength) {
                var available = filled - (int)(position - windowStart);
                var status = RecordFormat.TryRead(buffer.AsSpan((int)(position - windowStart), available), out var header, out var length);

                if (status == RecordStatus.Complete && header.Index == expectedIndex && header.Term >= previousTerm) {
                    AddLocation(new RecordLocation(position, header.Term));
                    _length = position + length;
                    previousTerm = header.Term;
                    expectedIndex++;
                    position += length;
                    continue;
                }

                // A whole record that is not where it belongs, or a damaged one.
                if (status == RecordStatus.Complete || status == RecordStatus.Damaged) {
                    stopped = true;
                    break;
                }

                // Incomplete: a torn tail when the file holds no more bytes for the
                // record, or when its length field points past the end of the file.
                if (windowStart + filled >= fileLength) {
                    stopped = true;
                    break;
                }

                if (RecordFormat.TryReadDeclaredLength(buffer.AsSpan((int)(position - windowStart), available), out var declaredLength)
                    && declaredLength > fileLength - position) {
                    stopped = true;
                    break;
                }

                // Read more for the record at position: move its bytes to the front
                // of the buffer and read behind them, growing the buffer when the
                // record does not fit in it.
                var keep = available;
                if (keep > 0 && position > windowStart)
                    Array.Copy(buffer, position - windowStart, buffer, 0, keep);
                windowStart = position;
                filled = keep;

                if (filled == buffer.Length) {
                    var bigger = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                    Array.Copy(buffer, bigger, filled);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = bigger;
                }

                while (filled < buffer.Length && windowStart + filled < fileLength) {
                    var read = await _file.ReadAsync(buffer.AsMemory(filled), windowStart + filled, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                        break;

                    filled += read;
                }
            }

            if (stopped) {
                // A sealed segment was complete before it was sealed: any change is
                // damage, refused without the search for a later write and without a
                // cut.
                if (_isSealed)
                    throw new InvalidDataException($"The sealed segment '{_file.Path}' is damaged at position {position}: the record of entry {expectedIndex} is not complete.");

                // Look behind the bad record for the first record of a later
                // write: proof that the bad record was already on disk. That is
                // damage: the segment refuses to open, and nothing is cut.
                var (found, bufferAfterSearch) = await SearchForLaterWriteAsync(position + 1, fileLength, expectedIndex, buffer, cancellationToken).ConfigureAwait(false);
                buffer = bufferAfterSearch;

                if (found)
                    throw new InvalidDataException($"The segment '{_file.Path}' is damaged at position {position}: the record of entry {expectedIndex} is followed by the start of a later write.");

                // A torn tail: cut it off, with everything after it.
                _file.SetLength(position);
                _file.FlushToDisk();
            }
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        Publish();
    }

    /// <summary>
    /// Looks from <paramref name="from"/>, one byte past the bad record the recovery
    /// stopped at, to the end of the file for a complete record that is marked as
    /// the first record of a write and has an index above
    /// <paramref name="expectedIndex"/>: proof that the bad record was already on
    /// disk, so that it is damage and not a torn tail. The search goes byte by byte,
    /// because the length field of the bad record cannot be trusted; a cheap filter
    /// on the fixed bytes of a record comes before the check number.
    /// </summary>
    /// <returns>Whether such a record was found, and the buffer the search ended
    /// with, which can be larger than the one it started with.</returns>
    async ValueTask<(bool Found, byte[] Buffer)> SearchForLaterWriteAsync(long from, long fileLength, ulong expectedIndex, byte[] buffer, CancellationToken cancellationToken) {
        var windowStart = from;
        var filled = 0;
        var position = from;

        // A complete record takes at least the header, so no candidate starts in
        // the last bytes of the file.
        while (position + RecordFormat.HeaderLength <= fileLength) {
            var available = filled - (int)(position - windowStart);

            if (available >= RecordFormat.HeaderLength) {
                var span = buffer.AsSpan((int)(position - windowStart), available);

                // The cheap filter first; it also makes sure the record fits in the
                // file, so reading more for it always ends.
                if (!RecordFormat.MayBeFirstOfWrite(span, fileLength - position)) {
                    position++;
                    continue;
                }

                var status = RecordFormat.TryRead(span, out var header, out _);
                if (status == RecordStatus.Complete && header.IsFirstOfWrite && header.Index > expectedIndex)
                    return (true, buffer);

                // Incomplete: the record fits in the file but not yet in the buffer.
                if (status != RecordStatus.Incomplete) {
                    position++;
                    continue;
                }
            }

            // Read more for the candidate at position: move its bytes to the front
            // of the buffer and read behind them, growing the buffer when the
            // record does not fit in it.
            var keep = available;
            if (keep > 0 && position > windowStart)
                Array.Copy(buffer, position - windowStart, buffer, 0, keep);
            windowStart = position;
            filled = keep;

            if (filled == buffer.Length) {
                var bigger = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                Array.Copy(buffer, bigger, filled);
                ArrayPool<byte>.Shared.Return(buffer);
                buffer = bigger;
            }

            while (filled < buffer.Length && windowStart + filled < fileLength) {
                var read = await _file.ReadAsync(buffer.AsMemory(filled), windowStart + filled, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    break;

                filled += read;
            }
        }

        return (false, buffer);
    }

    /// <summary>
    /// Throws when <paramref name="index"/> lies outside the entries of the segment.
    /// </summary>
    void CheckIndex(ulong index, SegmentState state) {
        if (index < _firstIndex || index >= _firstIndex + (ulong)state.Count)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within {_firstIndex} .. {LastIndex}.");
    }

    /// <summary>
    /// The number of bytes the record of the entry at <paramref name="i"/> in
    /// <paramref name="state"/> takes: up to the next location, or the length of the
    /// file for the last entry of the segment.
    /// </summary>
    static int RecordLength(SegmentState state, int i) {
        var end = i + 1 < state.Count ? state.Locations[i + 1].Position : state.Length;
        return (int)(end - state.Locations[i].Position);
    }

    /// <summary>
    /// Checks that the batch follows on the segment: the first index is one past the last
    /// entry, every next one is one higher, and the terms never go down. A violation is
    /// a caller bug, not a fault of the segment.
    /// </summary>
    void CheckBatch(ReadOnlySpan<Entry> entries) {
        var expectedIndex = LastIndex + 1;
        var previousTerm = _count == 0 ? 0ul : _locations[_count - 1].Term;

        for (var i = 0; i < entries.Length; i++) {
            ref readonly var entry = ref entries[i];
            if (entry.Index != expectedIndex + (ulong)i)
                throw new ArgumentException($"The batch does not follow on: entry {i} has index {entry.Index}, expected {expectedIndex + (ulong)i}.", nameof(entries));

            if (entry.Term < previousTerm)
                throw new ArgumentException($"The terms of a batch never go down: entry {entry.Index} has term {entry.Term}, below {previousTerm}.", nameof(entries));

            previousTerm = entry.Term;
        }
    }

    /// <summary>
    /// Adds one location to the in-memory array, doubling it when it is full.
    /// </summary>
    void AddLocation(RecordLocation location) {
        if (_count == _locations.Length) {
            var bigger = new RecordLocation[_locations.Length * 2];
            Array.Copy(_locations, bigger, _count);
            _locations = bigger;
        }

        _locations[_count++] = location;
    }

    /// <summary>
    /// Cuts the file to <paramref name="position"/> and fsyncs the cut: without that
    /// fsync a crash right after could bring back bytes that were meant to go, and
    /// recovery would take them as entries. When the cut itself fails, the segment
    /// is faulted and refuses everything from now on: the entries that should be
    /// gone may still be on disk and come back after a restart.
    /// <paramref name="cause"/> is the failure the cut answers, when there is one.
    /// </summary>
    void CutTo(long position, Exception? cause, string failure) {
        try {
            _file.SetLength(position);
            _file.FlushToDisk();
        } catch (Exception cutException) {
            _fault = new IOException(failure, cause is null ? cutException : new AggregateException(cause, cutException));
            throw _fault;
        }
    }
}
