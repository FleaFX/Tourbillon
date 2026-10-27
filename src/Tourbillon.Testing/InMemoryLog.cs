using Tourbillon.Log;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// A log in memory, on the real <see cref="StorageLimit"/> of the segmented log, so the
/// same rules decide what fits. Payloads are copied on append; the log counts its
/// appends and can hold one append at a gate, for tests. The failure switches throw on
/// the next append: <see cref="FailNextAppend"/> once, <see cref="FaultNextAppend"/> once
/// and from then on; a faulted log refuses every write and can still be read.
/// </summary>
sealed class InMemoryLog : IRaftLog {
    readonly List<Entry> _entries = new();
    readonly StorageLimit _storage;
    long _freeSpace = long.MaxValue;
    int _appendCount;
    IOException? _fault;

    /// <summary>
    /// An empty log on unlimited storage, with no reserve.
    /// </summary>
    public InMemoryLog() : this(null, 0) {
    }

    /// <summary>
    /// An empty log with a maximum size and a reserve; the free space starts unlimited.
    /// </summary>
    public InMemoryLog(long? maximumSize, long reserveSize) {
        _storage = new StorageLimit(() => _freeSpace, maximumSize, reserveSize);
    }

    /// <summary>
    /// The free space the storage limit measures; the cluster sets it to fill a replica or
    /// to free it again.
    /// </summary>
    public long FreeSpace {
        set => _freeSpace = value;
    }

    /// <summary>
    /// The number of appends this log has taken.
    /// </summary>
    public int AppendCount => _appendCount;

    /// <summary>
    /// While set, every append waits for it first: a test holds the append of the log
    /// until it opens the gate.
    /// </summary>
    public TaskCompletionSource? AppendGate { get; set; }

    /// <summary>
    /// While set, the next append throws once, without faulting the log: the write was
    /// cut back, the entries are in no log.
    /// </summary>
    public bool FailNextAppend { get; set; }

    /// <summary>
    /// While set, the next append throws and faults the log: from then on every write
    /// throws, and the log can still be read.
    /// </summary>
    public bool FaultNextAppend { get; set; }

    /// <inheritdoc/>
    public ulong FirstIndex => 1;

    /// <inheritdoc/>
    public ulong LastIndex => (ulong)_entries.Count;

    /// <inheritdoc/>
    public bool IsFaulted => _fault is not null;

    /// <inheritdoc/>
    public ulong GetTerm(ulong index) {
        if (index == 0)
            return 0;

        if (index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within 1 .. {LastIndex}.");

        return _entries[(int)index - 1].Term;
    }

    /// <inheritdoc/>
    public int GetRecordLength(ulong index) {
        if (index == 0 || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within 1 .. {LastIndex}.");

        return RecordFormat.GetLength(_entries[(int)index - 1].Payload.Length);
    }

    /// <inheritdoc/>
    public async ValueTask<int> AppendAsync(ReadOnlyMemory<Entry> entries) {
        if (_fault is { } fault)
            throw fault;

        if (AppendGate is { } gate)
            await gate.Task.ConfigureAwait(false);

        _appendCount++;

        if (entries.Length == 0)
            return 0;

        if (FaultNextAppend) {
            FaultNextAppend = false;
            _fault = new IOException("The in-memory log is faulted and refuses every write.");
            throw _fault;
        }

        if (FailNextAppend) {
            FailNextAppend = false;
            throw new IOException("The in-memory log refused the append.");
        }

        var count = _storage.TryTake(entries.Span, out var bytes);
        if (count == 0)
            return 0;

        var written = false;
        try {
            for (var i = 0; i < count; i++) {
                var entry = entries.Span[i];
                _entries.Add(new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray()));
            }

            written = true;
            return count;
        } finally {
            _storage.Settle(bytes, written);
        }
    }

    /// <inheritdoc/>
    public void TruncateFrom(ulong index) {
        if (_fault is { } fault)
            throw fault;

        if (index < FirstIndex || index > LastIndex + 1)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within {FirstIndex} .. {LastIndex + 1}.");

        if (index == LastIndex + 1)
            return;

        var released = 0L;
        for (var i = (int)index - 1; i < _entries.Count; i++)
            released += RecordFormat.GetLength(_entries[i].Payload.Length);

        _entries.RemoveRange((int)index - 1, _entries.Count - (int)index + 1);
        _storage.Release(released);
    }

    /// <inheritdoc/>
    public ValueTask<Entry> ReadAsync(ulong index, Memory<byte> buffer, CancellationToken cancellationToken = default) {
        if (index == 0 || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), index, $"The index must be within 1 .. {LastIndex}.");

        var entry = _entries[(int)index - 1];
        var recordLength = RecordFormat.GetLength(entry.Payload.Length);
        if (buffer.Length < recordLength)
            throw new ArgumentException($"The buffer must hold at least {recordLength} bytes.", nameof(buffer));

        RecordFormat.Write(entry, buffer.Span);
        return new ValueTask<Entry>(new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, buffer.Slice(RecordFormat.HeaderLength, entry.Payload.Length)));
    }

    /// <inheritdoc/>
    public ValueTask<int> ReadAsync(ulong firstIndex, ulong lastIndex, Memory<byte> buffer, Memory<Entry> entries, CancellationToken cancellationToken = default) {
        if (firstIndex < FirstIndex || firstIndex > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(firstIndex), firstIndex, $"The index must be within {FirstIndex} .. {LastIndex}.");
        if (lastIndex < firstIndex)
            throw new ArgumentOutOfRangeException(nameof(lastIndex), lastIndex, $"The last index must be at or above the first one ({firstIndex}).");
        if (entries.Length == 0)
            throw new ArgumentException("The entry array must hold at least one entry.", nameof(entries));

        // As many whole records as fit in the buffer and the entry array, up to the
        // last index; the payloads point into the buffer.
        var count = 0;
        var offset = 0;
        while (count < entries.Length && firstIndex + (ulong)count <= lastIndex && firstIndex + (ulong)count <= LastIndex) {
            var entry = _entries[(int)(firstIndex + (ulong)count) - 1];
            var recordLength = RecordFormat.GetLength(entry.Payload.Length);
            if (offset + recordLength > buffer.Length)
                break;

            RecordFormat.Write(entry, buffer.Span.Slice(offset));
            entries.Span[count] = new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, buffer.Slice(offset + RecordFormat.HeaderLength, entry.Payload.Length));
            offset += recordLength;
            count++;
        }

        if (count == 0) {
            var entry = _entries[(int)firstIndex - 1];
            throw new ArgumentException($"The buffer must hold at least {RecordFormat.GetLength(entry.Payload.Length)} bytes.", nameof(buffer));
        }

        return new ValueTask<int>(count);
    }
}
