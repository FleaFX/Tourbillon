using System.Buffers;
using System.Runtime.CompilerServices;

namespace Tourbillon.Raft;

/// <summary>
/// The accepted entries of one copy, in log order, from the index after the one the
/// user named when it opened the group. A batch is lent: it points into the feed's
/// buffer and is valid until the next read.
/// </summary>
sealed class Feed : IAsyncDisposable {
    /// <summary>
    /// The size of the buffer one batch is read into.
    /// </summary>
    const int BufferSize = 1024 * 1024;

    /// <summary>
    /// The number of entries one batch holds at most.
    /// </summary>
    const int MaxEntries = 4096;

    readonly Replica _replica;
    readonly IRaftLog _log;
    readonly bool _poison;
    ulong _delivered;
    byte[]? _buffer;
    Entry[]? _entries;

    /// <summary>
    /// The feed of <paramref name="replica"/>, read from <paramref name="log"/>, that
    /// hands the entries after <paramref name="delivered"/>; with
    /// <paramref name="poison"/> it poisons every batch it takes back.
    /// </summary>
    public Feed(Replica replica, IRaftLog log, ulong delivered, bool poison) {
        _replica = replica;
        _log = log;
        _delivered = delivered;
        _poison = poison;
    }

    /// <summary>
    /// The last index handed out.
    /// </summary>
    public ulong Delivered => _delivered;

    /// <summary>
    /// Waits until the commit index moves past <see cref="Delivered"/>: true when there
    /// is something to read, false when the replica stopped; a broken replica throws
    /// its fault.
    /// </summary>
    public async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) {
        while (true) {
            if (_replica.CommitIndex > _delivered)
                return true;
            if (_replica.Fault is { } fault)
                throw fault;
            if (_replica.Stopped)
                return false;

            var wakeup = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _replica.RegisterFeedReader(wakeup);
            try {
                // Re-checked after registering: a commit, a stop or a fault between the
                // checks above and the registration is seen here, or woke the reader.
                if (_replica.CommitIndex > _delivered)
                    return true;
                if (_replica.Fault is { } broken)
                    throw broken;
                if (_replica.Stopped)
                    return false;

                if (!await wakeup.Task.WaitAsync(cancellationToken).ConfigureAwait(false))
                    return false;  // the replica stopped
            } finally {
                _replica.UnregisterFeedReader(wakeup);
            }
        }
    }

    /// <summary>
    /// Reads the next batch: the entries from <see cref="Delivered"/> + 1 up to the
    /// commit index, as many whole records as fit in the buffer and the entry array.
    /// Empty when nothing new is committed. The batch is lent: it is valid until the
    /// next read, which first takes the memory back — poisoned with rubbish when the
    /// replica's options say so.
    /// </summary>
    public async ValueTask<ReadOnlyMemory<Entry>> ReadAsync(CancellationToken cancellationToken = default) {
        GiveBack();

        var commit = _replica.CommitIndex;
        if (commit <= _delivered)
            return ReadOnlyMemory<Entry>.Empty;

        // One pooled buffer and one pooled entry array for the batch; a record larger
        // than the buffer gets a larger one, and the batch stops at the sizes asked
        // for, not at the sizes the pool rounded up to.
        var first = _delivered + 1;
        var size = Math.Max(BufferSize, _log.GetRecordLength(first));
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        var entries = ArrayPool<Entry>.Shared.Rent(MaxEntries);
        try {
            var count = await _log.ReadAsync(first, commit, buffer.AsMemory(0, size), entries.AsMemory(0, MaxEntries), cancellationToken).ConfigureAwait(false);
            _delivered += (ulong)count;
            _buffer = buffer;
            _entries = entries;
            return entries.AsMemory(0, count);
        } catch {
            ArrayPool<byte>.Shared.Return(buffer);
            ArrayPool<Entry>.Shared.Return(entries);
            throw;
        }
    }

    /// <summary>
    /// Hands the entries from <see cref="Delivered"/> + 1 on, until the replica stops
    /// or breaks; a broken replica throws its fault.
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<Entry>> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default) {
        while (await WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            yield return await ReadAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() {
        GiveBack();
        return default;
    }

    /// <summary>
    /// Takes the memory of the current batch back: poisoned with rubbish when the
    /// replica's options say so, then returned to the pool.
    /// </summary>
    void GiveBack() {
        if (_buffer is null)
            return;

        var buffer = _buffer;
        var entries = _entries!;
        _buffer = null;
        _entries = null;

        if (_poison) {
            Array.Fill(buffer, (byte)0xA5);
            Array.Clear(entries);
        }

        ArrayPool<byte>.Shared.Return(buffer);
        ArrayPool<Entry>.Shared.Return(entries);
    }
}
