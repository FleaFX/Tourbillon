namespace Tourbillon.Raft;

/// <summary>
/// The log of one copy, as the replica uses it; the members are those of
/// <see cref="Log.SegmentedLog"/>, so that the segmented log takes this role in the
/// next step without an adapter.
/// </summary>
interface IRaftLog {
    /// <summary>
    /// The index of the first entry in the log.
    /// </summary>
    ulong FirstIndex { get; }

    /// <summary>
    /// The index of the last entry in the log; 0 when the log is empty.
    /// </summary>
    ulong LastIndex { get; }

    /// <summary>
    /// The term of the entry at <paramref name="index"/>; 0 for the place before the
    /// first entry.
    /// </summary>
    ulong GetTerm(ulong index);

    /// <summary>
    /// The number of bytes the record of the entry at <paramref name="index"/> takes.
    /// </summary>
    int GetRecordLength(ulong index);

    /// <summary>
    /// Appends the longest start of <paramref name="entries"/> that fits in the storage
    /// limit; the entries after it are refused because storage is full and touch
    /// nothing.
    /// </summary>
    /// <returns>The number of entries appended; 0 when the first one does not fit.</returns>
    ValueTask<int> AppendAsync(ReadOnlyMemory<Entry> entries);

    /// <summary>
    /// Removes the entry at <paramref name="index"/> and every entry after it.
    /// </summary>
    void TruncateFrom(ulong index);

    /// <summary>
    /// Reads the entry at <paramref name="index"/> into <paramref name="buffer"/>; the
    /// payload of the returned entry points into it.
    /// </summary>
    ValueTask<Entry> ReadAsync(ulong index, Memory<byte> buffer, CancellationToken cancellationToken = default);
}
