namespace Tourbillon.Log;

/// <summary>
/// The fields of a decoded record; its payload is the <see cref="PayloadLength"/> bytes
/// at <see cref="RecordFormat.HeaderLength"/> from the start of the record.
/// </summary>
readonly struct RecordHeader(ulong index, ulong term, EntryKind kind, EntryFlags flags, bool isFirstOfWrite, int payloadLength) {
    /// <summary>
    /// The index of the entry in the record.
    /// </summary>
    public ulong Index { get; } = index;

    /// <summary>
    /// The term of the entry in the record.
    /// </summary>
    public ulong Term { get; } = term;

    /// <summary>
    /// The kind of the entry in the record.
    /// </summary>
    public EntryKind Kind { get; } = kind;

    /// <summary>
    /// The flags of the entry in the record.
    /// </summary>
    public EntryFlags Flags { get; } = flags;

    /// <summary>
    /// Whether the record is marked as the first record of a write. The mark
    /// belongs to the record, not to the entry.
    /// </summary>
    public bool IsFirstOfWrite { get; } = isFirstOfWrite;

    /// <summary>
    /// The number of payload bytes after the header.
    /// </summary>
    public int PayloadLength { get; } = payloadLength;
}
