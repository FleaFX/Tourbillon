namespace Tourbillon.Log;

/// <summary>
/// Where one entry's record sits in its segment: the position of the record in the
/// file and the term of the entry.
/// </summary>
readonly record struct RecordLocation(long position, ulong term) {
    /// <summary>
    /// The position of the record in the segment file.
    /// </summary>
    public long Position { get; } = position;

    /// <summary>
    /// The term of the entry.
    /// </summary>
    public ulong Term { get; } = term;
}
