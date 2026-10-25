namespace Tourbillon;

/// <summary>
/// One entry of a log: its place, the term it was proposed in, its kind and flags, and
/// the user's opaque payload.
/// </summary>
readonly struct Entry(ulong index, ulong term, EntryKind kind, EntryFlags flags, ReadOnlyMemory<byte> payload) {
    /// <summary>
    /// The place of the entry in its log, starting at 1.
    /// </summary>
    public ulong Index { get; } = index;

    /// <summary>
    /// The term in which the entry was proposed, starting at 1.
    /// </summary>
    public ulong Term { get; } = term;

    /// <summary>
    /// What the entry is for.
    /// </summary>
    public EntryKind Kind { get; } = kind;

    /// <summary>
    /// The facts stored with the entry, such as whether it may use the reserve.
    /// </summary>
    public EntryFlags Flags { get; } = flags;

    /// <summary>
    /// The user's bytes; Tourbillon never reads them.
    /// </summary>
    public ReadOnlyMemory<byte> Payload { get; } = payload;
}
