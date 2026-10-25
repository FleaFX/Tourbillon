namespace Tourbillon;

/// <summary>
/// What an entry is for. Everything the user proposes is a <see cref="Command"/>.
/// </summary>
enum EntryKind : byte {
    /// <summary>
    /// An entry the user proposed, with an opaque payload.
    /// </summary>
    Command = 1,

    /// <summary>
    /// The empty entry a new leader adds in its own term before it takes proposals.
    /// </summary>
    NoOp = 2,

    /// <summary>
    /// The full membership of the group from this entry on.
    /// </summary>
    Config = 3,
}
