namespace Tourbillon;

/// <summary>
/// Facts about an entry that every replica must decide the same way, so they are stored
/// in it.
/// </summary>
[Flags]
enum EntryFlags : byte {
    /// <summary>
    /// No flags.
    /// </summary>
    None = 0,

    /// <summary>
    /// The entry may use the storage reserve, because it helps to free space.
    /// </summary>
    MayUseReserve = 1,
}
