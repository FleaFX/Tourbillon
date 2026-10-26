namespace Tourbillon.Log;

/// <summary>
/// The published state of a segment: the locations of its entries, how many there
/// are and the length of the file. Immutable; the segment replaces it after every
/// change, so a reader that takes one reference sees a consistent state, and a
/// locations array grown for a later state is a copy that still holds the entries
/// of an earlier one.
/// </summary>
sealed class SegmentState {
    /// <summary>
    /// The published state of a segment with <paramref name="count"/> entries in
    /// <paramref name="locations"/>, whose file holds <paramref name="length"/> bytes.
    /// </summary>
    public SegmentState(RecordLocation[] locations, int count, long length) {
        Locations = locations;
        Count = count;
        Length = length;
    }

    /// <summary>
    /// The locations of the entries, one per entry.
    /// </summary>
    public RecordLocation[] Locations { get; }

    /// <summary>
    /// The number of entries.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// The number of bytes of the segment's records on disk.
    /// </summary>
    public long Length { get; }
}
