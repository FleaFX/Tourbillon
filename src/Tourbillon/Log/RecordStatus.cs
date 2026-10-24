namespace Tourbillon.Log;

/// <summary>
/// What the decoder found at the start of the input.
/// </summary>
internal enum RecordStatus {
    /// <summary>
    /// A whole record with a correct check number.
    /// </summary>
    Complete,

    /// <summary>
    /// The input ends before the record does.
    /// </summary>
    Incomplete,

    /// <summary>
    /// The record is wrong: a bad length, check number, version or field.
    /// </summary>
    Damaged,
}
