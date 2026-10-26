namespace Tourbillon.Testing;

/// <summary>
/// An invariant of the group was violated; the message says which one and carries a dump
/// of every machine.
/// </summary>
sealed class InvariantViolationException : Exception {
    /// <summary>
    /// A violation of an invariant, with its message.
    /// </summary>
    public InvariantViolationException(string message) : base(message) {
    }
}
