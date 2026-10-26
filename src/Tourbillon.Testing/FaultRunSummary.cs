namespace Tourbillon.Testing;

/// <summary>
/// What one fault run came to: the outcomes of its proposals, the leaders it saw, the
/// terms it reached, and a hash of its trace.
/// </summary>
/// <param name="Accepted">How many proposals were accepted.</param>
/// <param name="Refused">How many proposals were refused.</param>
/// <param name="Unknown">How many proposals ended unknown.</param>
/// <param name="Leaders">The leaders it saw, in order: machine and term.</param>
/// <param name="Terms">The terms it saw, in order.</param>
/// <param name="TraceHash">A hash of the trace of the run.</param>
sealed record FaultRunSummary(
    int Accepted,
    int Refused,
    int Unknown,
    IReadOnlyList<(string Machine, ulong Term)> Leaders,
    IReadOnlyList<ulong> Terms,
    long TraceHash
);
