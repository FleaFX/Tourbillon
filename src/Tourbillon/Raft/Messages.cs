namespace Tourbillon.Raft;

/// <summary>
/// The question of a candidate to a voter: may this replica vote for me in this term?
/// </summary>
/// <param name="Term">The term the candidate would use; for a PreVote its current term + 1.</param>
/// <param name="LastIndex">The index of the last entry of the candidate's log.</param>
/// <param name="LastTerm">The term of that entry.</param>
/// <param name="PreVote">Whether this is the trial question, which raises and stores nothing.</param>
readonly record struct VoteRequest(ulong Term, ulong LastIndex, ulong LastTerm, bool PreVote);

/// <summary>
/// The answer of a voter to a candidate.
/// </summary>
/// <param name="Term">The term of the voter; higher than the asked one on a refusal.</param>
/// <param name="Granted">Whether the voter votes for the candidate.</param>
/// <param name="PreVote">Whether this answers a trial question.</param>
readonly record struct VoteResponse(ulong Term, bool Granted, bool PreVote);

/// <summary>
/// The append request of a leader to a follower: the entries after a place the follower
/// is asked to match, and how far the leader has committed.
/// </summary>
/// <param name="Term">The term of the leader.</param>
/// <param name="PrevIndex">The index of the entry right before the entries; 0 for none.</param>
/// <param name="PrevTerm">The term of that entry; 0 when there is none.</param>
/// <param name="Commit">The commit index of the leader.</param>
/// <param name="Entries">The entries to append; empty for a heartbeat.</param>
readonly record struct AppendRequest(ulong Term, ulong PrevIndex, ulong PrevTerm, ulong Commit, ReadOnlyMemory<Entry> Entries);

/// <summary>
/// The answer of a follower to an append request.
/// </summary>
/// <param name="Term">The term of the follower; higher than the leader's on a refusal.</param>
/// <param name="Result">What the follower did with the entries.</param>
/// <param name="Index">For <see cref="AppendResult.Stored"/> and <see cref="AppendResult.Full"/> the last index the follower now matches; for <see cref="AppendResult.Mismatch"/> the conflict index.</param>
/// <param name="ConflictTerm">For <see cref="AppendResult.Mismatch"/> the term of the conflicting entry; 0 when there is none.</param>
readonly record struct AppendResponse(ulong Term, AppendResult Result, ulong Index, ulong ConflictTerm);

/// <summary>
/// What a follower did with the entries of an append request.
/// </summary>
enum AppendResult : byte {
    /// <summary>
    /// The follower stored every entry of the request.
    /// </summary>
    Stored = 0,

    /// <summary>
    /// The follower's log conflicts with the request before the entries.
    /// </summary>
    Mismatch = 1,

    /// <summary>
    /// The follower stored only part of the entries; its storage is full.
    /// </summary>
    Full = 2,
}
