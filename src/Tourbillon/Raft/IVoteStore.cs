namespace Tourbillon.Raft;

/// <summary>
/// The term and vote of one replica; <see cref="SaveAsync"/> completes only once they are
/// on disk.
/// </summary>
interface IVoteStore {
    /// <summary>
    /// Reads the term and the vote of this replica.
    /// </summary>
    ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores the term and the vote of this replica.
    /// </summary>
    ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default);
}
