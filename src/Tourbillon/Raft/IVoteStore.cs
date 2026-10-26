namespace Tourbillon.Raft;

/// <summary>
/// The term and vote of one copy; <see cref="SaveAsync"/> completes only once they are
/// on disk.
/// </summary>
interface IVoteStore {
    /// <summary>
    /// Reads the term and the vote of this copy.
    /// </summary>
    ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores the term and the vote of this copy.
    /// </summary>
    ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default);
}
