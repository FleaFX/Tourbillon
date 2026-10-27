using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// The term and vote of one replica, in memory; the store survives a crash of the replica
/// that uses it.
/// </summary>
sealed class InMemoryVoteStore : IVoteStore {
    ulong _term;
    string? _votedFor;

    /// <inheritdoc/>
    public ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default) =>
        new((_term, _votedFor));

    /// <inheritdoc/>
    public ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default) {
        _term = term;
        _votedFor = votedFor;
        return default;
    }
}
