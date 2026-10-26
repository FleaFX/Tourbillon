namespace Tourbillon.Raft;

// Counts the saves and runs a probe at every save, so a test can prove what had or had
// not happened at the moment the replica stored its term and vote.
sealed class SequencedVoteStore : IVoteStore {
    readonly Tourbillon.Testing.InMemoryVoteStore _inner = new();
    readonly Func<bool> _probe;

    public SequencedVoteStore(Func<bool> probe) {
        _probe = probe;
    }

    public int Saves { get; private set; }

    public int ProbeHits { get; private set; }

    public ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default) =>
        _inner.LoadAsync(cancellationToken);

    public async ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default) {
        Saves++;
        if (_probe())
            ProbeHits++;

        await _inner.SaveAsync(term, votedFor, cancellationToken);
    }
}
