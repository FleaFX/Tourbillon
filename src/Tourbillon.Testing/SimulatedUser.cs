using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// The user of one copy: after every pass of turns it reads what the feed of its copy
/// hands out, checks every batch starts right after the one before, without gaps, and
/// copies the entries it keeps. It stores its state — D, the last index it applied —
/// at seeded moments of applying; a crash loses what it applied after its last store,
/// and the restart of its copy opens the group with that D.
/// </summary>
sealed class SimulatedUser {
    readonly string _machine;
    readonly Random _random;
    readonly List<Entry> _applied = new();
    ulong _delivered;
    ulong _stored;

    /// <summary>
    /// The user of one copy, on <paramref name="random"/> for the moments it stores
    /// its state.
    /// </summary>
    public SimulatedUser(string machine, Random random) {
        _machine = machine;
        _random = random;
    }

    /// <summary>
    /// The index a restart of this copy opens its group with: the last index this
    /// user stored.
    /// </summary>
    public ulong Stored => _stored;

    /// <summary>
    /// The entries this user applied, copied: what the invariant checker sees.
    /// </summary>
    public IReadOnlyList<Entry> Applied => _applied;

    /// <summary>
    /// Stores the state now: D is the last index this user applied. The moments of the
    /// simulation are seeded; a test stores when it wants a known D.
    /// </summary>
    public void Store() =>
        _stored = _delivered;

    /// <summary>
    /// Reads what the feed of the running <paramref name="replica"/> hands out: while
    /// the commit index is past what the feed delivered, one batch at a time, each
    /// starting right after the one before, and every entry copied before the next
    /// read. A batch that starts elsewhere or has a gap throws
    /// <see cref="InvariantViolationException"/>: the feed must hand every accepted
    /// entry, in order, without gaps, also after a restart.
    /// </summary>
    public void Read(Replica replica) {
        var feed = replica.Feed ?? throw new InvalidOperationException($"The copy '{_machine}' runs without a feed.");
        var applied = false;
        while (replica.CommitIndex > feed.Delivered) {
            var expected = feed.Delivered + 1;
            var read = feed.ReadAsync(CancellationToken.None);
            if (!read.IsCompletedSuccessfully)
                throw new InvalidOperationException($"The feed of '{_machine}' did not read synchronously; a step of the cluster must be repeatable.");

            var batch = read.Result;
            if (batch.Length == 0)
                throw new InvariantViolationException($"The feed of '{_machine}' handed an empty batch while its commit index is past {feed.Delivered}.");

            if (batch.Span[0].Index != expected)
                throw new InvariantViolationException($"The feed of '{_machine}' starts a batch at index {batch.Span[0].Index}, right after {expected - 1} was delivered: it skipped an index.");

            for (var i = 1; i < batch.Length; i++)
                if (batch.Span[i].Index != expected + (ulong)i)
                    throw new InvariantViolationException($"The feed of '{_machine}' hands a batch with a gap: entry {batch.Span[i].Index} follows {batch.Span[i - 1].Index}.");

            foreach (var entry in batch.Span)
                _applied.Add(new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray()));

            _delivered = batch.Span[^1].Index;
            applied = true;
        }

        // The state is stored at a seeded moment of applying: D is the last index
        // applied. A crash loses what was applied after the last store.
        if (applied && _random.Next(4) == 0)
            _stored = _delivered;
    }

    /// <summary>
    /// The copy crashed: what this user applied after its last store is lost, and its
    /// list of applied entries ends at the stored D.
    /// </summary>
    public void Crash() {
        _delivered = _stored;
        if (_applied.Count > (int)_stored)
            _applied.RemoveRange((int)_stored, _applied.Count - (int)_stored);
    }
}
