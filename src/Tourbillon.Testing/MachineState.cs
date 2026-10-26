using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// The observable state of one machine at one check: what the invariant checker sees.
/// </summary>
/// <param name="Name">The name of the machine.</param>
/// <param name="Running">Whether its replica runs; the log of a crashed copy survives.</param>
/// <param name="Role">The role of its replica; meaningless when it does not run.</param>
/// <param name="Term">Its current term.</param>
/// <param name="VotedFor">The copy it voted for in that term, or null.</param>
/// <param name="CommitIndex">The highest index it knows is committed; 0 when it does not run.</param>
/// <param name="Log">Its log: the entries 1..last, payloads copied.</param>
sealed record MachineState(
    string Name,
    bool Running,
    ReplicaRole Role,
    ulong Term,
    string? VotedFor,
    ulong CommitIndex,
    IReadOnlyList<Entry> Log
);
