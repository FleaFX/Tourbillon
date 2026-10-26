using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// Checks the invariants of the group over the whole run so far: at most one leader per
/// term, a leader that never overwrites or removes entries of its own log, same index and
/// term meaning identical up to there, a committed entry in the log of every later
/// leader, committed prefixes that agree, and a refused proposal in no log. A violation
/// throws <see cref="InvariantViolationException"/> with a dump of every machine.
/// </summary>
sealed class InvariantChecker {
    // At most one leader per term, over the whole run so far.
    readonly Dictionary<ulong, string> _leaderByTerm = new();

    // Per machine, while it stays leader in one term: its log at the previous check.
    readonly Dictionary<string, LeaderLog> _leaderLogs = new();

    // Per index, the term and payload first seen committed there, by any replica, and
    // the term of that replica when it reported the commit.
    readonly SortedDictionary<ulong, CommittedEntry> _committedByIndex = new();

    // Per machine, how far its committed prefix was verified.
    readonly Dictionary<string, ulong> _verifiedCommit = new();

    // The payloads of the proposals that were refused: in no log, ever.
    readonly List<byte[]> _refused = new();

    /// <summary>
    /// Notes the payload of a proposal that was refused: it must never appear in any log.
    /// </summary>
    public void NoteRefusedProposal(byte[] payload) =>
        _refused.Add(payload);

    /// <summary>
    /// Runs every check over the state of every machine; call it after every step of a
    /// run.
    /// </summary>
    public void Assert(IReadOnlyList<MachineState> machines) {
        CheckOneLeaderPerTerm(machines);
        CheckLeaderLogs(machines);
        CheckLogsMatch(machines);
        CheckCommittedPrefixes(machines);
        CheckLaterLeaders(machines);
        CheckNoRefusedPayload(machines);
    }

    // At most one leader per term, over the whole run so far.
    void CheckOneLeaderPerTerm(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            if (!machine.Running || machine.Role != ReplicaRole.Leader)
                continue;

            if (_leaderByTerm.TryGetValue(machine.Term, out var leader)) {
                if (leader != machine.Name)
                    throw Violation($"Two leaders in term {machine.Term}: '{leader}' and '{machine.Name}'.", machines);
            } else {
                _leaderByTerm.Add(machine.Term, machine.Name);
            }
        }
    }

    // While a copy stays leader in one term, its log starts with its log at the previous
    // check: a leader never overwrites or removes entries of its own log.
    void CheckLeaderLogs(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            if (!machine.Running || machine.Role != ReplicaRole.Leader) {
                _leaderLogs.Remove(machine.Name);
                continue;
            }

            if (_leaderLogs.TryGetValue(machine.Name, out var previous) && previous.Term == machine.Term) {
                if (machine.Log.Count < previous.Entries.Count)
                    throw Violation($"Leader '{machine.Name}' lost entries of its own log in term {machine.Term}: it had {previous.Entries.Count} entries, now {machine.Log.Count}.", machines);

                for (var i = 0; i < previous.Entries.Count; i++) {
                    if (!SameEntry(previous.Entries[i], machine.Log[i]))
                        throw Violation($"Leader '{machine.Name}' overwrote its own entry at index {previous.Entries[i].Index} in term {machine.Term}.", machines);
                }
            }

            _leaderLogs[machine.Name] = new LeaderLog(machine.Term, machine.Log);
        }
    }

    // Same index and term means identical up to there: for every pair of logs, below the
    // highest index where both have the same term, same term and payload everywhere.
    void CheckLogsMatch(IReadOnlyList<MachineState> machines) {
        for (var i = 0; i < machines.Count; i++) {
            for (var j = i + 1; j < machines.Count; j++) {
                var first = machines[i];
                var second = machines[j];
                var shared = Math.Min(first.Log.Count, second.Log.Count);

                var highest = 0;
                for (var index = shared; index >= 1; index--) {
                    if (first.Log[index - 1].Term == second.Log[index - 1].Term) {
                        highest = index;
                        break;
                    }
                }

                for (var index = 1; index <= highest; index++) {
                    var one = first.Log[index - 1];
                    var other = second.Log[index - 1];
                    if (one.Term != other.Term || !one.Payload.Span.SequenceEqual(other.Payload.Span))
                        throw Violation($"The logs of '{first.Name}' and '{second.Name}' differ at index {index}, below their highest matching index {highest}.", machines);
                }
            }
        }
    }

    // Every replica applies the same entry at an index: the committed prefix of every
    // replica agrees, in term and payload.
    void CheckCommittedPrefixes(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            if (!machine.Running)
                _verifiedCommit[machine.Name] = 0;

            _verifiedCommit.TryGetValue(machine.Name, out var from);
            var upTo = Math.Min(machine.CommitIndex, (ulong)machine.Log.Count);
            for (var index = from + 1; index <= upTo; index++) {
                var entry = machine.Log[(int)index - 1];
                if (_committedByIndex.TryGetValue(index, out var committed)) {
                    if (committed.Term != entry.Term || !committed.Payload.AsSpan().SequenceEqual(entry.Payload.Span))
                        throw Violation($"'{machine.Name}' commits term {entry.Term} at index {index}, but term {committed.Term} was already committed there by another copy.", machines);
                } else {
                    _committedByIndex.Add(index, new CommittedEntry(entry.Term, entry.Payload.ToArray(), machine.Term));
                }
            }

            if (upTo > from)
                _verifiedCommit[machine.Name] = upTo;
        }
    }

    // A committed entry is in the log of every later leader. A copy learns a commit only
    // from a leader of its own term, so the term of the copy that first reported it is a
    // term whose leader has the entry: every leader of that term or later has it too. A
    // leader of an earlier term is a stale believer and is exempt.
    void CheckLaterLeaders(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            if (!machine.Running || machine.Role != ReplicaRole.Leader)
                continue;

            foreach (var (index, committed) in _committedByIndex) {
                if (machine.Term < committed.CommitTerm)
                    continue;

                if (index > (ulong)machine.Log.Count)
                    throw Violation($"Leader '{machine.Name}' of term {machine.Term} misses the committed entry at index {index}: its log ends at {machine.Log.Count}.", machines);

                var entry = machine.Log[(int)index - 1];
                if (entry.Term != committed.Term || !entry.Payload.Span.SequenceEqual(committed.Payload))
                    throw Violation($"Leader '{machine.Name}' has a different entry at the committed index {index}.", machines);
            }
        }
    }

    // A refused proposal is in no log.
    void CheckNoRefusedPayload(IReadOnlyList<MachineState> machines) {
        if (_refused.Count == 0)
            return;

        foreach (var machine in machines) {
            foreach (var entry in machine.Log) {
                foreach (var payload in _refused) {
                    if (entry.Payload.Span.SequenceEqual(payload))
                        throw Violation($"The refused payload is in the log of '{machine.Name}' at index {entry.Index}.", machines);
                }
            }
        }
    }

    // Whether two entries are the same entry.
    static bool SameEntry(Entry first, Entry second) =>
        first.Index == second.Index
        && first.Term == second.Term
        && first.Kind == second.Kind
        && first.Flags == second.Flags
        && first.Payload.Span.SequenceEqual(second.Payload.Span);

    InvariantViolationException Violation(string message, IReadOnlyList<MachineState> machines) =>
        new($"{message}{Dump(machines)}");

    // A dump of every machine: role, term, vote, commit, log as index:term.
    static string Dump(IReadOnlyList<MachineState> machines) {
        var lines = new List<string> { "", "--- the cluster at the moment of the violation ---" };
        foreach (var machine in machines) {
            if (!machine.Running) {
                lines.Add($"  {machine.Name}: CRASHED, log=[{Terms(machine.Log)}]");
                continue;
            }

            var votedFor = machine.VotedFor ?? "null";
            lines.Add($"  {machine.Name}: role={machine.Role}, term={machine.Term}, votedFor={votedFor}, commit={machine.CommitIndex}, log=[{Terms(machine.Log)}]");
        }

        return string.Join(Environment.NewLine, lines);
    }

    static string Terms(IReadOnlyList<Entry> log) =>
        string.Join(", ", log.Select(entry => $"{entry.Index}:{entry.Term}"));

    /// <summary>
    /// The log of a leader at the previous check, while it stays leader in one term.
    /// </summary>
    /// <param name="Term">The term it leads.</param>
    /// <param name="Entries">Its log at the previous check.</param>
    sealed record LeaderLog(ulong Term, IReadOnlyList<Entry> Entries);

    /// <summary>
    /// A committed entry: its term and payload, as first seen committed by any replica,
    /// and the term of that replica then.
    /// </summary>
    /// <param name="Term">The term of the entry.</param>
    /// <param name="Payload">Its payload, copied.</param>
    /// <param name="CommitTerm">The term of the replica that first reported it committed.</param>
    sealed record CommittedEntry(ulong Term, byte[] Payload, ulong CommitTerm);
}
