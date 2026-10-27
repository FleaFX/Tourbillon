using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// Checks the invariants of the group over the whole run so far: at most one leader per
/// term, a leader that never overwrites or removes entries of its own log, same index and
/// term meaning identical up to there, every application applying the same entry at an index,
/// a committed entry in the log of every later leader, and a refused proposal in no log.
/// A violation throws <see cref="InvariantViolationException"/> with a dump of every
/// machine.
/// </summary>
sealed class InvariantChecker {
    // At most one leader per term, over the whole run so far.
    readonly Dictionary<ulong, string> _leaderByTerm = new();

    // Per machine, while it stays leader in one term: its log at the previous check.
    readonly Dictionary<string, LeaderLog> _leaderLogs = new();

    // Per index, the term and payload first seen applied there, by any machine's application,
    // and the term of that machine when it reported the entry.
    readonly SortedDictionary<ulong, CommittedEntry> _appliedByIndex = new();

    // Per machine, how far its application's applied entries were verified.
    readonly Dictionary<string, int> _verifiedApplied = new();

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
        CheckApplied(machines);
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

    // While a replica stays leader in one term, its log starts with its log at the previous
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

    // Invariant 9: every replica applies the same entry at a given index. What the
    // applications applied through the feed decides: one map of index to (term, payload)
    // over the whole run, and every applied entry of every machine — also again after
    // a restart — must match it, and follow the one before without a gap.
    void CheckApplied(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            var applied = machine.Applied;

            // A crash rolled the application back to its stored D: verify from there again.
            _verifiedApplied.TryGetValue(machine.Name, out var from);
            if (from > applied.Count)
                from = applied.Count;

            for (var i = from; i < applied.Count; i++) {
                var entry = applied[i];
                if (entry.Index != (ulong)i + 1)
                    throw Violation($"The application of '{machine.Name}' applied entry {entry.Index} where entry {i + 1} belongs: the feed skipped an index — after a restart it must start at its D + 1.", machines);

                if (_appliedByIndex.TryGetValue(entry.Index, out var committed)) {
                    if (committed.Term != entry.Term || !committed.Payload.AsSpan().SequenceEqual(entry.Payload.Span))
                        throw Violation($"'{machine.Name}' applied term {entry.Term} at index {entry.Index}, but term {committed.Term} was already applied there by another machine.", machines);
                } else {
                    _appliedByIndex.Add(entry.Index, new CommittedEntry(entry.Term, entry.Payload.ToArray(), machine.Term));
                }
            }

            if (applied.Count > from)
                _verifiedApplied[machine.Name] = applied.Count;
        }
    }

    // A committed entry is in the log of every later leader. An application applies an entry
    // only once its replica committed it, and a replica learns a commit only from a leader of
    // its own term, so the term of the machine whose application first applied it is a term
    // whose leader has the entry: every leader of that term or later has it too. A
    // leader of an earlier term is a stale believer and is exempt.
    void CheckLaterLeaders(IReadOnlyList<MachineState> machines) {
        foreach (var machine in machines) {
            if (!machine.Running || machine.Role != ReplicaRole.Leader)
                continue;

            foreach (var (index, committed) in _appliedByIndex) {
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

    // A dump of every machine: role, term, vote, commit, applied, log as index:term.
    static string Dump(IReadOnlyList<MachineState> machines) {
        var lines = new List<string> { "", "--- the cluster at the moment of the violation ---" };
        foreach (var machine in machines) {
            if (!machine.Running) {
                lines.Add($"  {machine.Name}: CRASHED, applied={machine.Applied.Count}, log=[{Terms(machine.Log)}]");
                continue;
            }

            var votedFor = machine.VotedFor ?? "null";
            lines.Add($"  {machine.Name}: role={machine.Role}, term={machine.Term}, votedFor={votedFor}, commit={machine.CommitIndex}, applied={machine.Applied.Count}, log=[{Terms(machine.Log)}]");
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
    /// A committed entry: its term and payload, as first seen applied by the application of a
    /// machine, and the term of that machine then.
    /// </summary>
    /// <param name="Term">The term of the entry.</param>
    /// <param name="Payload">Its payload, copied.</param>
    /// <param name="CommitTerm">The term of the machine whose application first applied it.</param>
    sealed record CommittedEntry(ulong Term, byte[] Payload, ulong CommitTerm);
}
