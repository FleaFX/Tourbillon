using System.Buffers.Binary;
using System.Text;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// One seeded fault run: a 3-copy cluster, 300 steps of 20 ms with one random fault per
/// step — proposals, crashes, restarts, partitions, directed cuts, full storage — the
/// invariants checked after every step, and a recovery at the end that requires one more
/// proposal to be accepted.
/// </summary>
static class FaultRun {
    /// <summary>
    /// Runs one fault run and returns what it came to. Throws
    /// <see cref="InvariantViolationException"/> on a violation, with the seed and the
    /// step, and <see cref="InvalidOperationException"/> when the group does not recover.
    /// </summary>
    public static async Task<FaultRunSummary> RunAsync(int seed, FaultRunOptions options) {
        await using var runner = new Runner(seed, options);
        return await runner.RunAsync();
    }

    /// <summary>
    /// One run: the cluster, the faults, the proposals, the checker, the trace.
    /// </summary>
    sealed class Runner(int seed, FaultRunOptions options) : IAsyncDisposable {
        const int Copies = 3;
        const int Steps = 300;
        const int SettleSteps = 500;
        const long Reserve = 64 * 1024;
        static readonly TimeSpan Step = TimeSpan.FromMilliseconds(20);

        readonly SimulatedCluster _cluster = new(Copies, seed, new SimulationOptions(options.DropChance, options.MinDelay, options.MaxDelay, options.AllowReorder));
        readonly Random _random = new(seed);
        readonly InvariantChecker _checker = new();
        readonly List<(Task<ProposalResult> Task, byte[] Payload)> _proposals = new();
        readonly StringBuilder _trace = new();
        readonly List<(string Machine, ulong Term)> _leaders = new();
        readonly SortedSet<ulong> _terms = new();
        int _accepted;
        int _refused;
        int _unknown;
        long _payloads;

        /// <summary>
        /// Runs the fault steps, settles the group, and requires one more proposal to be
        /// accepted.
        /// </summary>
        public async Task<FaultRunSummary> RunAsync() {
            await _cluster.StartAsync();

            var step = 0;
            for (; step < Steps; step++) {
                var action = await DoActionAsync();
                await _cluster.StepAsync(Step);
                CollectOutcomes();
                _trace.Append(action).Append(';');
                TraceState();
                RecordLeadersAndTerms();
                await CheckAsync(step);
            }

            // Settle: heal the network, restart every crashed copy, free every storage.
            _cluster.Network.Heal();
            foreach (var machine in _cluster.Machines) {
                if (!_cluster.IsRunning(machine))
                    await _cluster.RestartAsync(machine);
                _cluster.SetFreeSpace(machine, long.MaxValue);
            }

            // Run until every proposal has an outcome and the group has a ready leader.
            for (; step < Steps + SettleSteps; step++) {
                await _cluster.StepAsync(Step);
                CollectOutcomes();
                TraceState();
                RecordLeadersAndTerms();
                await CheckAsync(step);
                if (_proposals.Count == 0 && ReadyLeader() is not null)
                    break;
            }

            // One more proposal: the group recovered, so it is accepted.
            var leader = ReadyLeader() ?? throw new InvalidOperationException($"[seed {seed}] the group did not recover: no ready leader after {Steps} fault steps");
            var payload = NextPayload();
            var last = _cluster.Replica(leader).ProposeAsync(payload, mayUseReserve: false, CancellationToken.None).AsTask();
            for (var settle = 0; settle < SettleSteps && !last.IsCompleted; settle++) {
                await _cluster.StepAsync(Step);
                CollectOutcomes();
                TraceState();
                RecordLeadersAndTerms();
                await CheckAsync(Steps + SettleSteps + settle);
            }

            if (!last.IsCompleted)
                throw new InvalidOperationException($"[seed {seed}] the last proposal of the fault run never got an outcome");

            var result = last.Result;
            if (result.Status != ProposalStatus.Accepted)
                throw new InvalidOperationException($"[seed {seed}] the group did not recover: the last proposal ended {result.Status}");

            _trace.Append('|').Append(Convert.ToHexString(payload)).Append(':').Append(result.Status);
            _accepted++;
            return new FaultRunSummary(_accepted, _refused, _unknown, _leaders, _terms.ToList(), TraceHash(_trace));
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() =>
            await _cluster.DisposeAsync();

        // One random fault of one step; its name goes into the trace.
        async Task<string> DoActionAsync() {
            switch (_random.Next(9)) {
                case 0: {
                    var leader = RunningLeader();
                    if (leader is not null)
                        Propose(leader);
                    return "propose at the leader";
                }
                case 1: {
                    var running = RunningMachines();
                    if (running.Length > 0)
                        Propose(running[_random.Next(running.Length)]);
                    return "propose at a copy";
                }
                case 2: {
                    var running = RunningMachines();
                    if (running.Length > 0)
                        await _cluster.CrashAsync(running[_random.Next(running.Length)]);
                    return "crash";
                }
                case 3: {
                    var crashed = _cluster.Machines.Where(machine => !_cluster.IsRunning(machine)).ToArray();
                    if (crashed.Length > 0)
                        await _cluster.RestartAsync(crashed[_random.Next(crashed.Length)]);
                    return "restart";
                }
                case 4: {
                    var order = _cluster.Machines.OrderBy(_ => _random.Next()).ToArray();
                    var size = 1 + _random.Next(order.Length - 1);
                    _cluster.Network.Partition(order.Take(size).ToArray(), order.Skip(size).ToArray());
                    return "partition";
                }
                case 5: {
                    var from = _cluster.Machines[_random.Next(_cluster.Machines.Count)];
                    var to = _cluster.Machines[_random.Next(_cluster.Machines.Count)];
                    if (from != to)
                        _cluster.Network.Cut(from, to);
                    return "cut";
                }
                case 6:
                    _cluster.Network.Heal();
                    return "heal";
                case 7: {
                    var machine = _cluster.Machines[_random.Next(_cluster.Machines.Count)];
                    if (_random.Next(2) == 0)
                        _cluster.SetFreeSpace(machine, Reserve + _random.Next(28));  // full: no unflagged entry fits
                    else
                        _cluster.SetFreeSpace(machine, long.MaxValue);
                    return "fill or free storage";
                }
                default:
                    return "nothing";
            }
        }

        // Proposes a fresh payload at one copy; the outcome is collected after a step.
        void Propose(string machine) {
            var payload = NextPayload();
            var task = _cluster.Replica(machine).ProposeAsync(payload, mayUseReserve: false, CancellationToken.None).AsTask();
            _proposals.Add((task, payload));
        }

        // A payload no earlier proposal of this run used.
        byte[] NextPayload() {
            var payload = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(payload, _payloads++);
            return payload;
        }

        // Records the outcomes of the proposals that completed, and notes the refused
        // payloads to the checker.
        void CollectOutcomes() {
            for (var i = _proposals.Count - 1; i >= 0; i--) {
                var (task, payload) = _proposals[i];
                if (!task.IsCompleted)
                    continue;

                _proposals.RemoveAt(i);
                var result = task.Result;
                switch (result.Status) {
                    case ProposalStatus.Accepted:
                        _accepted++;
                        break;
                    case ProposalStatus.Refused:
                        _refused++;
                        _checker.NoteRefusedProposal(payload);
                        break;
                    case ProposalStatus.Unknown:
                        _unknown++;
                        break;
                }

                _trace.Append('|').Append(Convert.ToHexString(payload)).Append(':').Append(result.Status);
            }
        }

        // The state of every machine, for the trace.
        void TraceState() {
            foreach (var machine in _cluster.Machines) {
                var lastIndex = _cluster.Log(machine).LastIndex;
                if (_cluster.IsRunning(machine)) {
                    var replica = _cluster.Replica(machine);
                    _trace.Append(machine).Append(':').Append(replica.Role).Append(',').Append(replica.Term).Append(',').Append(replica.CommitIndex).Append(',').Append(lastIndex).Append(';');
                } else {
                    _trace.Append(machine).Append(":crash,").Append(lastIndex).Append(';');
                }
            }
        }

        // The leaders and terms the run saw, for the summary.
        void RecordLeadersAndTerms() {
            var leader = RunningLeader();
            if (leader is not null) {
                var term = _cluster.Replica(leader).Term;
                if (_leaders.Count == 0 || _leaders[^1] != (leader, term))
                    _leaders.Add((leader, term));
            }

            foreach (var machine in _cluster.Machines)
                if (_cluster.IsRunning(machine))
                    _terms.Add(_cluster.Replica(machine).Term);
        }

        // The invariants over the whole run so far; a violation carries the seed and the
        // step.
        async Task CheckAsync(int step) {
            try {
                _checker.Assert(await _cluster.SnapshotAsync());
            } catch (InvariantViolationException ex) {
                throw new InvariantViolationException($"[seed {seed}, step {step}] {ex.Message}");
            }
        }

        string[] RunningMachines() =>
            _cluster.Machines.Where(_cluster.IsRunning).ToArray();

        string? RunningLeader() =>
            _cluster.Machines.FirstOrDefault(machine => _cluster.IsRunning(machine) && _cluster.Replica(machine).Role == ReplicaRole.Leader);

        string? ReadyLeader() =>
            _cluster.Machines.FirstOrDefault(machine => _cluster.IsRunning(machine) && _cluster.Replica(machine).Role == ReplicaRole.Leader && _cluster.Replica(machine).IsReady);

        // A hash of the trace: the same seed gives the same hash, two seeds give
        // different ones.
        static long TraceHash(StringBuilder trace) {
            const ulong offsetBasis = 0xcbf29ce484222325;
            const ulong prime = 0x100000001b3;
            var hash = offsetBasis;
            for (var i = 0; i < trace.Length; i++)
                hash = (hash ^ trace[i]) * prime;

            return (long)hash;
        }
    }
}
