using System.Buffers;
using Microsoft.Extensions.Time.Testing;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// A deterministic cluster of replicas on one fake clock and one seeded random: machines
/// machine-1 … machine-n, each with an in-memory log and vote store that survive a
/// crash. The cluster drives the replicas itself: every step advances the clock in
/// ticks of 1 ms, delivers the messages that have arrived, and runs one turn per
/// replica in name order until none has work.
/// </summary>
sealed class SimulatedCluster : IAsyncDisposable {
    readonly FakeTimeProvider _time = new();
    readonly Random _random;
    readonly SimulatedNetwork _network;
    readonly string[] _members;
    readonly Dictionary<string, InMemoryLog> _logs = new();
    readonly Dictionary<string, InMemoryVoteStore> _votes = new();
    readonly Dictionary<string, Replica> _replicas = new();
    bool _started;

    /// <summary>
    /// A cluster of <paramref name="copies"/> copies, seeded with
    /// <paramref name="seed"/> and run with <paramref name="options"/>; the seed also
    /// seeds every replica's own random.
    /// </summary>
    public SimulatedCluster(int copies, int seed, SimulationOptions options) {
        _random = new Random(seed);
        _members = Enumerable.Range(1, copies).Select(index => $"machine-{index}").ToArray();
        _network = new SimulatedNetwork(_time, _random, options, _members);
        foreach (var machine in _members) {
            _logs.Add(machine, new InMemoryLog(maximumSize: null, reserveSize: 64 * 1024));
            _votes.Add(machine, new InMemoryVoteStore());
            CreateReplica(machine);
        }
    }

    /// <summary>
    /// The names of the copies, in order.
    /// </summary>
    public IReadOnlyList<string> Machines => _members;

    /// <summary>
    /// The network the copies send through.
    /// </summary>
    public SimulatedNetwork Network => _network;

    /// <summary>
    /// The clock every copy and the network run on.
    /// </summary>
    public FakeTimeProvider Time => _time;

    /// <summary>
    /// The running replica of one copy; a crashed copy has none until it restarts.
    /// </summary>
    public Replica Replica(string machine) => _replicas[machine];

    /// <summary>
    /// Whether one copy is currently running.
    /// </summary>
    public bool IsRunning(string machine) => _replicas.ContainsKey(machine);

    /// <summary>
    /// The log of one copy, surviving crashes; read it, never write it.
    /// </summary>
    public InMemoryLog Log(string machine) => _logs[machine];

    /// <summary>
    /// The vote store of one copy, surviving crashes.
    /// </summary>
    public InMemoryVoteStore Votes(string machine) => _votes[machine];

    /// <summary>
    /// Seeds one copy with a term, a vote and entries, before <see cref="StartAsync"/>.
    /// </summary>
    public async ValueTask SeedAsync(string machine, ulong term, string? votedFor, params Entry[] entries) {
        if (_started)
            throw new InvalidOperationException($"The cluster has started; {machine} can no longer be seeded.");

        await _logs[machine].AppendAsync(entries);
        await _votes[machine].SaveAsync(term, votedFor);
    }

    /// <summary>
    /// Loads every copy: its term and vote, and its election timer.
    /// </summary>
    public async ValueTask StartAsync() {
        foreach (var machine in _members)
            await _replicas[machine].LoadAsync();

        _started = true;
    }

    /// <summary>
    /// Advances the clock by <paramref name="duration"/> in ticks of 1 ms: every tick
    /// fires the timers that are due, delivers the messages that have arrived, and runs
    /// one turn per running copy in name order until none has work. A turn that does
    /// not complete synchronously fails the step: a run must be repeatable.
    /// </summary>
    public ValueTask StepAsync(TimeSpan duration) {
        var ticks = duration.Ticks / TimeSpan.TicksPerMillisecond;
        for (var tick = 0; tick < ticks; tick++) {
            _time.Advance(TimeSpan.FromMilliseconds(1));
            _network.DeliverDue(_time.GetUtcNow(), Deliver);
            RunAllTurns();
        }

        return default;
    }

    /// <summary>
    /// Crashes one copy: its replica stops, its proposals end, and every message on its
    /// way to it is dropped; its log and vote store survive.
    /// </summary>
    public async ValueTask CrashAsync(string machine) {
        if (_replicas.Remove(machine, out var replica))
            await replica.DisposeAsync();

        _network.DropInFlightTo(machine);
    }

    /// <summary>
    /// Restarts a crashed copy from its log and vote store.
    /// </summary>
    public async ValueTask RestartAsync(string machine) {
        CreateReplica(machine);
        await _replicas[machine].LoadAsync();
    }

    /// <summary>
    /// Sets the free space the storage limit of one copy measures.
    /// </summary>
    public void SetFreeSpace(string machine, long bytes) =>
        _logs[machine].FreeSpace = bytes;

    /// <summary>
    /// The observable state of every copy at one moment, for the invariant checker: the
    /// log with the payloads copied, and the role, term, vote and commit index of its
    /// replica when it runs.
    /// </summary>
    public async ValueTask<IReadOnlyList<MachineState>> SnapshotAsync() {
        var states = new List<MachineState>(_members.Length);
        foreach (var machine in _members) {
            var entries = await ReadAllAsync(_logs[machine]);

            if (_replicas.TryGetValue(machine, out var replica)) {
                states.Add(new MachineState(machine, true, replica.Role, replica.Term, replica.VotedFor, replica.CommitIndex, entries));
            } else {
                var (term, votedFor) = await _votes[machine].LoadAsync(CancellationToken.None);
                states.Add(new MachineState(machine, false, ReplicaRole.Follower, term, votedFor, 0, entries));
            }
        }

        return states;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        foreach (var replica in _replicas.Values)
            await replica.DisposeAsync();
    }

    void CreateReplica(string machine) {
        var replica = new Replica(
            machine,
            _members,
            _logs[machine],
            _votes[machine],
            new SimulatedTransport(machine, _network),
            new ReplicaOptions(),
            _time,
            new Random(_random.Next()));
        _replicas[machine] = replica;
    }

    // Every entry of one log, payloads copied, through one pooled buffer.
    static async ValueTask<Entry[]> ReadAllAsync(InMemoryLog log) {
        var total = 0;
        for (ulong index = 1; index <= log.LastIndex; index++)
            total += log.GetRecordLength(index);

        var buffer = ArrayPool<byte>.Shared.Rent(total);
        try {
            var entries = new Entry[log.LastIndex];
            var offset = 0;
            for (ulong index = 1; index <= log.LastIndex; index++) {
                var length = log.GetRecordLength(index);
                var entry = await log.ReadAsync(index, buffer.AsMemory(offset, length), CancellationToken.None);
                entries[index - 1] = new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray());
                offset += length;
            }

            return entries;
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    void Deliver(string from, string to, object message) {
        if (!_replicas.TryGetValue(to, out var replica))
            return;  // crashed: the message is lost

        switch (message) {
            case VoteRequest request:
                replica.Receive(from, request);
                break;
            case VoteResponse response:
                replica.Receive(from, response);
                break;
            case AppendRequest request:
                replica.Receive(from, request);
                break;
            case AppendResponse response:
                replica.Receive(from, response);
                break;
        }
    }

    // One turn per running copy, in name order, until none has work: messages sent with
    // zero delay arrive in the same tick and get their turn in the next pass.
    void RunAllTurns() {
        while (true) {
            var delivered = _network.DeliverDue(_time.GetUtcNow(), Deliver);
            var worked = false;
            foreach (var machine in _members) {
                if (!_replicas.TryGetValue(machine, out var replica))
                    continue;  // crashed

                var turn = replica.RunTurnAsync();
                if (!turn.IsCompletedSuccessfully)
                    throw new InvalidOperationException($"The turn of {machine} did not complete synchronously; a step of the cluster must be repeatable.");

                if (turn.Result)
                    worked = true;
            }

            if (!worked && delivered == 0)
                return;
        }
    }
}
