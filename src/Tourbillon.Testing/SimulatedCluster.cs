using System.Buffers;

using Microsoft.Extensions.Time.Testing;

using Tourbillon.Log;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// A deterministic cluster of replicas on one fake clock and one seeded random: machines
/// machine-1 … machine-n, each with a log and a vote store that survive a crash, and a
/// application that reads the feed of its replica. Without <see cref="SimulationOptions.Disk"/>
/// the logs are in memory; with it every machine gets a simulated file system of its
/// own, a segmented log of small segments and a vote store on it, and a crash can lose
/// what was not flushed. The cluster drives the replicas itself: every step advances
/// the clock in ticks of 1 ms, delivers the messages that have arrived, and runs one
/// turn per replica in name order until none has work; the applications read their feeds after
/// every pass of turns, and the machines are checked after every turn.
/// </summary>
sealed class SimulatedCluster : IAsyncDisposable {
    // The size of the segments of the logs on disk: small, so a run rotates and
    // truncates across segments.
    const long SegmentSize = 4096;

    // The reserve of the storage limit of a log: a full replica is left this much free
    // space, so no unflagged entry fits.
    const long ReserveSize = 64 * 1024;

    // The directory of every machine's log and vote store on its own file system.
    const string DataDirectory = "/logs";

    readonly FakeTimeProvider _time = new();
    readonly Random _random;
    readonly SimulatedNetwork _network;
    readonly string[] _members;
    readonly SimulationOptions _options;
    readonly Dictionary<string, IRaftLog> _logs = new();
    readonly Dictionary<string, IVoteStore> _votes = new();
    readonly Dictionary<string, Replica> _replicas = new();
    readonly Dictionary<string, SimulatedApplication> _applications = new();
    readonly Dictionary<string, SimulatedFileSystem> _fileSystems = new();
    readonly Dictionary<string, StorageLimit> _storages = new();
    bool _started;

    SimulatedCluster(int replicas, int seed, SimulationOptions options) {
        _random = new Random(seed);
        _options = options;
        _members = Enumerable.Range(1, replicas).Select(index => $"machine-{index}").ToArray();
        _network = new SimulatedNetwork(_time, _random, options, _members);
        for (var i = 0; i < _members.Length; i++) {
            var machine = _members[i];
            _applications.Add(machine, new SimulatedApplication(machine, ApplicationRandom(seed, i)));
            if (options.Disk) {
                var fileSystem = new SimulatedFileSystem(new Random(_random.Next()));
                _fileSystems.Add(machine, fileSystem);
                _storages.Add(machine, new StorageLimit(() => fileSystem.FreeSpace, maximumSize: null, reserveSize: ReserveSize));
                _votes.Add(machine, new FileVoteStore(fileSystem, DataDirectory));
            } else {
                _logs.Add(machine, new InMemoryLog(maximumSize: null, reserveSize: ReserveSize));
                _votes.Add(machine, new InMemoryVoteStore());
            }
        }
    }

    /// <summary>
    /// Creates a cluster of <paramref name="replicas"/> replicas, seeded with
    /// <paramref name="seed"/> and run with <paramref name="options"/>; the seed also
    /// seeds every replica's own random. With <see cref="SimulationOptions.Disk"/>
    /// every machine gets a simulated file system and its log and vote store on it.
    /// The logs are opened before the cluster is returned, so a replica can be seeded.
    /// </summary>
    public static async Task<SimulatedCluster> CreateAsync(int replicas, int seed, SimulationOptions options) {
        var cluster = new SimulatedCluster(replicas, seed, options);
        await cluster.OpenAsync();
        return cluster;
    }

    /// <summary>
    /// The names of the replicas, in order.
    /// </summary>
    public IReadOnlyList<string> Machines => _members;

    /// <summary>
    /// The network the replicas send through.
    /// </summary>
    public SimulatedNetwork Network => _network;

    /// <summary>
    /// The clock every replica and the network run on.
    /// </summary>
    public FakeTimeProvider Time => _time;

    /// <summary>
    /// The running replica on one machine; a crashed machine has none until it restarts.
    /// </summary>
    public Replica Replica(string machine) => _replicas[machine];

    /// <summary>
    /// Whether one replica is currently running.
    /// </summary>
    public bool IsRunning(string machine) => _replicas.ContainsKey(machine);

    /// <summary>
    /// The log of one replica, surviving crashes; read it, never write it.
    /// </summary>
    public IRaftLog Log(string machine) => _logs[machine];

    /// <summary>
    /// The vote store of one replica, surviving crashes.
    /// </summary>
    public IVoteStore Votes(string machine) => _votes[machine];

    /// <summary>
    /// The application of one replica: it reads the feed of its replica after every pass of turns,
    /// and its stored D is what a restart opens the group with.
    /// </summary>
    public SimulatedApplication Application(string machine) => _applications[machine];

    /// <summary>
    /// Seeds one replica with a term, a vote and entries, before <see cref="StartAsync"/>.
    /// </summary>
    public async ValueTask SeedAsync(string machine, ulong term, string? votedFor, params Entry[] entries) {
        if (_started)
            throw new InvalidOperationException($"The cluster has started; {machine} can no longer be seeded.");

        await _logs[machine].AppendAsync(entries);
        await _votes[machine].SaveAsync(term, votedFor);
    }

    /// <summary>
    /// Loads every replica: its term and vote, its feed from the index its application stored,
    /// and its election timer.
    /// </summary>
    public async ValueTask StartAsync() {
        foreach (var machine in _members)
            await _replicas[machine].LoadAsync(_applications[machine].Stored);

        _started = true;
    }

    /// <summary>
    /// Advances the clock by <paramref name="duration"/> in ticks of 1 ms: every tick
    /// fires the timers that are due, delivers the messages that have arrived, and runs
    /// one turn per running replica in name order until none has work, with the applications
    /// reading their feeds after every pass of turns and the machines checked after
    /// every turn. A turn that does not complete synchronously fails the step: a run
    /// must be repeatable. A crash during a file operation that is still armed at the
    /// end of the step crashes its machine then.
    /// </summary>
    public async ValueTask StepAsync(TimeSpan duration) {
        var ticks = duration.Ticks / TimeSpan.TicksPerMillisecond;
        for (var tick = 0; tick < ticks; tick++) {
            _time.Advance(TimeSpan.FromMilliseconds(1));
            _network.DeliverDue(_time.GetUtcNow(), Deliver);
            await RunAllTurnsAsync();
        }

        // A crash during a file operation that never came: the replica did fewer file
        // operations than it was armed for, and crashes now, at the end of the step.
        if (_options.Disk)
            foreach (var machine in _members)
                if (_fileSystems[machine].IsArmed)
                    await CrashAsync(machine);
    }

    /// <summary>
    /// Crashes one replica: its replica stops, its proposals end, and every message on its
    /// way to it is dropped; its log and vote store survive. On disk the file system
    /// takes its crash — what was not flushed is lost — and the log is opened again
    /// right away, so the checker sees exactly what survived; a log that refuses to
    /// open fails here. The application of the replica loses what it applied after its last store.
    /// </summary>
    public async ValueTask CrashAsync(string machine) {
        if (_replicas.Remove(machine, out var replica))
            await replica.DisposeAsync();

        _network.DropInFlightTo(machine);

        if (_options.Disk) {
            var fileSystem = _fileSystems[machine];
            if (fileSystem.IsCrashed)
                fileSystem.Restart();  // a crash during an operation already took its model
            else
                fileSystem.Crash(_random);  // crash now: the model takes what was not flushed

            (_logs[machine] as SegmentedLog)?.Dispose();
            _logs[machine] = await SegmentedLog.OpenAsync(fileSystem, DataDirectory, SegmentSize, _storages[machine], CancellationToken.None);
        }

        _applications[machine].Crash();
    }

    /// <summary>
    /// Arms a crash during a file operation of one replica: its
    /// <paramref name="operations"/>-th following change of its file system crashes it
    /// first. When the step ends before that — the replica did fewer file operations —
    /// it crashes at the end of the step.
    /// </summary>
    public ValueTask CrashDuringAsync(string machine, int operations) {
        if (!_options.Disk)
            throw new InvalidOperationException("A crash during a file operation needs a cluster on disk.");

        _fileSystems[machine].CrashAfter(operations);
        return default;
    }

    /// <summary>
    /// Restarts a crashed replica from its log and vote store, and opens its group with
    /// the D its application stored.
    /// </summary>
    public async ValueTask RestartAsync(string machine) {
        CreateReplica(machine);
        await _replicas[machine].LoadAsync(_applications[machine].Stored);
    }

    /// <summary>
    /// Sets the free space the storage limit of one replica measures.
    /// </summary>
    public void SetFreeSpace(string machine, long bytes) {
        if (_options.Disk)
            _fileSystems[machine].FreeSpace = bytes;
        else if (_logs[machine] is InMemoryLog log)
            log.FreeSpace = bytes;
    }

    /// <summary>
    /// The observable state of every replica at one moment, for the invariant checker: the
    /// log with the payloads copied, what the application of the replica applied through the
    /// feed, and the role, term, vote and commit index of its replica when it runs.
    /// </summary>
    public async ValueTask<IReadOnlyList<MachineState>> SnapshotAsync() {
        var states = new List<MachineState>(_members.Length);
        foreach (var machine in _members) {
            var entries = await ReadAllAsync(_logs[machine]);
            var applied = _applications[machine].Applied.ToArray();

            if (_replicas.TryGetValue(machine, out var replica)) {
                states.Add(new MachineState(machine, true, replica.Role, replica.Term, replica.VotedFor, replica.CommitIndex, entries, applied));
            } else {
                var (term, votedFor) = await _votes[machine].LoadAsync(CancellationToken.None);
                states.Add(new MachineState(machine, false, ReplicaRole.Follower, term, votedFor, 0, entries, applied));
            }
        }

        return states;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        foreach (var replica in _replicas.Values)
            await replica.DisposeAsync();

        if (_options.Disk)
            foreach (var machine in _members)
                (_logs[machine] as SegmentedLog)?.Dispose();
    }

    // The seed of one machine's application: from the cluster's seed and the machine's place,
    // so the moments the applications store at are seeded without drawing from the cluster's
    // own random — the runs on the in-memory log stay what they were.
    static Random ApplicationRandom(int seed, int machine) =>
        new(seed * 1000003 + machine);

    // Opens the logs on disk and creates the replicas; the in-memory logs are ready
    // from the constructor.
    async Task OpenAsync() {
        if (_options.Disk)
            foreach (var machine in _members)
                _logs.Add(machine, await SegmentedLog.OpenAsync(_fileSystems[machine], DataDirectory, SegmentSize, _storages[machine], CancellationToken.None));

        foreach (var machine in _members)
            CreateReplica(machine);
    }

    void CreateReplica(string machine) {
        var replica = new Replica(
            machine,
            _members,
            _logs[machine],
            _votes[machine],
            new SimulatedTransport(machine, _network),
            new ReplicaOptions { PoisonFeed = true },
            _time,
            new Random(_random.Next()));
        _replicas[machine] = replica;
    }

    // Every entry of one log, payloads copied, through one pooled buffer.
    static async ValueTask<Entry[]> ReadAllAsync(IRaftLog log) {
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

    // One turn per running replica, in name order, until none has work: messages sent with
    // zero delay arrive in the same tick and get their turn in the next pass. After
    // every pass of turns the applications read their feeds.
    async ValueTask RunAllTurnsAsync() {
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

                await CheckAfterTurnAsync();
            }

            RunApplications();

            if (!worked && delivered == 0)
                return;
        }
    }

    // After every turn: a file system that crashed during an operation crashes its
    // machine, and any other fault of a replica fails the step — in the simulation a
    // broken replica is a bug.
    async ValueTask CheckAfterTurnAsync() {
        if (_options.Disk)
            foreach (var machine in _members)
                if (_fileSystems[machine].IsCrashed && _replicas.ContainsKey(machine))
                    await CrashAsync(machine);

        foreach (var machine in _members) {
            if (_replicas.TryGetValue(machine, out var replica) && replica.Fault is { } fault)
                throw new InvalidOperationException($"The replica '{machine}' broke in the simulation: {fault.Message}", fault);
        }
    }

    // The applications of the running replicas read what their feeds hand out.
    void RunApplications() {
        foreach (var machine in _members)
            if (_replicas.TryGetValue(machine, out var replica))
                _applications[machine].Read(replica);
    }
}
