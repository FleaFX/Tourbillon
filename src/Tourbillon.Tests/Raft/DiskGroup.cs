using AwesomeAssertions;

using Microsoft.Extensions.Time.Testing;

using Tourbillon.Log;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

// One group of copies on the real disk, each in its own temporary directory, with
// real loops and an in-process transport, for the tests of replicas on disk. A copy
// that is not started runs on a fake clock: the test runs its turns itself.
sealed class DiskGroup : IAsyncDisposable {
    public const long SegmentSize = 4096;

    readonly List<string> _machines;
    readonly ReplicaOptions _options;
    readonly Func<string, IVoteStore>? _votes;
    readonly List<TempDirectory> _directories = new();
    readonly List<SegmentedLog> _logs = new();
    readonly List<Replica> _replicas = new();
    readonly Dictionary<string, Replica> _byName = new();

    DiskGroup(List<string> machines, ReplicaOptions options, Func<string, IVoteStore>? votes) {
        _machines = machines;
        _options = options;
        _votes = votes;
    }

    public IReadOnlyList<string> Machines => _machines;

    public Replica Replica(string machine) => _byName[machine];

    public SegmentedLog Log(string machine) => _logs[_machines.IndexOf(machine)];

    public Feed Feed(string machine) => _byName[machine].Feed!;

    public static async Task<DiskGroup> StartAsync(
        int copies,
        ReplicaOptions? options = null,
        Func<string, IVoteStore>? votes = null,
        IReadOnlyList<string>? withoutLoop = null
    ) {
        var machines = Enumerable.Range(1, copies).Select(index => $"machine-{index}").ToList();
        var group = new DiskGroup(machines, options ?? new ReplicaOptions(), votes);
        foreach (var machine in machines) {
            var index = machines.IndexOf(machine);
            var directory = new TempDirectory();
            group._directories.Add(directory);
            var log = await SegmentedLog.OpenAsync(DiskFileSystem.Instance, directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            group._logs.Add(log);
            var replica = new Replica(
                machine,
                machines,
                log,
                votes is not null ? votes(machine) : new FileVoteStore(DiskFileSystem.Instance, directory.Path),
                new InProcessTransport(machine, group._byName),
                group._options,
                withoutLoop?.Contains(machine) == true ? new FakeTimeProvider() : TimeProvider.System,
                new Random(1 + index));
            group._byName[machine] = replica;
            group._replicas.Add(replica);
        }

        foreach (var machine in machines) {
            var replica = group._byName[machine];
            if (withoutLoop?.Contains(machine) == true)
                await replica.LoadAsync(0, TestContext.Current.CancellationToken);
            else
                await replica.StartAsync(0, TestContext.Current.CancellationToken);
        }

        return group;
    }

    // Stops one copy and opens it again on the same directory, with the feed at the
    // index the user applied: recovery gives the log back, the feed starts after it.
    public async Task ReopenAsync(string machine, ulong applied, bool startLoop = true) {
        var index = _machines.IndexOf(machine);
        await _replicas[index].DisposeAsync();
        _logs[index].Dispose();
        var log = await SegmentedLog.OpenAsync(DiskFileSystem.Instance, _directories[index].Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
        _logs[index] = log;
        var replica = new Replica(
            machine,
            _machines,
            log,
            _votes is not null ? _votes(machine) : new FileVoteStore(DiskFileSystem.Instance, _directories[index].Path),
            new InProcessTransport(machine, _byName),
            _options,
            TimeProvider.System,
            new Random(1 + index));
        _byName[machine] = replica;
        _replicas[index] = replica;
        if (startLoop)
            await replica.StartAsync(applied, TestContext.Current.CancellationToken);
        else
            await replica.LoadAsync(applied, TestContext.Current.CancellationToken);
    }

    // Reads one copy's feed up to the target index, batch by batch: every batch must
    // start right after the one before and never go beyond the commit index; the
    // entries are copied into the list.
    public static async Task ReadUpToAsync(Replica replica, ulong target, List<Entry> into) {
        var feed = replica.Feed!;
        while (feed.Delivered < target) {
            (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
            var start = feed.Delivered + 1;
            var batch = await feed.ReadAsync(TestContext.Current.CancellationToken);
            batch.Length.Should().BePositive();
            batch.Span[0].Index.Should().Be(start);
            batch.Span[^1].Index.Should().BeLessThanOrEqualTo(replica.CommitIndex);
            foreach (var entry in batch.Span)
                into.Add(new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray()));
        }
    }

    public async ValueTask DisposeAsync() {
        foreach (var replica in _replicas)
            await replica.DisposeAsync();
        foreach (var log in _logs)
            log.Dispose();
        foreach (var directory in _directories)
            directory.Dispose();
    }

    public static StorageLimit UnlimitedStorage() => new(() => long.MaxValue, maximumSize: null, reserveSize: 0);
}
