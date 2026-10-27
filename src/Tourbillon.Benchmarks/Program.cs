using System.Diagnostics;
using System.Globalization;

using Tourbillon;
using Tourbillon.Log;
using Tourbillon.Raft;
using Tourbillon.Testing;

// Measures the log under concurrent appends (mode "log") or a group of three replicas
// on the real disk (mode "raft"): per combination of log count or proposer count and
// batch size, a closed loop of writers runs back to back, and the run reports
// throughput and per-operation latency percentiles as a Markdown table.
var mode = "log";
var directory = ".";
var logCounts = new[] { 1, 16, 64 };
var batchSizes = new[] { 1, 10, 100, 1000 };
var payloadSize = 256;
var seconds = 10;
var warmupSeconds = 2;
var slowReaderMs = 0;

var argument = 0;
if (args.Length > 0 && !args[0].StartsWith("--", StringComparison.Ordinal)) {
    mode = args[0];
    argument = 1;
}

for (; argument < args.Length; argument++) {
    switch (args[argument]) {
        case "--dir":
            directory = args[++argument];
            break;
        case "--logs":
            logCounts = ParseList(args[++argument]);
            break;
        case "--batch":
            batchSizes = ParseList(args[++argument]);
            break;
        case "--payload":
            payloadSize = int.Parse(args[++argument], CultureInfo.InvariantCulture);
            break;
        case "--seconds":
            seconds = int.Parse(args[++argument], CultureInfo.InvariantCulture);
            break;
        case "--warmup":
            warmupSeconds = int.Parse(args[++argument], CultureInfo.InvariantCulture);
            break;
        case "--slow-reader":
            slowReaderMs = int.Parse(args[++argument], CultureInfo.InvariantCulture);
            break;
        default:
            throw new ArgumentException($"Unknown argument '{args[argument]}'.");
    }
}

if (mode == "raft")
    await RunRaftAsync(directory, batchSizes, payloadSize, seconds, warmupSeconds, slowReaderMs);
else if (mode == "log")
    RunLog(directory, logCounts, batchSizes, payloadSize, seconds, warmupSeconds);
else
    throw new ArgumentException($"Unknown mode '{mode}': 'log' or 'raft'.");

static int[] ParseList(string value) =>
    value.Split(',').Select(int.Parse).ToArray();

// The "log" mode: per combination of log count and batch size, every log appends
// batches back to back on a dedicated thread.
static void RunLog(
    string directory,
    int[] logCounts,
    int[] batchSizes,
    int payloadSize,
    int seconds,
    int warmupSeconds
) {
    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"mode=log dir={directory} payload={payloadSize} seconds={seconds} warmup={warmupSeconds}"));
    Console.WriteLine();

    var payload = new byte[payloadSize];
    var recordLength = RecordFormat.GetLength(payloadSize);

    foreach (var logCount in logCounts)
        foreach (var batchSize in batchSizes)
            RunCombination(directory, logCount, batchSize, payload, recordLength, warmupSeconds, seconds);
}

// The "raft" mode: per batch size, one group of three replicas, and a closed loop of
// that many concurrent proposers at the leader.
static async Task RunRaftAsync(
    string directory,
    int[] batchSizes,
    int payloadSize,
    int seconds,
    int warmupSeconds,
    int slowReaderMs
) {
    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"mode=raft dir={directory} payload={payloadSize} seconds={seconds} warmup={warmupSeconds} slow-reader={slowReaderMs}"));
    Console.WriteLine();

    var payload = new byte[payloadSize];

    foreach (var batchSize in batchSizes)
        await RunRaftCombinationAsync(directory, batchSize, payload, warmupSeconds, seconds, slowReaderMs);
}

// One run of the "raft" mode: a group of three replicas, each on its own directory
// under the root, with real loops and the real disk; a closed loop of batchSize
// concurrent proposers at the leader, and a reader on every replica's feed.
static async Task RunRaftCombinationAsync(
    string directory,
    int batchSize,
    byte[] payload,
    int warmupSeconds,
    int seconds,
    int slowReaderMs
) {
    const long SegmentSize = 64 * 1024 * 1024;
    var machines = new[] { "machine-1", "machine-2", "machine-3" };

    var root = Path.Combine(directory, $"raft-{batchSize}");
    Delete(root);
    Directory.CreateDirectory(root);

    // Every replica gets a segmented log and a vote store on the real disk; the logs
    // are counted, so the leader's appends show the real batch size.
    var logs = new SegmentedLog[machines.Length];
    var counting = new CountingLog[machines.Length];
    var replicas = new Dictionary<string, Replica>();
    for (var i = 0; i < machines.Length; i++) {
        var replicaDirectory = Path.Combine(root, machines[i]);
        Directory.CreateDirectory(replicaDirectory);
        var storage = new StorageLimit(replicaDirectory, maximumSize: null, reserveSize: 0);
        logs[i] = await SegmentedLog.OpenAsync(DiskFileSystem.Instance, replicaDirectory, SegmentSize, storage);
        counting[i] = new CountingLog(logs[i]);
        replicas[machines[i]] = new Replica(
            machines[i],
            machines,
            counting[i],
            new FileVoteStore(DiskFileSystem.Instance, replicaDirectory),
            new InProcessTransport(machines[i], replicas),
            new ReplicaOptions(),
            TimeProvider.System,
            new Random(1 + i));
    }

    foreach (var machine in machines)
        await replicas[machine].StartAsync(0);

    // The group elects one leader; the proposals go to it.
    var leader = await WaitReadyLeaderAsync(replicas);
    var leaderLog = counting[Array.IndexOf(machines, leader)];

    // Every replica's feed is read by a reader that copies nothing.
    var readers = new Task[machines.Length];
    for (var i = 0; i < machines.Length; i++)
        readers[i] = ReadFeedAsync(replicas[machines[i]].Feed!, slowReaderMs);

    // Warmup: the first proposals pay for JIT, the election and cold caches; the
    // outcomes are dropped.
    await RunProposersAsync(replicas[leader], payload, batchSize, warmupSeconds, null);

    // Measure: a closed loop of batchSize concurrent proposers, one duration per
    // proposal that completes inside the window.
    var latencies = new List<long>();
    var appendsBefore = leaderLog.Appends;
    var (committed, other) = await RunProposersAsync(replicas[leader], payload, batchSize, seconds, latencies);
    var appends = leaderLog.Appends - appendsBefore;

    // Stop the group: the feeds end, the readers follow.
    foreach (var machine in machines)
        await replicas[machine].DisposeAsync();
    await Task.WhenAll(readers);

    foreach (var log in logs)
        log.Dispose();

    var sorted = latencies.ToArray();
    Array.Sort(sorted);

    var window = (double)seconds;
    var proposalsPerSecond = committed / window;
    var appendsPerSecond = appends / window;

    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"## raft batch={batchSize} payload={payload.Length}"));
    Console.WriteLine();
    Console.WriteLine("| proposals/s | p50 | p99 | max | appends/s |");
    Console.WriteLine("| ---: | ---: | ---: | ---: | ---: |");
    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"| {proposalsPerSecond:N0} | {Percentile(sorted, 0.50):N0} us | {Percentile(sorted, 0.99):N0} us | {Max(sorted):N0} us | {appendsPerSecond:N0} |"));
    Console.WriteLine();
    if (other > 0) {
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"proposals that were not committed: {other}"));
        Console.WriteLine();
    }

    Delete(root);
}

// Waits for the group to elect a ready leader; a replica that breaks fails the run.
static async Task<string> WaitReadyLeaderAsync(Dictionary<string, Replica> replicas) {
    var deadline = Stopwatch.GetTimestamp() + 60L * Stopwatch.Frequency;
    while (Stopwatch.GetTimestamp() < deadline) {
        foreach (var (machine, replica) in replicas)
            if (replica.Fault is { } fault)
                throw new InvalidOperationException($"The replica '{machine}' broke: {fault.Message}", fault);

        var leader = replicas.Where(pair => pair.Value.IsReady).Select(pair => pair.Key).FirstOrDefault();
        if (leader is not null)
            return leader;

        await Task.Delay(10);
    }

    throw new InvalidOperationException("The group did not elect a ready leader within 60 s.");
}

// Reads one replica's feed until the replica stops, copying nothing; a slow reader sleeps
// per batch, to show that committing never waits for the feed.
static async Task ReadFeedAsync(Feed feed, int slowReaderMs) {
    while (await feed.WaitToReadAsync()) {
        await feed.ReadAsync();
        if (slowReaderMs > 0)
            await Task.Delay(slowReaderMs);
    }
}

// A closed loop of count concurrent proposers at the leader, until the window of
// seconds ends; one duration per proposal that completes inside the window, into
// latencies when it is given. Returns the committed and the other outcomes.
static async Task<(long Committed, long Other)> RunProposersAsync(
    Replica leader,
    byte[] payload,
    int count,
    int seconds,
    List<long>? latencies
) {
    var end = Stopwatch.GetTimestamp() + seconds * Stopwatch.Frequency;
    var proposers = new Task<(long Committed, long Other)>[count];
    for (var i = 0; i < count; i++)
        proposers[i] = Task.Run(async () => {
            var own = new List<long>();
            long committed = 0;
            long other = 0;
            while (Stopwatch.GetTimestamp() < end) {
                var start = Stopwatch.GetTimestamp();
                var result = await leader.ProposeAsync(payload, false, CancellationToken.None);
                if (Stopwatch.GetTimestamp() > end)
                    break;  // completed after the window: not measured

                if (result.Status == ProposalStatus.Committed) {
                    committed++;
                    own.Add(Stopwatch.GetTimestamp() - start);
                } else {
                    other++;
                }
            }

            if (latencies is not null)
                lock (latencies)
                    latencies.AddRange(own);

            return (committed, other);
        });

    var results = await Task.WhenAll(proposers);
    return (results.Sum(result => result.Committed), results.Sum(result => result.Other));
}

static void RunCombination(
    string directory,
    int logCount,
    int batchSize,
    byte[] payload,
    int recordLength,
    int warmupSeconds,
    int seconds
) {
    const long SegmentSize = 64 * 1024 * 1024;

    var root = Path.Combine(directory, $"bench-{logCount}-{batchSize}");
    Delete(root);
    Directory.CreateDirectory(root);

    // One limit for all logs, no maximum: the free space of the disk is the only limit.
    var storage = new StorageLimit(root, maximumSize: null, reserveSize: 0);

    var logs = new SegmentedLog[logCount];
    for (var i = 0; i < logCount; i++)
        logs[i] = SegmentedLog.OpenAsync(DiskFileSystem.Instance, Path.Combine(root, $"log-{i:D3}"), SegmentSize, storage).AsTask().GetAwaiter().GetResult();

    var results = new (long Entries, long Appends, long[] Durations)[logCount];
    var threads = new Thread[logCount];
    for (var i = 0; i < logCount; i++) {
        var log = logs[i];
        var slot = i;
        var batch = new Entry[batchSize];
        threads[i] = new Thread(() => {
            var index = 1UL;

            // Warmup: the first appends pay for JIT and cold caches; the results are dropped.
            long warmupAppends = 0;
            var warmupEnd = Stopwatch.GetTimestamp() + warmupSeconds * Stopwatch.Frequency;
            while (Stopwatch.GetTimestamp() < warmupEnd) {
                for (var e = 0; e < batchSize; e++)
                    batch[e] = new Entry(index + (ulong)e, 1, EntryKind.Command, EntryFlags.None, payload);

                var written = log.AppendAsync(batch).AsTask().GetAwaiter().GetResult();
                if (written == 0)
                    break;

                index += (ulong)written;
                warmupAppends++;
            }

            // Measure: one duration per append in a preallocated array, sized from the
            // warmup rate; it doubles when the estimate was too low.
            var capacity = warmupSeconds > 0 ? Math.Max(1024L, (long)(warmupAppends * 2.0 * seconds / warmupSeconds)) : 1024L;
            var measured = new long[capacity];
            var count = 0;
            long entries = 0;
            long appends = 0;
            var end = Stopwatch.GetTimestamp() + seconds * Stopwatch.Frequency;
            while (Stopwatch.GetTimestamp() < end) {
                for (var e = 0; e < batchSize; e++)
                    batch[e] = new Entry(index + (ulong)e, 1, EntryKind.Command, EntryFlags.None, payload);

                var start = Stopwatch.GetTimestamp();
                var written = log.AppendAsync(batch).AsTask().GetAwaiter().GetResult();
                if (count == measured.Length)
                    Array.Resize(ref measured, measured.Length * 2);

                measured[count++] = Stopwatch.GetTimestamp() - start;
                if (written == 0)
                    break;

                // The index advances by what was written: a refused entry leaves no gap.
                index += (ulong)written;
                entries += written;
                appends++;
            }

            results[slot] = (entries, appends, measured[..count]);
        }) { IsBackground = true };
        threads[i].Start();
    }

    foreach (var thread in threads)
        thread.Join();

    foreach (var log in logs)
        log.Dispose();

    long totalEntries = 0;
    long totalAppends = 0;
    var all = new List<long>();
    foreach (var (entries, appends, threadDurations) in results) {
        totalEntries += entries;
        totalAppends += appends;
        all.AddRange(threadDurations);
    }

    var sorted = all.ToArray();
    Array.Sort(sorted);

    var window = (double)seconds;
    var entriesPerSecond = totalEntries / window;
    var mbPerSecond = totalEntries * (double)recordLength / window / (1024 * 1024);
    var appendsPerSecond = totalAppends / window;

    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"## logs={logCount} batch={batchSize} payload={payload.Length}"));
    Console.WriteLine();
    Console.WriteLine("| entries/s | MB/s | appends/s | p50 | p99 | max |");
    Console.WriteLine("| ---: | ---: | ---: | ---: | ---: | ---: |");
    Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"| {entriesPerSecond:N0} | {mbPerSecond:N1} | {appendsPerSecond:N0} | {Percentile(sorted, 0.50):N0} us | {Percentile(sorted, 0.99):N0} us | {Max(sorted):N0} us |"));
    Console.WriteLine();

    Delete(root);
}

static double Percentile(long[] sorted, double fraction) {
    if (sorted.Length == 0)
        return 0;

    return TicksToUs(sorted[(long)(fraction * (sorted.Length - 1))]);
}

static double Max(long[] sorted) =>
    sorted.Length == 0 ? 0 : TicksToUs(sorted[^1]);

static double TicksToUs(long ticks) =>
    ticks * 1_000_000.0 / Stopwatch.Frequency;

static void Delete(string path) {
    try {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    } catch (IOException) {
        // A file still flushing must not fail the run.
    }
}

/// <summary>
/// A log that counts its appends: the leader's append count over the measured window
/// shows the real batch size.
/// </summary>
sealed class CountingLog(IRaftLog inner) : IRaftLog {
    long _appends;

    /// <summary>
    /// The number of appends so far.
    /// </summary>
    public long Appends => Interlocked.Read(ref _appends);

    /// <inheritdoc/>
    public ulong FirstIndex => inner.FirstIndex;

    /// <inheritdoc/>
    public ulong LastIndex => inner.LastIndex;

    /// <inheritdoc/>
    public bool IsFaulted => inner.IsFaulted;

    /// <inheritdoc/>
    public ulong GetTerm(ulong index) => inner.GetTerm(index);

    /// <inheritdoc/>
    public int GetRecordLength(ulong index) => inner.GetRecordLength(index);

    /// <inheritdoc/>
    public void TruncateFrom(ulong index) => inner.TruncateFrom(index);

    /// <inheritdoc/>
    public ValueTask<Entry> ReadAsync(ulong index, Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.ReadAsync(index, buffer, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> ReadAsync(ulong firstIndex, ulong lastIndex, Memory<byte> buffer, Memory<Entry> entries, CancellationToken cancellationToken = default) =>
        inner.ReadAsync(firstIndex, lastIndex, buffer, entries, cancellationToken);

    /// <summary>
    /// Appends through the inner log and counts the append.
    /// </summary>
    public async ValueTask<int> AppendAsync(ReadOnlyMemory<Entry> entries) {
        Interlocked.Increment(ref _appends);
        return await inner.AppendAsync(entries);
    }
}
