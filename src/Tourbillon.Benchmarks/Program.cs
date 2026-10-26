using System.Diagnostics;
using System.Globalization;
using Tourbillon;
using Tourbillon.Log;

// Measures the log under concurrent appends: per combination of log count and batch
// size, every log appends batches back to back on a dedicated thread, and the run
// reports throughput and per-append latency percentiles as a Markdown table.
var directory = ".";
var logCounts = new[] { 1, 16, 64 };
var batchSizes = new[] { 1, 10, 100, 1000 };
var payloadSize = 256;
var seconds = 10;
var warmupSeconds = 2;

for (var i = 0; i < args.Length; i++) {
    switch (args[i]) {
        case "--dir":
            directory = args[++i];
            break;
        case "--logs":
            logCounts = ParseList(args[++i]);
            break;
        case "--batch":
            batchSizes = ParseList(args[++i]);
            break;
        case "--payload":
            payloadSize = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--seconds":
            seconds = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--warmup":
            warmupSeconds = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        default:
            throw new ArgumentException($"Unknown argument '{args[i]}'.");
    }
}

Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"dir={directory} payload={payloadSize} seconds={seconds} warmup={warmupSeconds}"));
Console.WriteLine();

var payload = new byte[payloadSize];
var recordLength = RecordFormat.GetLength(payloadSize);

foreach (var logCount in logCounts)
    foreach (var batchSize in batchSizes)
        RunCombination(directory, logCount, batchSize, payload, recordLength, warmupSeconds, seconds);

static int[] ParseList(string value) =>
    value.Split(',').Select(int.Parse).ToArray();

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
        logs[i] = SegmentedLog.OpenAsync(Path.Combine(root, $"log-{i:D3}"), SegmentSize, storage).AsTask().GetAwaiter().GetResult();

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
