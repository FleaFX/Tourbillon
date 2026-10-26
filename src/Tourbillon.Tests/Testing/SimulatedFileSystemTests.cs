using AwesomeAssertions;
using Tourbillon.Log;

namespace Tourbillon.Testing;

public class SimulatedFileSystemTests {
    static byte[] Bytes(int start, int length) {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++)
            bytes[i] = (byte)(start + i);

        return bytes;
    }

    static async Task<byte[]> ReadAllAsync(IFileSystem fs, string path, CancellationToken cancellationToken) {
        using var file = fs.OpenFile(path);
        var bytes = new byte[file.Length];
        var read = 0;
        while (read < bytes.Length) {
            var readNow = await file.ReadAsync(bytes.AsMemory(read), read, cancellationToken);
            if (readNow == 0)
                break;

            read += readNow;
        }

        return bytes[..read];
    }

    static byte[] MakePayload(ulong index, int length) {
        var payload = new byte[length];
        for (var i = 0; i < length; i++)
            payload[i] = (byte)(index * 31 + (ulong)i * 7 + 3);

        return payload;
    }

    static Entry MakeEntry(ulong index, int payloadLength = 100) =>
        new(index, 1, EntryKind.Command, EntryFlags.None, MakePayload(index, payloadLength));

    [Fact]
    public async Task Flushed_bytes_survive_a_crash_and_unflushed_ones_are_kept_or_lost_whole() {
        var sawKept = 0;
        var sawLost = 0;
        for (var seed = 1; seed <= 50; seed++) {
            var fs = new SimulatedFileSystem(new Random(seed));
            using (var file = fs.OpenFile("/logs/a")) {
                await file.WriteAsync(Bytes(0, 100), 0);
                file.FlushToDisk();
                fs.FlushDirectory("/logs");
                await file.WriteAsync(Bytes(100, 100), 100);
            }

            fs.Crash(new Random(seed));

            using (var file = fs.OpenFile("/logs/a")) {
                // The flushed bytes survive; the unflushed ones are kept or lost
                // whole, per their sector: the length is the durable one or the
                // current one, never in between.
                (file.Length == 100 || file.Length == 200).Should().BeTrue();
                var buffer = new byte[100];
                await file.ReadAsync(buffer, 0, TestContext.Current.CancellationToken);
                buffer.Should().Equal(Bytes(0, 100));
                if (file.Length == 200)
                    sawKept++;
                else
                    sawLost++;
            }
        }

        sawKept.Should().BeGreaterThan(0);
        sawLost.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task A_seed_that_keeps_every_sector_loses_nothing_and_a_lost_sector_before_a_kept_one_reads_as_zeros() {
        var keptEverySector = 0;
        var lostBeforeKept = 0;
        for (var seed = 1; seed <= 200; seed++) {
            var fs = new SimulatedFileSystem(new Random(seed));
            using (var file = fs.OpenFile("/logs/a")) {
                await file.WriteAsync(Bytes(0, 512), 0);
                file.FlushToDisk();
                fs.FlushDirectory("/logs");
                await file.WriteAsync(Bytes(512, 600), 512);
            }

            fs.Crash(new Random(seed));

            var read = await ReadAllAsync(fs, "/logs/a", TestContext.Current.CancellationToken);
            if (read.Length == 1112 && read.AsSpan(512).SequenceEqual(Bytes(512, 600))) {
                keptEverySector++;
                continue;
            }

            // A lost sector before a kept one reads as zeros.
            if (read.Length == 1112
                && read.AsSpan(512, 512).SequenceEqual(new byte[512])
                && read.AsSpan(1024).SequenceEqual(Bytes(1024, 88)))
                lostBeforeKept++;
        }

        keptEverySector.Should().BeGreaterThan(0);
        lostBeforeKept.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task An_unflushed_truncate_create_delete_or_rename_is_applied_or_not_never_half() {
        var sawApplied = 0;
        var sawReverted = 0;
        for (var seed = 1; seed <= 100; seed++) {
            // A truncate.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                    fs.FlushDirectory("/logs");
                    file.SetLength(10);
                }

                fs.Crash(new Random(seed));

                using (var file = fs.OpenFile("/logs/a")) {
                    (file.Length == 100 || file.Length == 10).Should().BeTrue();
                    var buffer = new byte[file.Length];
                    await file.ReadAsync(buffer, 0, TestContext.Current.CancellationToken);
                    buffer.Should().Equal(Bytes(0, (int)file.Length));
                    if (file.Length == 10)
                        sawApplied++;
                    else
                        sawReverted++;
                }
            }

            // A create.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/new")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.Crash(new Random(seed));

                if (fs.FileExists("/logs/new")) {
                    var read = await ReadAllAsync(fs, "/logs/new", TestContext.Current.CancellationToken);
                    read.Should().Equal(Bytes(0, 100));
                    sawApplied++;
                } else {
                    sawReverted++;
                }
            }

            // A delete.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.FlushDirectory("/logs");
                fs.DeleteFile("/logs/a");
                fs.Crash(new Random(seed));

                if (fs.FileExists("/logs/a")) {
                    var read = await ReadAllAsync(fs, "/logs/a", TestContext.Current.CancellationToken);
                    read.Should().Equal(Bytes(0, 100));
                    sawReverted++;
                } else {
                    sawApplied++;
                }
            }

            // A rename.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.FlushDirectory("/logs");
                fs.MoveFile("/logs/a", "/logs/b");
                fs.Crash(new Random(seed));

                var aExists = fs.FileExists("/logs/a");
                var bExists = fs.FileExists("/logs/b");
                (aExists ^ bExists).Should().BeTrue();
                if (bExists) {
                    var read = await ReadAllAsync(fs, "/logs/b", TestContext.Current.CancellationToken);
                    read.Should().Equal(Bytes(0, 100));
                    sawApplied++;
                } else {
                    var read = await ReadAllAsync(fs, "/logs/a", TestContext.Current.CancellationToken);
                    read.Should().Equal(Bytes(0, 100));
                    sawReverted++;
                }
            }
        }

        sawApplied.Should().BeGreaterThan(0);
        sawReverted.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task A_directory_flush_makes_a_create_a_delete_and_a_rename_survive_a_crash() {
        for (var seed = 1; seed <= 50; seed++) {
            // A create.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.FlushDirectory("/logs");
                fs.Crash(new Random(seed));

                var read = await ReadAllAsync(fs, "/logs/a", TestContext.Current.CancellationToken);
                read.Should().Equal(Bytes(0, 100));
            }

            // A delete.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.FlushDirectory("/logs");
                fs.DeleteFile("/logs/a");
                fs.FlushDirectory("/logs");
                fs.Crash(new Random(seed));

                fs.FileExists("/logs/a").Should().BeFalse();
            }

            // A rename.
            {
                var fs = new SimulatedFileSystem(new Random(seed));
                using (var file = fs.OpenFile("/logs/a")) {
                    await file.WriteAsync(Bytes(0, 100), 0);
                    file.FlushToDisk();
                }

                fs.FlushDirectory("/logs");
                fs.MoveFile("/logs/a", "/logs/b");
                fs.FlushDirectory("/logs");
                fs.Crash(new Random(seed));

                fs.FileExists("/logs/a").Should().BeFalse();
                var read = await ReadAllAsync(fs, "/logs/b", TestContext.Current.CancellationToken);
                read.Should().Equal(Bytes(0, 100));
            }
        }
    }

    [Fact]
    public async Task CrashAfter_crashes_on_the_nth_operation_and_every_later_call_throws_until_restart() {
        var fs = new SimulatedFileSystem(new Random(1));
        using var file = fs.OpenFile("/logs/a");
        fs.CrashAfter(3);

        await file.WriteAsync(Bytes(0, 10), 0);
        await file.WriteAsync(Bytes(10, 10), 10);
        var third = async () => await file.WriteAsync(Bytes(100, 10), 20);
        await third.Should().ThrowAsync<SimulatedCrashException>();
        fs.IsCrashed.Should().BeTrue();

        // Every later call throws, until the machine restarts.
        var writeAgain = async () => await file.WriteAsync(Bytes(200, 10), 30);
        await writeAgain.Should().ThrowAsync<SimulatedCrashException>();
        var open = () => fs.OpenFile("/logs/b");
        open.Should().ThrowExactly<SimulatedCrashException>();
        var exists = () => fs.FileExists("/logs/a");
        exists.Should().ThrowExactly<SimulatedCrashException>();

        fs.Restart();
        fs.IsCrashed.Should().BeFalse();

        // The crash happened before the third operation: its bytes are not in the
        // file, and the file system answers again.
        file.Length.Should().BeLessThanOrEqualTo(20);
        await file.WriteAsync(Bytes(200, 10), 30);
        file.Length.Should().Be(40);
    }

    [Fact]
    public async Task The_log_recovers_after_a_crash_during_its_last_append() {
        const int flushedBatches = 3;
        const int batchSize = 5;
        const long segmentSize = 64 * 1024;

        for (var seed = 1; seed <= 200; seed++) {
            foreach (var crashAfter in new[] { 1, 2 }) {
                var fs = new SimulatedFileSystem(new Random(seed));
                var storage = new StorageLimit(() => fs.FreeSpace, maximumSize: null, reserveSize: 0);

                var expected = new List<Entry>();
                var log = await SegmentedLog.OpenAsync(fs, "/logs", segmentSize, storage, TestContext.Current.CancellationToken);
                for (var batch = 0; batch < flushedBatches; batch++) {
                    var entries = new Entry[batchSize];
                    for (var i = 0; i < batchSize; i++)
                        entries[i] = MakeEntry((ulong)(batch * batchSize + i + 1));

                    expected.AddRange(entries);
                    await log.AppendAsync(entries);
                }

                var flushed = log.LastIndex;

                // The last append: a crash on its write (1) or on its fsync (2).
                var lastBatch = new Entry[batchSize];
                for (var i = 0; i < batchSize; i++)
                    lastBatch[i] = MakeEntry(flushed + 1 + (ulong)i);

                expected.AddRange(lastBatch);
                fs.CrashAfter(crashAfter);
                var append = async () => await log.AppendAsync(lastBatch);
                await append.Should().ThrowAsync<IOException>();
                fs.Restart();
                log.Dispose();

                using (var recovered = await SegmentedLog.OpenAsync(fs, "/logs", segmentSize, storage, TestContext.Current.CancellationToken)) {
                    // Exactly the entries of the flushed batches, possibly plus the
                    // last batch's whole entries before a torn one, never a partial
                    // entry.
                    recovered.FirstIndex.Should().Be(1UL);
                    recovered.LastIndex.Should().BeInRange(flushed, flushed + (ulong)batchSize);
                    if (crashAfter == 1)
                        recovered.LastIndex.Should().Be(flushed);

                    for (var index = 1UL; index <= recovered.LastIndex; index++) {
                        var wanted = expected[(int)index - 1];
                        recovered.GetTerm(index).Should().Be(wanted.Term);
                        var buffer = new byte[recovered.GetRecordLength(index)];
                        var entry = await recovered.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                        entry.Index.Should().Be(wanted.Index);
                        entry.Term.Should().Be(wanted.Term);
                        entry.Kind.Should().Be(wanted.Kind);
                        entry.Flags.Should().Be(wanted.Flags);
                        entry.Payload.ToArray().Should().Equal(wanted.Payload.ToArray());
                    }
                }
            }
        }
    }

    [Fact]
    public async Task A_crash_at_every_operation_of_a_state_file_write_leaves_the_old_or_the_new_content() {
        var oldContent = new byte[] { 1, 2, 3, 4, 5 };
        var newContent = new byte[] { 6, 7, 8, 9 };

        var sawOld = 0;
        var sawNew = 0;
        var sawNone = 0;
        for (var seed = 1; seed <= 100; seed++) {
            for (var crashAfter = 1; crashAfter <= 6; crashAfter++) {
                // A write over an old version.
                {
                    var fs = new SimulatedFileSystem(new Random(seed));
                    await StateFile.WriteAsync(fs, "/logs", "vote.state", oldContent, TestContext.Current.CancellationToken);
                    fs.CrashAfter(crashAfter);
                    var write = async () => await StateFile.WriteAsync(fs, "/logs", "vote.state", newContent, TestContext.Current.CancellationToken);
                    await write.Should().ThrowAsync<IOException>();
                    fs.Restart();

                    var read = await StateFile.ReadAsync(fs, "/logs", "vote.state", TestContext.Current.CancellationToken);
                    (read is null || read.AsSpan().SequenceEqual(oldContent) || read.AsSpan().SequenceEqual(newContent)).Should().BeTrue();
                    if (read is null || read.AsSpan().SequenceEqual(oldContent))
                        sawOld++;
                    else
                        sawNew++;
                }

                // A first write: no old version.
                {
                    var fs = new SimulatedFileSystem(new Random(seed));
                    fs.CrashAfter(crashAfter);
                    var write = async () => await StateFile.WriteAsync(fs, "/logs", "vote.state", newContent, TestContext.Current.CancellationToken);
                    await write.Should().ThrowAsync<IOException>();
                    fs.Restart();

                    var read = await StateFile.ReadAsync(fs, "/logs", "vote.state", TestContext.Current.CancellationToken);
                    (read is null || read.AsSpan().SequenceEqual(newContent)).Should().BeTrue();
                    if (read is null)
                        sawNone++;
                    else
                        sawNew++;
                }
            }
        }

        sawOld.Should().BeGreaterThan(0);
        sawNew.Should().BeGreaterThan(0);
        sawNone.Should().BeGreaterThan(0);
    }
}
