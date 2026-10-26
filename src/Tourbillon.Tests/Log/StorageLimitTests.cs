using AwesomeAssertions;

namespace Tourbillon.Log;

public class StorageLimitTests {
    // The worked example of the design: a maximum of 100 KB, a reserve of 20 KB,
    // 75 KB already used, entries of 2 KB (2075 bytes a record) and plenty of free
    // disk, so the room is 100 KB - 75 KB = 25 KB.
    const long Maximum = 100 * 1024;
    const long Reserve = 20 * 1024;
    const int PayloadLength = 2048;

    static Entry MakeEntry(EntryFlags flags = EntryFlags.None, ulong index = 1) =>
        new(index, 1, EntryKind.Command, flags, new byte[PayloadLength]);

    static Entry[] MakeEntries(int count, EntryFlags flags = EntryFlags.None, ulong firstIndex = 1) {
        var entries = new Entry[count];
        for (var i = 0; i < count; i++)
            entries[i] = MakeEntry(flags, firstIndex + (ulong)i);

        return entries;
    }

    static StorageLimit MakeStorage(long used) {
        var storage = new StorageLimit(() => long.MaxValue, Maximum, Reserve);
        storage.Add(used);
        return storage;
    }

    [Fact]
    public void Unflagged_entries_stop_at_the_reserve() {
        var storage = MakeStorage(75 * 1024);

        var count = storage.TryTake(MakeEntries(5), out var bytes);

        // Each fits while the bytes of the batch stay within 25 KB - 20 KB = 5 KB.
        count.Should().Be(2);
        bytes.Should().Be(2 * RecordFormat.GetLength(PayloadLength));
        storage.Used.Should().Be(75 * 1024);
    }

    [Fact]
    public void Flagged_entries_use_the_reserve() {
        var storage = MakeStorage(75 * 1024);

        var count = storage.TryTake(MakeEntries(20, EntryFlags.MayUseReserve), out var bytes);

        // Each fits while the bytes of the batch stay within 25 KB.
        count.Should().Be(12);
        bytes.Should().Be(12 * RecordFormat.GetLength(PayloadLength));
    }

    [Fact]
    public void A_mixed_batch_stops_at_the_first_that_does_not_fit() {
        var storage = MakeStorage(79 * 1024);

        var batch = new[] { MakeEntry(EntryFlags.MayUseReserve), MakeEntry(), MakeEntry(EntryFlags.MayUseReserve) };

        var count = storage.TryTake(batch, out _);

        // The room is 21 KB: the flagged first entry fits, the unflagged second does
        // not (4 KB > 1 KB), and the third is never looked at.
        count.Should().Be(1);
    }

    [Fact]
    public void Settle_counts_written_bytes_as_used_and_gives_back_unwritten() {
        var storage = new StorageLimit(() => long.MaxValue, Maximum, Reserve);

        var count = storage.TryTake(MakeEntries(2), out var bytes);
        count.Should().Be(2);
        storage.Used.Should().Be(0);

        storage.Settle(bytes, written: true);
        storage.Used.Should().Be(bytes);

        count = storage.TryTake(MakeEntries(2), out var refused);
        storage.Settle(refused, written: false);
        storage.Used.Should().Be(bytes);
    }

    [Fact]
    public void Add_and_Release_move_Used() {
        var storage = new StorageLimit(() => long.MaxValue, maximumSize: null, reserveSize: 0);

        storage.Add(100);
        storage.Used.Should().Be(100);
        storage.Release(40);
        storage.Used.Should().Be(60);
    }

    [Fact]
    public void Concurrent_takes_never_pass_the_limit() {
        const int maximum = 100 * 1024;
        var storage = new StorageLimit(() => long.MaxValue, maximum, reserveSize: 0);
        var violations = new int[1];

        var threads = new Thread[8];
        for (var t = 0; t < threads.Length; t++) {
            threads[t] = new Thread(() => {
                for (var i = 0; i < 2000; i++) {
                    var count = storage.TryTake(MakeEntries(3, EntryFlags.MayUseReserve), out var bytes);
                    if (count == 0) {
                        storage.Settle(bytes, written: false);
                        continue;
                    }

                    // The bytes this take holds are in flight: used plus in flight
                    // never passes the maximum.
                    if (storage.Used + bytes > maximum)
                        Interlocked.Increment(ref violations[0]);

                    storage.Settle(bytes, written: true);
                }
            });
            threads[t].Start();
        }

        foreach (var thread in threads)
            thread.Join();

        violations[0].Should().Be(0);
        storage.Used.Should().BeLessThanOrEqualTo(maximum);
        storage.Used.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Measures_the_real_disk() {
        using var directory = new TempDirectory();
        var storage = new StorageLimit(directory.Path, maximumSize: null, reserveSize: 0);

        var count = storage.TryTake(MakeEntries(1), out var bytes);

        count.Should().Be(1);
        bytes.Should().Be(RecordFormat.GetLength(PayloadLength));
    }
}
