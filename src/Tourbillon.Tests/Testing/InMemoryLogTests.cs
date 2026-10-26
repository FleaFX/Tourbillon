using AwesomeAssertions;
using Tourbillon.Log;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

public class InMemoryLogTests {
    static Entry MakeEntry(ulong index, ulong term = 1, EntryKind kind = EntryKind.Command, EntryFlags flags = EntryFlags.None, byte[]? payload = null) =>
        new(index, term, kind, flags, payload ?? new byte[] { (byte)index });

    static Entry[] MakeEntries(int count, EntryFlags flags = EntryFlags.None, ulong firstIndex = 1) {
        var entries = new Entry[count];
        for (var i = 0; i < count; i++)
            entries[i] = MakeEntry(firstIndex + (ulong)i, flags: flags);

        return entries;
    }

    [Fact]
    public async Task Appended_entries_read_back() {
        var log = new InMemoryLog();
        var entries = new[] { MakeEntry(1), MakeEntry(2, term: 2), MakeEntry(3, term: 2, EntryKind.NoOp) };

        var count = await log.AppendAsync(entries);

        count.Should().Be(3);
        log.FirstIndex.Should().Be(1UL);
        log.LastIndex.Should().Be(3UL);
        log.GetTerm(1).Should().Be(1UL);
        log.GetTerm(2).Should().Be(2UL);
        log.GetRecordLength(2).Should().Be(RecordFormat.GetLength(1));

        var buffer = new byte[log.GetRecordLength(2)];
        var read = await log.ReadAsync(2, buffer, TestContext.Current.CancellationToken);

        read.Index.Should().Be(2UL);
        read.Term.Should().Be(2UL);
        read.Kind.Should().Be(EntryKind.Command);
        read.Flags.Should().Be(EntryFlags.None);
        read.Payload.ToArray().Should().Equal(new byte[] { 2 });
    }

    [Fact]
    public async Task Appends_copy_the_payloads() {
        var log = new InMemoryLog();
        var payload = new byte[] { 1, 2, 3 };

        await log.AppendAsync(new[] { MakeEntry(1, payload: payload) });

        payload[0] = 99;

        var buffer = new byte[log.GetRecordLength(1)];
        var read = await log.ReadAsync(1, buffer, TestContext.Current.CancellationToken);

        read.Payload.ToArray().Should().Equal(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void The_place_before_the_first_entry_has_term_zero() {
        var log = new InMemoryLog();

        log.GetTerm(0).Should().Be(0UL);
    }

    [Fact]
    public void GetTerm_throws_beyond_the_last_entry() {
        var log = new InMemoryLog();

        var getTerm = () => log.GetTerm(1);

        getTerm.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void ReadAsync_throws_beyond_the_last_entry() {
        var log = new InMemoryLog();

        var read = () => log.ReadAsync(1, new byte[RecordFormat.GetLength(1)], TestContext.Current.CancellationToken);

        read.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task ReadAsync_throws_when_the_buffer_is_too_short() {
        var log = new InMemoryLog();
        await log.AppendAsync(new[] { MakeEntry(1, payload: new byte[10]) });

        var read = () => log.ReadAsync(1, new byte[RecordFormat.GetLength(10) - 1], TestContext.Current.CancellationToken);

        read.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task TruncateFrom_removes_the_entries_and_gives_their_bytes_back() {
        // A maximum of one record: after a truncate the next append fits again.
        var log = new InMemoryLog(maximumSize: RecordFormat.GetLength(1), reserveSize: 0);

        (await log.AppendAsync(new[] { MakeEntry(1) })).Should().Be(1);
        (await log.AppendAsync(new[] { MakeEntry(2) })).Should().Be(0);

        log.TruncateFrom(1);

        (await log.AppendAsync(new[] { MakeEntry(1) })).Should().Be(1);
        log.LastIndex.Should().Be(1UL);
        log.GetTerm(1).Should().Be(1UL);
    }

    [Fact]
    public async Task TruncateFrom_at_the_end_changes_nothing() {
        var log = new InMemoryLog();
        await log.AppendAsync(new[] { MakeEntry(1), MakeEntry(2) });

        log.TruncateFrom(3);

        log.LastIndex.Should().Be(2UL);
    }

    [Fact]
    public void TruncateFrom_throws_outside_the_log() {
        var log = new InMemoryLog();

        var below = () => log.TruncateFrom(0);
        var above = () => log.TruncateFrom(2);

        below.Should().Throw<ArgumentOutOfRangeException>();
        above.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Unflagged_entries_stop_at_the_reserve() {
        // A record of one payload byte takes 28 bytes; the room is 100 bytes, the
        // reserve 20, so unflagged entries stay within 80 bytes: two of them.
        var log = new InMemoryLog(maximumSize: 100, reserveSize: 20);

        var count = await log.AppendAsync(MakeEntries(5));

        count.Should().Be(2);
        log.LastIndex.Should().Be(2UL);
    }

    [Fact]
    public async Task Flagged_entries_use_the_reserve() {
        // Flagged entries stay within the 100 bytes: three records of 28 bytes.
        var log = new InMemoryLog(maximumSize: 100, reserveSize: 20);

        var count = await log.AppendAsync(MakeEntries(5, EntryFlags.MayUseReserve));

        count.Should().Be(3);
        log.LastIndex.Should().Be(3UL);
    }

    [Fact]
    public async Task A_batch_appends_its_start_and_refuses_the_rest() {
        // The room is 100 bytes: three records of 28 bytes fit, the fourth does not
        // and touches nothing.
        var log = new InMemoryLog(maximumSize: 100, reserveSize: 0);

        var count = await log.AppendAsync(MakeEntries(4));

        count.Should().Be(3);
        log.LastIndex.Should().Be(3UL);

        var getTerm = () => log.GetTerm(4);
        getTerm.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Nothing_fits_when_there_is_no_free_space() {
        var log = new InMemoryLog();
        log.FreeSpace = 0;

        var count = await log.AppendAsync(MakeEntries(1, EntryFlags.MayUseReserve));

        count.Should().Be(0);
        log.LastIndex.Should().Be(0UL);
    }

    [Fact]
    public async Task The_gate_holds_an_append_until_it_opens() {
        var log = new InMemoryLog();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        log.AppendGate = gate;

        var append = log.AppendAsync(new[] { MakeEntry(1) }).AsTask();

        append.IsCompleted.Should().BeFalse();

        gate.SetResult();

        (await append).Should().Be(1);
        log.LastIndex.Should().Be(1UL);
    }

    [Fact]
    public async Task AppendCount_counts_the_appends() {
        var log = new InMemoryLog();

        log.AppendCount.Should().Be(0);

        await log.AppendAsync(MakeEntries(2));
        log.AppendCount.Should().Be(1);

        await log.AppendAsync(MakeEntries(1, firstIndex: 3));
        log.AppendCount.Should().Be(2);
    }
}
