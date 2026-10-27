using AwesomeAssertions;

using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class FaultRunTests {
    public static IEnumerable<object[]> Seeds() {
        for (var seed = 101; seed <= 200; seed++)
            yield return new object[] { seed };
    }

    public static IEnumerable<object[]> DiskSeeds() {
        for (var seed = 101; seed <= 200; seed++)
            yield return new object[] { seed };
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task A_fault_run_never_violates_an_invariant(int seed) {
        var summary = await FaultRun.RunAsync(seed, new FaultRunOptions());

        summary.Committed.Should().BeGreaterThan(0, "the run proposes, and recovers at the end");
    }

    [Theory]
    [MemberData(nameof(DiskSeeds))]
    public async Task A_fault_run_on_the_log_on_disk_never_violates_an_invariant(int seed) {
        var summary = await FaultRun.RunAsync(seed, new FaultRunOptions { Disk = true });

        summary.Committed.Should().BeGreaterThan(0, "the run proposes, and recovers at the end");
    }

    [Fact]
    public async Task The_same_seed_twice_gives_the_same_trace_hash() {
        var first = await FaultRun.RunAsync(101, new FaultRunOptions());
        var second = await FaultRun.RunAsync(101, new FaultRunOptions());

        first.TraceHash.Should().Be(second.TraceHash);
    }

    [Fact]
    public async Task The_same_seed_twice_gives_the_same_trace_hash_on_disk() {
        var first = await FaultRun.RunAsync(101, new FaultRunOptions { Disk = true });
        var second = await FaultRun.RunAsync(101, new FaultRunOptions { Disk = true });

        first.TraceHash.Should().Be(second.TraceHash);
    }

    [Fact]
    public async Task Two_seeds_give_different_trace_hashes() {
        var first = await FaultRun.RunAsync(101, new FaultRunOptions());
        var second = await FaultRun.RunAsync(102, new FaultRunOptions());

        first.TraceHash.Should().NotBe(second.TraceHash);
    }

    [Fact]
    public void The_checker_catches_two_leaders_in_one_term() {
        var log = ClusterRuns.Entries(2, 5);
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 5, "machine-1", 2, log, log),
            new MachineState("machine-2", true, ReplicaRole.Leader, 5, "machine-2", 2, log, log),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-1", 2, log, log),
        };

        var checker = new InvariantChecker();
        Action check = () => checker.Assert(states);

        check.Should().Throw<InvariantViolationException>();
    }

    [Fact]
    public void The_checker_exempts_a_stale_leader_that_misses_an_entry_committed_in_a_later_term() {
        // Entry 2 of term 2 is committed by the leader of term 5; machine-1 still leads
        // term 4 and has its own entry 2 there.
        var committed = CommittedLog();
        var stale = new[] { committed[0], new Entry(2, 4, EntryKind.NoOp, EntryFlags.None, ReadOnlyMemory<byte>.Empty) };
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 4, "machine-1", 1, stale, new[] { committed[0] }),
            new MachineState("machine-2", true, ReplicaRole.Follower, 5, "machine-3", 2, committed, committed),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-3", 2, committed, committed),
        };

        var checker = new InvariantChecker();
        Action check = () => checker.Assert(states);

        check.Should().NotThrow();
    }

    [Fact]
    public void The_checker_catches_a_leader_of_a_later_term_that_misses_a_committed_entry() {
        var committed = CommittedLog();
        var checker = new InvariantChecker();
        checker.Assert(new[] {
            new MachineState("machine-1", true, ReplicaRole.Follower, 5, "machine-3", 1, committed.Take(1).ToArray(), new[] { committed[0] }),
            new MachineState("machine-2", true, ReplicaRole.Follower, 5, "machine-3", 2, committed, committed),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-3", 2, committed, committed),
        });

        var missing = new[] { committed[0], new Entry(2, 6, EntryKind.NoOp, EntryFlags.None, ReadOnlyMemory<byte>.Empty) };
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 6, "machine-1", 1, missing, new[] { committed[0] }),
            new MachineState("machine-2", true, ReplicaRole.Follower, 6, "machine-1", 2, committed, committed),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-3", 2, committed, committed),
        };
        Action check = () => checker.Assert(states);

        check.Should().Throw<InvariantViolationException>();
    }

    // Entry 1 of term 1 and entry 2 of term 2.
    static Entry[] CommittedLog() => new[] {
        new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
        new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
    };

    [Fact]
    public void The_checker_catches_a_refused_payload_in_a_log() {
        var payload = new byte[] { 9, 9, 9 };
        var log = ClusterRuns.Entries(2, 5).ToList();
        log.Add(new Entry(3, 5, EntryKind.Command, EntryFlags.None, payload));
        var applied = ClusterRuns.Entries(2, 5);
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 5, "machine-1", 2, log, applied),
            new MachineState("machine-2", true, ReplicaRole.Follower, 5, "machine-1", 2, ClusterRuns.Entries(2, 5), applied),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-1", 2, ClusterRuns.Entries(2, 5), applied),
        };

        var checker = new InvariantChecker();
        checker.NoteRefusedProposal(payload);
        Action check = () => checker.Assert(states);

        check.Should().Throw<InvariantViolationException>();
    }

    [Fact]
    public void The_checker_catches_two_machines_that_applied_different_entries_at_one_index() {
        var committed = CommittedLog();
        var other = new[] { committed[0], new Entry(2, 3, EntryKind.Command, EntryFlags.None, new byte[] { 9, 9 }) };
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 5, "machine-1", 2, committed, committed),
            new MachineState("machine-2", true, ReplicaRole.Follower, 5, "machine-1", 2, committed, other),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-1", 2, committed, committed),
        };

        var checker = new InvariantChecker();
        Action check = () => checker.Assert(states);

        check.Should().Throw<InvariantViolationException>();
    }

    [Fact]
    public void The_checker_catches_a_feed_that_skips_an_index_after_a_restart() {
        var committed = ClusterRuns.Entries(4, 5);
        var skipped = new[] { committed[0], committed[1], committed[3] };  // 1, 2, 4: the feed skipped 3
        var states = new[] {
            new MachineState("machine-1", true, ReplicaRole.Leader, 5, "machine-1", 4, committed, skipped),
            new MachineState("machine-2", true, ReplicaRole.Follower, 5, "machine-1", 4, committed, committed),
            new MachineState("machine-3", true, ReplicaRole.Follower, 5, "machine-1", 4, committed, committed),
        };

        var checker = new InvariantChecker();
        Action check = () => checker.Assert(states);

        check.Should().Throw<InvariantViolationException>();
    }
}
