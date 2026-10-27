using AwesomeAssertions;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class BookExampleTests {
    [Fact]
    public async Task Chapter_4_the_election() {
        // Machine 1 leads in term 4. Entry 800 is on the disks of machines 1 and 2: that
        // is 2 of 3, so it is committed. Machine 3 has only up to entry 799. Machine 1
        // crashes.
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 1, options: ClusterRuns.Reliable);
        await cluster.SeedAsync("machine-1", 4, "machine-1", ClusterRuns.Entries(800, 4));
        await cluster.SeedAsync("machine-2", 4, "machine-1", ClusterRuns.Entries(800, 4));
        await cluster.SeedAsync("machine-3", 4, "machine-1", ClusterRuns.Entries(799, 4));
        await cluster.StartAsync();
        await cluster.CrashAsync("machine-1");

        // Machine 3 times out first and asks in term 5. The trial raises nothing.
        var asked = await ClusterRuns.UntilAsync(cluster,
            () => cluster.Network.Sent.Any(send => send.Message is VoteRequest),
            step: TimeSpan.FromMilliseconds(1));
        asked.Should().BeTrue();
        var first = cluster.Network.Sent.First(send => send.Message is VoteRequest);
        first.From.Should().Be("machine-3");
        first.Message.Should().BeOfType<VoteRequest>().Which.PreVote.Should().BeTrue();
        cluster.Replica("machine-3").Term.Should().Be(4UL, "a trial question raises no term");

        // Machine 2 refuses: its own log has entry 800, the log of machine 3 does not.
        // Machine 3 has only its own vote, 1 of 3, and does not become leader.
        var refused = await ClusterRuns.UntilAsync(cluster,
            () => cluster.Network.Sent.Any(send => send.From == "machine-2" && send.To == "machine-3"
                && send.Message is VoteResponse { PreVote: true, Granted: false }),
            step: TimeSpan.FromMilliseconds(1));
        refused.Should().BeTrue("machine 2 refuses the trial of machine 3");
        cluster.Replica("machine-3").Term.Should().Be(4UL, "a refused trial raises no term");

        // A moment later, machine 2 asks in term 5 — not 6, because machine 3 never
        // raised a term of its own — and machine 3 agrees, since the log of machine 2 is
        // at least as complete as its own. Machine 2 has 2 of 3 votes and leads, with
        // entry 800.
        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) == "machine-2");
        elected.Should().BeTrue();
        cluster.Replica("machine-2").Role.Should().Be(ReplicaRole.Leader);
        cluster.Replica("machine-2").Term.Should().Be(5UL);
        cluster.Log("machine-2").LastIndex.Should().BeGreaterThanOrEqualTo(800UL);
        cluster.Log("machine-2").GetTerm(800).Should().Be(4UL);

        // Entry 800 is committed once the NoOp of term 5 is on machines 2 and 3.
        var committed = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Replica("machine-2").CommitIndex >= 800 && cluster.Replica("machine-3").CommitIndex >= 800);
        committed.Should().BeTrue("entry 800 is committed on machines 2 and 3");

        // Machine 3 voted for machine 2, never for itself: it adopted term 5 with that
        // vote, and never raised a term of its own.
        var (votedTerm, votedFor) = await cluster.Votes("machine-3").LoadAsync(TestContext.Current.CancellationToken);
        votedTerm.Should().Be(5UL);
        votedFor.Should().Be("machine-2");
        cluster.Replica("machine-3").Term.Should().Be(5UL);

        // The vote rule of the example, checked directly: a real vote request of machine
        // 3 for term 5 is refused by machine 2.
        cluster.Replica("machine-2").Receive("machine-3", new VoteRequest(5, 799, 4, PreVote: false));
        await cluster.StepAsync(TimeSpan.FromMilliseconds(20));
        var answer = cluster.Network.Sent.Last(send => send.From == "machine-2" && send.To == "machine-3"
            && send.Message is VoteResponse response && !response.PreVote);
        answer.Message.Should().Be(new VoteResponse(5, false, PreVote: false));
        var (leaderTerm, leaderVote) = await cluster.Votes("machine-2").LoadAsync(TestContext.Current.CancellationToken);
        leaderTerm.Should().Be(5UL);
        leaderVote.Should().Be("machine-2");
    }

    [Fact]
    public async Task Chapter_4_entry_801_replaced() {
        // The book's entry 801 is index 12 here: the log holds entries 1..11 after the
        // NoOp of term 6.
        await using var cluster = await StartWithMachineOneLeadingAsync(seed: 2);

        // Machine 1 leads and writes the entry to its disk. Before it can hand it on, it
        // is cut off from the network.
        cluster.Network.Partition(["machine-2", "machine-3"]);
        var first = cluster.Replica("machine-1").ProposeAsync(new byte[] { 42 }, false, TestContext.Current.CancellationToken);
        var ended = await ClusterRuns.UntilAsync(cluster, () => first.IsCompleted);
        ended.Should().BeTrue("the stranded leader steps down by CheckQuorum");
        (await first).Status.Should().Be(ProposalStatus.Unknown);
        cluster.Log("machine-1").LastIndex.Should().Be(12UL);

        // Machines 2 and 3 choose machine 2 as leader. Machine 2 takes a new proposal,
        // and that one becomes the entry at index 13 in the logs of machines 2 and 3:
        // committed.
        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) == "machine-2");
        elected.Should().BeTrue();
        cluster.Replica("machine-2").Term.Should().Be(7UL);
        var ready = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-2").IsReady);
        ready.Should().BeTrue();
        var second = cluster.Replica("machine-2").ProposeAsync(new byte[] { 43 }, false, TestContext.Current.CancellationToken);
        var committed = await ClusterRuns.UntilAsync(cluster, () => second.IsCompleted);
        committed.Should().BeTrue();
        (await second).Should().Be(ProposalResult.Committed(13));

        // Machine 1 comes back. The leader sees that its entry at index 12 differs, and
        // replaces it with the committed entry.
        cluster.Network.Heal();
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Log("machine-1").LastIndex >= 13 && cluster.Replica("machine-1").CommitIndex >= 13);
        caughtUp.Should().BeTrue();
        cluster.Log("machine-1").GetTerm(12).Should().Be(7UL, "the entry of term 6 is replaced");
        (await PayloadAsync(cluster, "machine-1", 13)).Should().Equal(new byte[] { 43 });
        (await PayloadAsync(cluster, "machine-3", 13)).Should().Equal(new byte[] { 43 });
        cluster.Replica("machine-1").Term.Should().Be(7UL);
        cluster.Replica("machine-3").CommitIndex.Should().BeGreaterThanOrEqualTo(13UL);
    }

    [Fact]
    public async Task Chapter_4_entry_801_committed_after_a_timeout() {
        // The book's entry 801 is index 12 here.
        await using var cluster = await StartWithMachineOneLeadingAsync(seed: 3);
        cluster.Network.Heal();
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () => cluster.Log("machine-3").LastIndex >= 11);
        caughtUp.Should().BeTrue();

        // Machine 1 writes the entry and hands it to machine 2, which stores it. The
        // link 2→1 is cut before machine 1 hears back, and machine 3 is cut off too:
        // machine 1 hears no majority.
        cluster.Network.Cut("machine-1", "machine-3");
        cluster.Network.Cut("machine-2", "machine-1");
        var proposal = cluster.Replica("machine-1").ProposeAsync(new byte[] { 42 }, false, TestContext.Current.CancellationToken);
        var stored = await ClusterRuns.UntilAsync(cluster, () => cluster.Log("machine-2").LastIndex >= 12);
        stored.Should().BeTrue("machine 2 stores the entry");

        // Machine 1 is cut off completely: it steps down, and the application that proposed the
        // entry gets no answer — a timeout.
        cluster.Network.Cut("machine-1", "machine-2");
        var ended = await ClusterRuns.UntilAsync(cluster, () => proposal.IsCompleted);
        ended.Should().BeTrue();
        (await proposal).Status.Should().Be(ProposalStatus.Unknown);

        // Machines 2 and 3 choose machine 2 as leader. Its log has the entry, so that
        // entry stays, and becomes committed after all: the NoOp of term 7 commits it.
        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) == "machine-2");
        elected.Should().BeTrue();
        cluster.Replica("machine-2").Term.Should().Be(7UL);
        var committed = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-2").CommitIndex >= 13);
        committed.Should().BeTrue("the NoOp of term 7 commits the entry of term 6 before it");

        // Machine 1 comes back: the entry stays in its log, and is committed on every
        // replica.
        cluster.Network.Heal();
        var everywhere = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 12));
        everywhere.Should().BeTrue("the entry is committed on every replica");
        foreach (var machine in cluster.Machines)
            (await PayloadAsync(cluster, machine, 12)).Should().Equal(new byte[] { 42 });
    }

    [Fact]
    public async Task Chapter_4_when_a_replica_is_full() {
        await using var cluster = await StartWithMachineOneLeadingAsync(seed: 4);
        cluster.Network.Heal();
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Log(machine).LastIndex >= 11));
        caughtUp.Should().BeTrue();

        // Step 1: the disk of machine 3 is full — 27 bytes over its reserve of 64 KiB,
        // so one unflagged record of a 10-byte payload (37 bytes) does not fit. Machines
        // 1 (the leader) and 2 still have room: the entry is committed, as if machine 3
        // were down.
        cluster.SetFreeSpace("machine-3", 64 * 1024 + 27);
        var first = cluster.Replica("machine-1").ProposeAsync(Payload(1), false, TestContext.Current.CancellationToken);
        var committed = await ClusterRuns.UntilAsync(cluster, () => first.IsCompleted);
        committed.Should().BeTrue();
        (await first).Should().Be(ProposalResult.Committed(12));

        // Step 2: machine 2 is full too — only the leader has room, 1 of 3. The leader
        // writes the next entry, and learns from the answers "full" that no majority has
        // room.
        cluster.SetFreeSpace("machine-2", 64 * 1024 + 27);
        var stuck = cluster.Replica("machine-1").ProposeAsync(Payload(2), false, TestContext.Current.CancellationToken);
        var written = await ClusterRuns.UntilAsync(cluster, () => cluster.Log("machine-1").LastIndex >= 13);
        written.Should().BeTrue();
        var full = await ClusterRuns.UntilAsync(cluster, () => cluster.Network.Sent.Any(send =>
            send.From == "machine-2" && send.To == "machine-1"
            && send.Message is AppendResponse { Result: AppendResult.Full }));
        full.Should().BeTrue();
        await cluster.StepAsync(TimeSpan.FromMilliseconds(200));
        stuck.IsCompleted.Should().BeFalse("no majority has the entry");
        cluster.Log("machine-1").LastIndex.Should().Be(13UL);

        // New entries are refused straight away, with "storage full", before they are
        // written: the refusal is certain, the entry is in no log.
        var refused = cluster.Replica("machine-1").ProposeAsync(Payload(3), false, TestContext.Current.CancellationToken);
        await cluster.StepAsync(TimeSpan.FromMilliseconds(20));
        (await refused).Should().Be(ProposalResult.Refused(RefusalReason.StorageFull));
        cluster.Log("machine-1").LastIndex.Should().Be(13UL);

        // Step 3: an entry that may use the reserve goes through, while a majority has
        // reserve left.
        var flagged = cluster.Replica("machine-1").ProposeAsync(Payload(4), true, TestContext.Current.CancellationToken);
        var through = await ClusterRuns.UntilAsync(cluster, () => cluster.Log("machine-1").LastIndex >= 14);
        through.Should().BeTrue();
        await cluster.StepAsync(TimeSpan.FromMilliseconds(200));
        flagged.IsCompleted.Should().BeFalse("the full followers cannot take it yet");

        // Room comes back on machine 3: the stuck entries are stored, and committed.
        cluster.SetFreeSpace("machine-3", long.MaxValue);
        var recovered = await ClusterRuns.UntilAsync(cluster, () => stuck.IsCompleted && flagged.IsCompleted);
        recovered.Should().BeTrue();
        (await stuck).Should().Be(ProposalResult.Committed(13));
        (await flagged).Should().Be(ProposalResult.Committed(14));
        (await PayloadAsync(cluster, "machine-3", 13)).Should().Equal(Payload(2));
        (await PayloadAsync(cluster, "machine-3", 14)).Should().Equal(Payload(4));
        var committedEverywhere = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Replica("machine-1").CommitIndex >= 14 && cluster.Replica("machine-3").CommitIndex >= 14);
        committedEverywhere.Should().BeTrue("the leader tells machine 3 what it committed");

        // Machine 2 is still full: it has, and commits, nothing beyond its own log.
        cluster.Log("machine-2").LastIndex.Should().Be(12UL);
        cluster.Replica("machine-2").CommitIndex.Should().Be(12UL);
    }

    [Fact]
    public async Task Chapter_5_the_isolated_machine_never_raises_its_term() {
        await using var cluster = await StartWithMachineOneLeadingAsync(seed: 5);
        cluster.Network.Heal();
        var follows = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-3").Term == 6);
        follows.Should().BeTrue("machine 3 follows machine 1 in term 6");

        // Machine 3 is cut off for a minute while machine 1 leads in term 6.
        cluster.Network.Partition(["machine-1", "machine-2"], ["machine-3"]);
        await cluster.StepAsync(TimeSpan.FromSeconds(60));

        cluster.Replica("machine-3").Term.Should().Be(6UL, "an isolated replica never raises its term: its trials find no answer");
        cluster.Replica("machine-1").Role.Should().Be(ReplicaRole.Leader, "machine 1 still hears machine 2");

        // The network comes back — staged, so that machine 3 times out and asks before it
        // hears machine 1 again, the order of the book: machine 2 answers "no".
        cluster.Network.Heal();
        cluster.Network.Cut("machine-1", "machine-3");
        var refused = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Network.Sent.Any(send => send.From == "machine-2" && send.To == "machine-3"
                && send.Message is VoteResponse { PreVote: true, Granted: false }));
        refused.Should().BeTrue("machine 2 answers 'no' to the trial of machine 3");
        cluster.Replica("machine-3").Term.Should().Be(6UL);

        // Machine 1 reaches machine 3 again: machine 3 stays in term 6, and hears
        // machine 1.
        cluster.Network.Heal();
        var heard = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-3").Leader == "machine-1");
        heard.Should().BeTrue();
        cluster.Replica("machine-3").Term.Should().Be(6UL);
        cluster.Replica("machine-3").Role.Should().Be(ReplicaRole.Follower);
        cluster.Replica("machine-1").Role.Should().Be(ReplicaRole.Leader);
    }

    [Fact]
    public async Task Chapter_5_the_half_working_network() {
        await using var cluster = await StartWithMachineOneLeadingAsync(seed: 6);
        cluster.Network.Heal();
        var follows = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-3").Term == 6);
        follows.Should().BeTrue("machine 3 follows machine 1 in term 6");

        // Only the link 1→3 is cut: machine 1 cannot reach machine 3, but machine 3 can
        // reach both others.
        cluster.Network.Cut("machine-1", "machine-3");
        await cluster.StepAsync(TimeSpan.FromSeconds(2));

        var refused = cluster.Network.Sent.Any(send => send.From == "machine-2" && send.To == "machine-3"
            && send.Message is VoteResponse { PreVote: true, Granted: false });
        refused.Should().BeTrue("machine 2 still says 'no' to the trial of machine 3");
        cluster.Replica("machine-1").Term.Should().Be(6UL, "no term rises");
        cluster.Replica("machine-2").Term.Should().Be(6UL, "no term rises");
        cluster.Replica("machine-3").Term.Should().Be(6UL, "no term rises");
        cluster.Replica("machine-1").Role.Should().Be(ReplicaRole.Leader, "machine 1 still hears machine 2");
        cluster.Replica("machine-2").Role.Should().Be(ReplicaRole.Follower);
    }

    // A cluster in which machine 1 deterministically leads term 6: it has the longest
    // log, and machine 3 is cut off from the start, so neither machine 2 nor machine 3
    // can win the first election. The NoOp of term 6 (entry 11) commits on machines 1
    // and 2; machine 3 is still at entry 8.
    static async Task<SimulatedCluster> StartWithMachineOneLeadingAsync(int seed) {
        var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: seed, options: ClusterRuns.Reliable);
        await cluster.SeedAsync("machine-1", 5, "machine-1", ClusterRuns.Entries(10, 5));
        await cluster.SeedAsync("machine-2", 5, "machine-1", ClusterRuns.Entries(9, 5));
        await cluster.SeedAsync("machine-3", 5, "machine-1", ClusterRuns.Entries(8, 5));
        cluster.Network.Partition(["machine-1", "machine-2"], ["machine-3"]);
        await cluster.StartAsync();

        var leads = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-1").Role == ReplicaRole.Leader);
        leads.Should().BeTrue("machine 1 has the longest log, and machine 3 is cut off");
        cluster.Replica("machine-1").Term.Should().Be(6UL);

        var ready = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica("machine-1").IsReady);
        ready.Should().BeTrue("the NoOp of term 6 commits on machines 1 and 2");
        return cluster;
    }

    // The payload of one entry of one replica's log.
    static async Task<byte[]> PayloadAsync(SimulatedCluster cluster, string machine, ulong index) {
        var log = cluster.Log(machine);
        var buffer = new byte[log.GetRecordLength(index)];
        var entry = await log.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
        return entry.Payload.ToArray();
    }

    // A 10-byte payload: its record is 37 bytes, one over the 27 a full replica still has
    // outside its reserve.
    static byte[] Payload(byte fill) =>
        Enumerable.Repeat(fill, 10).ToArray();
}
