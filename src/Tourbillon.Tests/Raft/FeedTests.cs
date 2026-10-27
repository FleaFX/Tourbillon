using AwesomeAssertions;

using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class FeedTests {
    [Fact]
    public async Task Every_replicas_feed_hands_the_same_entries_in_log_order() {
        await using var group = await DiskGroup.StartAsync(3);

        // Wait for a leader: every replica's feed wakes when its NoOp commits.
        await Task.WhenAll(group.Machines.Select(machine => group.Feed(machine).WaitToReadAsync(TestContext.Current.CancellationToken).AsTask()));
        var leader = group.Machines.Select(machine => group.Replica(machine)).Single(replica => replica.IsReady);

        // Five commands, one at a time: every replica's feed hands them over in batches
        // that start right after the one before, from index 1.
        var collected = group.Machines.ToDictionary(machine => machine, _ => new List<Entry>());
        for (var i = 0; i < 5; i++) {
            var index = (ulong)(i + 2);
            var committed = await leader.ProposeAsync(new[] { (byte)(i + 1) }, false, TestContext.Current.CancellationToken);
            committed.Should().Be(ProposalResult.Committed(index));
            foreach (var machine in group.Machines)
                await DiskGroup.ReadUpToAsync(group.Replica(machine), index, collected[machine]);
        }

        // Every replica handed the same entries: the NoOp of the term and the five
        // commands, in log order.
        var expected = collected["machine-1"];
        expected.Should().HaveCount(6);
        for (var i = 0; i < expected.Count; i++)
            expected[i].Index.Should().Be((ulong)(i + 1));
        foreach (var machine in group.Machines) {
            var entries = collected[machine];
            entries.Should().HaveCount(expected.Count);
            for (var i = 0; i < entries.Count; i++) {
                entries[i].Index.Should().Be(expected[i].Index);
                entries[i].Term.Should().Be(expected[i].Term);
                entries[i].Kind.Should().Be(expected[i].Kind);
                entries[i].Flags.Should().Be(expected[i].Flags);
                entries[i].Payload.ToArray().Should().Equal(expected[i].Payload.ToArray());
            }
        }
    }

    [Fact]
    public async Task A_restart_with_D_starts_the_feed_after_it() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        // The replica elects itself: the NoOp of the first term commits.
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();

        // Sixty commands commit; the application applies up to 40 and stores D = 40.
        var proposals = new Task<ProposalResult>[60];
        for (var i = 0; i < proposals.Length; i++)
            proposals[i] = replica.ProposeAsync(new[] { (byte)i }, false, TestContext.Current.CancellationToken).AsTask();
        (await Task.WhenAll(proposals)).Should().OnlyContain(result => result.Status == ProposalStatus.Committed);
        replica.CommitIndex.Should().Be(61UL);
        var applied = await ApplyUpToAsync(feed, 40);
        applied.Should().Be(40UL);

        await group.ReopenAsync("machine-1", applied);

        // After the restart the feed starts at 41.
        var batch = await ReadBatchAsync(group.Feed("machine-1"));
        batch.Span[0].Index.Should().Be(41UL);
    }

    [Fact]
    public async Task A_restart_with_D_at_the_last_index_repeats_nothing() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        // The replica elects itself: the NoOp of the first term commits.
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();

        // The application applies everything: D = the last index.
        var proposals = new Task<ProposalResult>[60];
        for (var i = 0; i < proposals.Length; i++)
            proposals[i] = replica.ProposeAsync(new[] { (byte)i }, false, TestContext.Current.CancellationToken).AsTask();
        (await Task.WhenAll(proposals)).Should().OnlyContain(result => result.Status == ProposalStatus.Committed);
        var applied = await ApplyUpToAsync(feed, 61);
        applied.Should().Be(61UL);

        await group.ReopenAsync("machine-1", applied);

        // The first batch after the restart starts after the last index: nothing is
        // repeated.
        var batch = await ReadBatchAsync(group.Feed("machine-1"));
        batch.Span[0].Index.Should().Be(62UL);
    }

    [Fact]
    public async Task D_in_the_middle_of_a_handed_batch_resumes_at_D_plus_one() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        // The replica elects itself: the NoOp of the first term commits.
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();

        // Ten commands commit; the feed hands the NoOp and them in one batch.
        var proposals = new Task<ProposalResult>[10];
        for (var i = 0; i < proposals.Length; i++)
            proposals[i] = replica.ProposeAsync(new[] { (byte)i }, false, TestContext.Current.CancellationToken).AsTask();
        (await Task.WhenAll(proposals)).Should().OnlyContain(result => result.Status == ProposalStatus.Committed);
        var handed = await ReadBatchAsync(feed);
        handed.Length.Should().Be(11);
        handed.Span[0].Index.Should().Be(1UL);
        handed.Span[^1].Index.Should().Be(11UL);

        // The application applies only up to 5 of the handed batch: D = 5, in the middle of it.
        var applied = 0UL;
        foreach (var entry in handed.Span) {
            applied = entry.Index;  // the application applies the entry
            if (applied == 5)
                break;
        }

        applied.Should().Be(5UL);

        await group.ReopenAsync("machine-1", applied);

        // After the restart the feed resumes exactly at D + 1.
        var resumed = await ReadBatchAsync(group.Feed("machine-1"));
        resumed.Span[0].Index.Should().Be(6UL);
    }

    [Fact]
    public async Task A_followers_feed_follows_its_own_commit_index() {
        await using var group = await DiskGroup.StartAsync(3, withoutLoop: new[] { "machine-2" });

        // machine-1 and machine-3 run their loops; the test runs machine-2's turns.
        await Task.WhenAll(
            group.Feed("machine-1").WaitToReadAsync(TestContext.Current.CancellationToken).AsTask(),
            group.Feed("machine-3").WaitToReadAsync(TestContext.Current.CancellationToken).AsTask());
        var leader = new[] { group.Replica("machine-1"), group.Replica("machine-3") }.Single(replica => replica.IsReady);

        var committed = await leader.ProposeAsync(new byte[] { 9 }, false, TestContext.Current.CancellationToken);
        committed.Should().Be(ProposalResult.Committed(2));

        // machine-2 has committed nothing: its feed is empty, though the leader committed.
        var follower = group.Replica("machine-2");
        follower.CommitIndex.Should().Be(0UL);
        (await follower.Feed!.ReadAsync(TestContext.Current.CancellationToken)).Length.Should().Be(0);

        // The test runs machine-2's turns: it catches up, and its feed hands the same entries.
        while (follower.CommitIndex < 2) {
            await follower.RunTurnAsync();
            if (follower.CommitIndex < 2)
                await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        var collected = new List<Entry>();
        await DiskGroup.ReadUpToAsync(follower, 2, collected);
        collected.Should().HaveCount(2);
        collected[0].Kind.Should().Be(EntryKind.NoOp);
        collected[1].Payload.ToArray().Should().Equal(new byte[] { 9 });
    }

    [Fact]
    public async Task A_record_bigger_than_the_buffer_comes_in_a_batch_of_its_own() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        var noOp = await feed.ReadAsync(TestContext.Current.CancellationToken);
        noOp.Length.Should().Be(1);

        // A record bigger than the 1 MiB buffer, then a small one.
        var big = new byte[1024 * 1024 + 1];
        var committedBig = await replica.ProposeAsync(big, false, TestContext.Current.CancellationToken);
        committedBig.Should().Be(ProposalResult.Committed(2));
        var committedSmall = await replica.ProposeAsync(new byte[] { 9 }, false, TestContext.Current.CancellationToken);
        committedSmall.Should().Be(ProposalResult.Committed(3));

        // The big record gets a buffer of its own: the small one does not fit beside it.
        var first = await feed.ReadAsync(TestContext.Current.CancellationToken);
        first.Length.Should().Be(1);
        first.Span[0].Index.Should().Be(2UL);
        first.Span[0].Payload.Length.Should().Be(big.Length);

        var second = await feed.ReadAsync(TestContext.Current.CancellationToken);
        second.Length.Should().Be(1);
        second.Span[0].Index.Should().Be(3UL);
        second.Span[0].Payload.ToArray().Should().Equal(new byte[] { 9 });
    }

    [Fact]
    public async Task A_poisoned_feed_turns_a_kept_batch_into_rubbish() {
        await using var group = await DiskGroup.StartAsync(1, options: new ReplicaOptions { PoisonFeed = true });
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        await feed.ReadAsync(TestContext.Current.CancellationToken);  // the NoOp

        var committed = await replica.ProposeAsync(new byte[] { 42 }, false, TestContext.Current.CancellationToken);
        committed.Should().Be(ProposalResult.Committed(2));
        var batch = await feed.ReadAsync(TestContext.Current.CancellationToken);
        batch.Length.Should().Be(1);
        var kept = batch.Span[0];  // the application keeps the entry instead of copying it
        kept.Payload.ToArray().Should().Equal(new byte[] { 42 });

        // No new entry is committed: the next read is empty, and the kept payload is
        // rubbish.
        var next = await feed.ReadAsync(TestContext.Current.CancellationToken);
        next.Length.Should().Be(0);
        kept.Payload.ToArray().Should().Equal(new byte[] { 0xA5 });
    }

    [Fact]
    public async Task Without_poisoning_a_kept_batch_stays_intact() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        await feed.ReadAsync(TestContext.Current.CancellationToken);  // the NoOp

        var committed = await replica.ProposeAsync(new byte[] { 42 }, false, TestContext.Current.CancellationToken);
        committed.Should().Be(ProposalResult.Committed(2));
        var batch = await feed.ReadAsync(TestContext.Current.CancellationToken);
        batch.Length.Should().Be(1);
        var kept = batch.Span[0];  // the application keeps the entry instead of copying it
        kept.Payload.ToArray().Should().Equal(new byte[] { 42 });

        // The control: without poisoning, the same payload is still intact.
        var next = await feed.ReadAsync(TestContext.Current.CancellationToken);
        next.Length.Should().Be(0);
        kept.Payload.ToArray().Should().Equal(new byte[] { 42 });
    }

    [Fact]
    public async Task A_slow_reader_does_not_slow_committing() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        // The reader takes one batch — the NoOp — and never asks again.
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        var first = await feed.ReadAsync(TestContext.Current.CancellationToken);
        first.Length.Should().Be(1);
        feed.Delivered.Should().Be(1UL);

        // A thousand proposals are still committed: the commit never waits for the feed.
        var proposals = new Task<ProposalResult>[1000];
        for (var i = 0; i < proposals.Length; i++)
            proposals[i] = replica.ProposeAsync(new byte[] { 1, 2, 3 }, false, TestContext.Current.CancellationToken).AsTask();
        (await Task.WhenAll(proposals)).Should().OnlyContain(result => result.Status == ProposalStatus.Committed);
        replica.CommitIndex.Should().Be(1001UL);
        feed.Delivered.Should().Be(1UL);
    }

    [Fact]
    public async Task A_stopped_replica_ends_the_feed() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        await feed.ReadAsync(TestContext.Current.CancellationToken);

        await replica.DisposeAsync();
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeFalse();
    }

    [Fact]
    public async Task A_broken_replica_throws_its_fault_from_the_feed() {
        var votes = new FailingVoteStore { FailNextSave = true };
        await using var group = await DiskGroup.StartAsync(1, votes: _ => votes);
        var feed = group.Feed("machine-1");

        // The first election cannot store its term and vote: the replica breaks, and the
        // feed throws the fault.
        var wait = async () => await feed.WaitToReadAsync(TestContext.Current.CancellationToken);
        await wait.Should().ThrowExactlyAsync<IOException>();
    }

    [Fact]
    public async Task On_the_cluster_every_replicas_feed_hands_the_same_entries_in_log_order() {
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 21, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var leader = await ReadyLeaderAsync(cluster);

        // Five commands commit on every replica; the applications apply the NoOp and them.
        for (var i = 0; i < 5; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)(i + 1) }, (ulong)(i + 2));

        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 6));
        caughtUp.Should().BeTrue();

        // Every application applied the same six entries, in log order, from index 1.
        var expected = cluster.Application("machine-1").Applied;
        expected.Should().HaveCount(6);
        for (var i = 0; i < expected.Count; i++)
            expected[i].Index.Should().Be((ulong)(i + 1));
        foreach (var machine in cluster.Machines)
            ShouldBeTheSameEntries(expected, cluster.Application(machine).Applied);
    }

    [Fact]
    public async Task On_the_cluster_a_restart_resumes_the_applications_feed_at_its_stored_D() {
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 22, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var leader = await ReadyLeaderAsync(cluster);
        var follower = cluster.Machines.First(machine => machine != leader);
        var other = cluster.Machines.First(machine => machine != leader && machine != follower);

        // Ten commands commit on every replica; the applications apply them.
        for (var i = 0; i < 10; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)i }, (ulong)(i + 2));
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 11));
        caughtUp.Should().BeTrue();

        // Six more commit while the follower is cut off: its application applies nothing more.
        cluster.Network.Partition([leader, other], [follower]);
        for (var i = 0; i < 6; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)(10 + i) }, (ulong)(12 + i));
        var majority = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Replica(leader).CommitIndex >= 17 && cluster.Replica(other).CommitIndex >= 17);
        majority.Should().BeTrue();
        cluster.Application(follower).Applied.Should().HaveCount(11);

        // The follower crashes: its application loses what it applied after its last store.
        var application = cluster.Application(follower);
        var stored = application.Stored;
        stored.Should().BeLessThan(11UL, "the seeded stores left the application's D behind: the crash loses what it applied after its last store");
        await cluster.CrashAsync(follower);
        application.Applied.Should().HaveCount((int)stored);

        // The restart opens the feed at the stored D...
        await cluster.RestartAsync(follower);
        cluster.Replica(follower).Feed!.Delivered.Should().Be(stored);

        // ...and the leader brings the entries again: the application re-applies them, in order.
        cluster.Network.Heal();
        var caughtUpAgain = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica(follower).CommitIndex >= 17);
        caughtUpAgain.Should().BeTrue();
        ShouldBeTheSameEntries(cluster.Application(leader).Applied, application.Applied);
    }

    [Fact]
    public async Task On_the_cluster_a_restart_with_D_at_the_last_index_repeats_nothing() {
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 23, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var leader = await ReadyLeaderAsync(cluster);
        var follower = cluster.Machines.First(machine => machine != leader);

        // Ten commands commit; the applications apply up to index 11.
        for (var i = 0; i < 10; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)i }, (ulong)(i + 2));
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 11));
        caughtUp.Should().BeTrue();

        // The application stores D = 11, the last index, and the replica crashes right away.
        var application = cluster.Application(follower);
        application.Store();
        await cluster.CrashAsync(follower);
        application.Applied.Should().HaveCount(11);

        // The restart opens the feed at D: nothing at or below it is repeated.
        await cluster.RestartAsync(follower);
        cluster.Replica(follower).Feed!.Delivered.Should().Be(11UL);

        // Two more commands commit; the application applies only them.
        for (var i = 0; i < 2; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)(10 + i) }, (ulong)(12 + i));
        var caughtUpAgain = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 13));
        caughtUpAgain.Should().BeTrue();

        application.Applied.Should().HaveCount(13);
        ShouldBeTheSameEntries(cluster.Application(leader).Applied, application.Applied);
    }

    [Fact]
    public async Task On_the_cluster_a_followers_feed_follows_its_own_commit_index() {
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 24, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var leader = await ReadyLeaderAsync(cluster);
        var follower = cluster.Machines.First(machine => machine != leader);
        var other = cluster.Machines.First(machine => machine != leader && machine != follower);

        // The NoOp commits on every replica.
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 1));
        caughtUp.Should().BeTrue();

        // Isolate the follower: the majority commits three commands, its application applies
        // nothing more — the feed follows its own commit index.
        cluster.Network.Partition([leader, other], [follower]);
        for (var i = 0; i < 3; i++)
            await ProposeAsync(cluster, leader, new[] { (byte)(i + 1) }, (ulong)(i + 2));
        var majority = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Replica(leader).CommitIndex >= 4 && cluster.Replica(other).CommitIndex >= 4);
        majority.Should().BeTrue();
        cluster.Application(follower).Applied.Should().HaveCount(1);

        // The network heals: the follower catches up, and its application applies the same entries.
        cluster.Network.Heal();
        var caughtUpAgain = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 4));
        caughtUpAgain.Should().BeTrue();
        ShouldBeTheSameEntries(cluster.Application(leader).Applied, cluster.Application(follower).Applied);
    }

    [Fact]
    public async Task On_the_cluster_a_record_bigger_than_the_buffer_is_applied_whole() {
        await using var cluster = await SimulatedCluster.CreateAsync(replicas: 3, seed: 25, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var leader = await ReadyLeaderAsync(cluster);

        // A record bigger than the feed's 1 MiB buffer, then a small one.
        var big = new byte[1024 * 1024 + 1];
        for (var i = 0; i < big.Length; i++)
            big[i] = (byte)i;
        await ProposeAsync(cluster, leader, big, 2);
        await ProposeAsync(cluster, leader, new byte[] { 9 }, 3);

        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Machines.All(machine => cluster.Replica(machine).CommitIndex >= 3));
        caughtUp.Should().BeTrue();

        // Every application applied the big record whole, and the small one after it.
        foreach (var machine in cluster.Machines) {
            var applied = cluster.Application(machine).Applied;
            applied.Should().HaveCount(3);
            applied[1].Payload.Length.Should().Be(big.Length);
            applied[1].Payload.ToArray().Should().Equal(big);
            applied[2].Payload.ToArray().Should().Equal(new byte[] { 9 });
        }
    }

    // Reads the feed and applies the entries one by one up to the target index: the
    // application's D is the last index it applied, even when a batch handed more.
    static async Task<ulong> ApplyUpToAsync(Feed feed, ulong target) {
        var applied = 0UL;
        while (applied < target) {
            (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
            var batch = await feed.ReadAsync(TestContext.Current.CancellationToken);
            foreach (var entry in batch.Span) {
                applied = entry.Index;  // the application applies the entry
                if (applied == target)
                    break;
            }
        }

        return applied;
    }

    // Waits for the next batch and reads it.
    static async Task<ReadOnlyMemory<Entry>> ReadBatchAsync(Feed feed) {
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        return await feed.ReadAsync(TestContext.Current.CancellationToken);
    }

    // Waits for a ready leader and returns its name.
    static async Task<string> ReadyLeaderAsync(SimulatedCluster cluster) {
        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);
        elected.Should().BeTrue("a 3-replica cluster with a reliable network must elect a leader");
        var leader = ClusterRuns.Leader(cluster)!;
        var ready = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica(leader).IsReady);
        ready.Should().BeTrue("the NoOp of the leader's term must commit");
        return leader;
    }

    // Proposes one payload at the leader and waits for the outcome: committed at the index.
    static async Task ProposeAsync(SimulatedCluster cluster, string leader, byte[] payload, ulong index) {
        var proposal = cluster.Replica(leader).ProposeAsync(payload, false, TestContext.Current.CancellationToken);
        var committed = await ClusterRuns.UntilAsync(cluster, () => proposal.IsCompleted);
        committed.Should().BeTrue("the proposal must commit on a healthy 3-replica cluster");
        (await proposal).Should().Be(ProposalResult.Committed(index));
    }

    // Asserts two applications applied the same entries: index, term, kind, flags and payload.
    static void ShouldBeTheSameEntries(IReadOnlyList<Entry> expected, IReadOnlyList<Entry> actual) {
        actual.Should().HaveCount(expected.Count);
        for (var i = 0; i < expected.Count; i++) {
            actual[i].Index.Should().Be(expected[i].Index);
            actual[i].Term.Should().Be(expected[i].Term);
            actual[i].Kind.Should().Be(expected[i].Kind);
            actual[i].Flags.Should().Be(expected[i].Flags);
            actual[i].Payload.ToArray().Should().Equal(expected[i].Payload.ToArray());
        }
    }
}
