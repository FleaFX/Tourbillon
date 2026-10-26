using AwesomeAssertions;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class SimulatedClusterTests {
    [Fact]
    public async Task Three_copies_elect_exactly_one_leader() {
        await using var cluster = await SimulatedCluster.CreateAsync(copies: 3, seed: 1, ClusterRuns.Reliable);
        await cluster.StartAsync();

        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);

        elected.Should().BeTrue("a 3-copy cluster with a reliable network must elect a leader");
        cluster.Machines.Count(machine => cluster.Replica(machine).Role == ReplicaRole.Leader).Should().Be(1);
    }

    [Fact]
    public async Task A_proposal_is_committed_on_every_copy() {
        await using var cluster = await SimulatedCluster.CreateAsync(copies: 3, seed: 2, ClusterRuns.Reliable);
        await cluster.StartAsync();
        await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);
        var leader = ClusterRuns.Leader(cluster)!;

        var proposal = cluster.Replica(leader).ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        var committed = await ClusterRuns.UntilAsync(cluster, () => proposal.IsCompleted);
        committed.Should().BeTrue("the proposal must commit on a healthy 3-copy cluster");

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Accepted);

        foreach (var machine in cluster.Machines) {
            var caughtUp = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica(machine).CommitIndex >= result.Index);
            caughtUp.Should().BeTrue($"'{machine}' should catch up to the committed index");
        }
    }

    [Fact]
    public async Task A_restarted_copy_catches_up() {
        await using var cluster = await SimulatedCluster.CreateAsync(copies: 3, seed: 3, ClusterRuns.Reliable);
        await cluster.StartAsync();
        await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);
        var leader = ClusterRuns.Leader(cluster)!;

        var first = cluster.Replica(leader).ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await ClusterRuns.UntilAsync(cluster, () => first.IsCompleted);
        var firstResult = await first;
        firstResult.Status.Should().Be(ProposalStatus.Accepted);

        // Crash a follower, let it miss a proposal, restart it: it catches up.
        var follower = cluster.Machines.First(machine => machine != leader);
        await cluster.CrashAsync(follower);

        var second = cluster.Replica(leader).ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await ClusterRuns.UntilAsync(cluster, () => second.IsCompleted);
        var secondResult = await second;
        secondResult.Status.Should().Be(ProposalStatus.Accepted);

        await cluster.RestartAsync(follower);
        var caughtUp = await ClusterRuns.UntilAsync(cluster, () =>
            cluster.Log(follower).LastIndex >= secondResult.Index && cluster.Replica(follower).CommitIndex >= secondResult.Index);
        caughtUp.Should().BeTrue($"'{follower}' should catch up after its restart");
    }

    [Fact]
    public async Task A_minority_island_cannot_commit_while_the_majority_does() {
        await using var cluster = await SimulatedCluster.CreateAsync(copies: 3, seed: 4, ClusterRuns.Reliable);
        await cluster.StartAsync();
        await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);
        var leader = ClusterRuns.Leader(cluster)!;
        await ClusterRuns.UntilAsync(cluster, () => cluster.Replica(leader).IsReady);

        // The leader's island against the other two.
        var majority = cluster.Machines.Where(machine => machine != leader).ToArray();
        cluster.Network.Partition([leader], majority);

        // The stranded leader writes the entry, hears no majority, steps down: the
        // proposal ends unknown.
        var stranded = cluster.Replica(leader).ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        var ended = await ClusterRuns.UntilAsync(cluster, () => stranded.IsCompleted);
        ended.Should().BeTrue("the stranded leader steps down and ends the proposal");

        var strandedResult = await stranded;
        strandedResult.Status.Should().Be(ProposalStatus.Unknown);
        cluster.Replica(leader).Role.Should().Be(ReplicaRole.Follower);

        // The majority elects a new leader and commits while the minority cannot.
        var elected = await ClusterRuns.UntilAsync(cluster, () => majority.Any(machine => cluster.Replica(machine).Role == ReplicaRole.Leader));
        elected.Should().BeTrue("the majority must elect a new leader");
        var newLeader = majority.First(machine => cluster.Replica(machine).Role == ReplicaRole.Leader);

        var proposal = cluster.Replica(newLeader).ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        var committed = await ClusterRuns.UntilAsync(cluster, () => proposal.IsCompleted);
        committed.Should().BeTrue("the majority commits while the minority cannot");

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Accepted);
    }

    [Fact]
    public async Task A_crash_during_a_file_operation_takes_the_copy_down() {
        await using var cluster = await SimulatedCluster.CreateAsync(copies: 3, seed: 5, ClusterRuns.Reliable with { Disk = true });
        await cluster.StartAsync();
        var elected = await ClusterRuns.UntilAsync(cluster, () => ClusterRuns.Leader(cluster) is not null);
        elected.Should().BeTrue();
        var leader = ClusterRuns.Leader(cluster)!;
        var ready = await ClusterRuns.UntilAsync(cluster, () => cluster.Replica(leader).IsReady);
        ready.Should().BeTrue();

        // The next file operation of the leader crashes it: the write of a proposal.
        await cluster.CrashDuringAsync(leader, 1);
        var proposal = cluster.Replica(leader).ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await cluster.StepAsync(TimeSpan.FromMilliseconds(200));

        cluster.IsRunning(leader).Should().BeFalse("the copy crashed during the write of its entry");
        proposal.IsCompleted.Should().BeTrue();
        (await proposal).Status.Should().Be(ProposalStatus.Unknown);
    }
}
