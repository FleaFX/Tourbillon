using AwesomeAssertions;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class ReplicaProposalTests {
    [Fact]
    public async Task A_proposal_at_a_follower_is_refused_with_the_leader_name() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new AppendRequest(1, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();
        h.Replica.Leader.Should().Be("machine-2");

        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Refused);
        result.Reason.Should().Be(RefusalReason.NotLeader);
        result.Leader.Should().Be("machine-2");
        result.Index.Should().Be(0UL);
    }

    [Fact]
    public async Task A_proposal_on_a_one_replica_group_is_committed() {
        await using var h = await ReplicaHarness.MakeAsync(members: new[] { "machine-1" });
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.IsReady.Should().BeTrue();  // the NoOp commits in the same turn

        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Committed);
        result.Index.Should().Be(2UL);
    }

    [Fact]
    public async Task The_leader_is_not_ready_until_its_NoOp_commits() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        h.Replica.IsReady.Should().BeFalse();

        h.StoreAt("machine-2", h.Log.LastIndex);
        await h.TurnAsync();

        h.Replica.CommitIndex.Should().Be(1UL);
        h.Replica.IsReady.Should().BeTrue();
    }

    [Fact]
    public async Task A_waiting_proposal_is_refused_when_the_leader_steps_down() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        // The ready gate is closed: the proposal waits, it is not written.
        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        proposal.IsCompleted.Should().BeFalse();
        h.Log.LastIndex.Should().Be(1UL);

        h.Replica.Receive("machine-2", new VoteRequest(9, 0, 0, PreVote: false));
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Refused);
        result.Reason.Should().Be(RefusalReason.NotLeader);
        result.Leader.Should().BeNull();
    }

    [Fact]
    public async Task A_written_proposal_ends_unknown_when_the_leader_steps_down() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        proposal.IsCompleted.Should().BeFalse();
        h.Log.LastIndex.Should().Be(2UL);

        h.Replica.Receive("machine-2", new VoteRequest(9, 0, 0, PreVote: false));
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Unknown);
    }

    [Fact]
    public async Task A_cancelled_written_proposal_ends_unknown() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        using var cts = new CancellationTokenSource();
        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, cts.Token);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(2UL);

        cts.Cancel();
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Unknown);
    }

    [Fact]
    public async Task A_cancelled_waiting_proposal_ends_unknown_and_is_never_written() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        using var cts = new CancellationTokenSource();
        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, cts.Token);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(1UL);

        cts.Cancel();
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Unknown);
        h.Log.LastIndex.Should().Be(1UL);
    }

    [Fact]
    public async Task A_new_leader_without_room_for_its_NoOp_refuses_waiting_proposals_storage_full() {
        // A NoOp record takes 27 bytes; with a maximum of 54 and a reserve of 28 it does
        // not fit.
        await using var h = await ReplicaHarness.MakeAsync(log: new InMemoryLog(maximumSize: 54, reserveSize: 28));
        await h.LoadAsync();
        await h.ElectAsync();
        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Log.LastIndex.Should().Be(0UL);

        var proposal = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        var result = await proposal;
        result.Status.Should().Be(ProposalStatus.Refused);
        result.Reason.Should().Be(RefusalReason.StorageFull);
        h.Log.LastIndex.Should().Be(0UL);
        h.Replica.IsReady.Should().BeFalse();
    }

    [Fact]
    public async Task A_full_own_log_refuses_the_rest_of_the_batch_and_takes_a_flagged_entry() {
        // A NoOp record takes 27 bytes, a one-byte payload 28; with a reserve of 28,
        // after the NoOp one unflagged entry fits and nothing more, while a flagged
        // entry still fits exactly.
        await using var h = await ReplicaHarness.MakeAsync(log: new InMemoryLog(maximumSize: 83, reserveSize: 28));
        await h.LoadAsync();
        await h.ElectReadyAsync();
        h.Replica.IsReady.Should().BeTrue();

        var first = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        var second = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        var refused = await second;
        refused.Status.Should().Be(ProposalStatus.Refused);
        refused.Reason.Should().Be(RefusalReason.StorageFull);
        first.IsCompleted.Should().BeFalse();
        h.Log.LastIndex.Should().Be(2UL);

        var flagged = h.Replica.ProposeAsync(new byte[] { 3 }, true, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        h.Log.LastIndex.Should().Be(3UL);
        h.Log.GetTerm(3).Should().Be(h.Replica.Term);
    }
}
