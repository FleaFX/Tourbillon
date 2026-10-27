using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaRoomTests {
    [Fact]
    public async Task A_full_follower_does_not_stop_the_group() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        var first = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        // machine-2 is full: it took nothing of the entry and reports Full.
        h.Replica.Receive("machine-2", new AppendResponse(h.Replica.Term, AppendResult.Full, 1, 0));
        await h.TurnAsync();

        // machine-3 stores the entry: a majority has it, the proposal is committed.
        h.Replica.Receive("machine-3", new AppendResponse(h.Replica.Term, AppendResult.Stored, 2, 0));
        await h.TurnAsync();

        var committed = await first;
        committed.Status.Should().Be(ProposalStatus.Committed);
        committed.Index.Should().Be(2UL);

        // One full follower leaves a majority with room: the group goes on.
        var second = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(3UL);

        h.StoreAt("machine-3", 3);
        await h.TurnAsync();
        var committedSecond = await second;
        committedSecond.Status.Should().Be(ProposalStatus.Committed);
        committedSecond.Index.Should().Be(3UL);
    }

    [Fact]
    public async Task With_no_majority_free_an_unflagged_proposal_is_refused_before_writing_and_a_flagged_one_goes_through() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        var stuck = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(2UL);

        // Both followers are full: neither takes the entry, both are reserve-only.
        h.Replica.Receive("machine-2", new AppendResponse(h.Replica.Term, AppendResult.Full, 1, 0));
        await h.TurnAsync();
        h.Replica.Receive("machine-3", new AppendResponse(h.Replica.Term, AppendResult.Full, 1, 0));
        await h.TurnAsync();
        stuck.IsCompleted.Should().BeFalse();

        // No majority is free: an unflagged proposal is refused before it is written.
        var refused = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        var result = await refused;
        result.Status.Should().Be(ProposalStatus.Refused);
        result.Reason.Should().Be(RefusalReason.StorageFull);
        result.Index.Should().Be(0UL);
        h.Log.LastIndex.Should().Be(2UL);  // not written

        // A flagged one goes through: it may use the reserve of the full followers.
        var flagged = h.Replica.ProposeAsync(new byte[] { 3 }, true, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(3UL);
    }

    [Fact]
    public async Task When_room_returns_the_stuck_entry_is_stored_and_a_new_proposal_is_committed() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        var stuck = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        // Both followers are full; the entry is written but stuck.
        h.Replica.Receive("machine-2", new AppendResponse(h.Replica.Term, AppendResult.Full, 1, 0));
        await h.TurnAsync();
        h.Replica.Receive("machine-3", new AppendResponse(h.Replica.Term, AppendResult.Full, 1, 0));
        await h.TurnAsync();
        stuck.IsCompleted.Should().BeFalse();

        // Room returns on machine-2: the stuck entry is stored and commits.
        h.Replica.Receive("machine-2", new AppendResponse(h.Replica.Term, AppendResult.Stored, 2, 0));
        await h.TurnAsync();

        var stored = await stuck;
        stored.Status.Should().Be(ProposalStatus.Committed);
        stored.Index.Should().Be(2UL);

        // One follower free is a majority: a new proposal is committed again.
        var next = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(3UL);

        h.StoreAt("machine-3", 3);
        await h.TurnAsync();
        var committed = await next;
        committed.Status.Should().Be(ProposalStatus.Committed);
        committed.Index.Should().Be(3UL);
    }
}
