using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaCheckQuorumTests {
    [Fact]
    public async Task ALeader_that_hears_a_majority_every_check_stays() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        // The first check passes on the answer of machine-2.
        h.Advance(TimeSpan.FromMilliseconds(150));
        await h.TurnAsync();
        h.Replica.Role.Should().Be(ReplicaRole.Leader);

        // The second on the answer of machine-3: every check counts the answers since
        // the last one.
        h.StoreAt("machine-3", h.Log.LastIndex);
        await h.TurnAsync();
        h.Advance(TimeSpan.FromMilliseconds(150));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(1UL);
        h.Replica.IsReady.Should().BeTrue();
    }

    [Fact]
    public async Task ALeader_that_hears_no_majority_steps_down_written_proposals_end_unknown_and_new_ones_are_refused() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        // The first check passes: machine-2 answered in this term.
        h.Advance(TimeSpan.FromMilliseconds(150));
        await h.TurnAsync();
        h.Replica.Role.Should().Be(ReplicaRole.Leader);

        var written = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.Log.LastIndex.Should().Be(2UL);

        // No follower answers within the next check: the leader steps down to a
        // follower of the same term, leader unknown.
        h.Advance(TimeSpan.FromMilliseconds(150));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(1UL);
        h.Replica.Leader.Should().BeNull();
        h.Replica.IsReady.Should().BeFalse();

        var unknown = await written;
        unknown.Status.Should().Be(ProposalStatus.Unknown);

        var refused = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        var result = await refused;
        result.Status.Should().Be(ProposalStatus.Refused);
        result.Reason.Should().Be(RefusalReason.NotLeader);
    }
}
