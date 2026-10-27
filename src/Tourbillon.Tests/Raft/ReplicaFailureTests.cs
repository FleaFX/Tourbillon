using AwesomeAssertions;

using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class ReplicaFailureTests {
    [Fact]
    public async Task A_failed_write_at_the_leader_refuses_the_batch_and_stays_leader() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        h.Log.FailNextAppend = true;
        var sent = h.Transport.SendCount;
        var first = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        var second = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        (await first).Should().Be(ProposalResult.Refused(RefusalReason.WriteFailed));
        (await second).Should().Be(ProposalResult.Refused(RefusalReason.WriteFailed));
        h.Transport.SendCount.Should().Be(sent);
        h.Log.LastIndex.Should().Be(1UL);
        h.Log.IsFaulted.Should().BeFalse();
        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.IsReady.Should().BeTrue();
        h.Replica.Fault.Should().BeNull();

        var third = h.Replica.ProposeAsync(new byte[] { 3 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        h.StoreAt("machine-2", h.Log.LastIndex);
        await h.TurnAsync();

        (await third).Should().Be(ProposalResult.Committed(2));
    }

    [Fact]
    public async Task A_failed_write_at_a_follower_leaves_it_silent_until_the_next_heartbeat() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();

        var entries = new[] {
            new Entry(1, 2, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };

        h.Log.FailNextAppend = true;
        h.Replica.Receive("machine-2", new AppendRequest(2, 0, 0, 0, entries));
        await h.TurnAsync();

        h.Transport.AppendResponses.Should().BeEmpty();
        h.Log.LastIndex.Should().Be(0UL);
        h.Log.IsFaulted.Should().BeFalse();
        h.Replica.Fault.Should().BeNull();

        h.Replica.Receive("machine-2", new AppendRequest(2, 0, 0, 2, entries));
        await h.TurnAsync();

        h.Log.LastIndex.Should().Be(2UL);
        h.Replica.CommitIndex.Should().Be(2UL);
        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Peer.Should().Be("machine-2");
        response.Message.Should().Be(new AppendResponse(2, AppendResult.Stored, 2, 0));
    }

    [Fact]
    public async Task A_faulted_log_at_the_leader_breaks_the_replica() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        var written = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();
        written.IsCompleted.Should().BeFalse();

        h.Log.FaultNextAppend = true;
        var inFlight = h.Replica.ProposeAsync(new byte[] { 2 }, false, TestContext.Current.CancellationToken);
        await h.TurnAsync();

        (await inFlight).Should().Be(ProposalResult.Unknown);
        (await written).Should().Be(ProposalResult.Unknown);
        h.Replica.Fault.Should().BeAssignableTo<IOException>();
        h.Log.IsFaulted.Should().BeTrue();
        h.Log.LastIndex.Should().Be(2UL);

        var refused = await h.Replica.ProposeAsync(new byte[] { 3 }, false, TestContext.Current.CancellationToken);
        refused.Should().Be(ProposalResult.Refused(RefusalReason.WriteFailed));

        var sent = h.Transport.SendCount;
        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.Transport.SendCount.Should().Be(sent);

        h.Replica.Receive("machine-2", new VoteRequest(9, 0, 0, PreVote: false));
        h.Replica.Receive("machine-2", new AppendRequest(9, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();
        h.Transport.SendCount.Should().Be(sent);
        h.Transport.VoteResponses.Should().BeEmpty();
        h.Transport.AppendResponses.Should().BeEmpty();
    }

    [Fact]
    public async Task A_waiting_proposal_is_refused_when_the_log_faults() {
        await using var h = await ReplicaHarness.MakeAsync();
        h.Log.FailNextAppend = true;
        await h.LoadAsync();
        await h.ElectAsync();

        // The NoOp's append failed without a fault: it stays pending, the replica stays
        // the leader, and the proposals wait behind the closed ready gate.
        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.IsReady.Should().BeFalse();
        h.Log.LastIndex.Should().Be(0UL);
        h.Log.IsFaulted.Should().BeFalse();

        var waiting = h.Replica.ProposeAsync(new byte[] { 1 }, false, TestContext.Current.CancellationToken);
        waiting.IsCompleted.Should().BeFalse();

        h.Log.FaultNextAppend = true;
        await h.TurnAsync();

        (await waiting).Should().Be(ProposalResult.Refused(RefusalReason.WriteFailed));
        h.Replica.Fault.Should().BeAssignableTo<IOException>();
        h.Log.IsFaulted.Should().BeTrue();
    }

    [Fact]
    public async Task A_failing_vote_store_breaks_the_replica_and_sends_no_vote() {
        var votes = new FailingVoteStore();
        await using var h = await ReplicaHarness.MakeAsync(term: 1, votes: votes);
        await h.LoadAsync();

        votes.FailNextSave = true;
        h.Replica.Receive("machine-2", new VoteRequest(2, 0, 0, PreVote: false));
        await h.TurnAsync();

        h.Replica.Fault.Should().BeAssignableTo<IOException>();
        h.Transport.VoteResponses.Should().BeEmpty();
        h.Replica.Term.Should().Be(2UL);
        var (term, votedFor) = await votes.LoadAsync(TestContext.Current.CancellationToken);
        term.Should().Be(1UL);
        votedFor.Should().BeNull();

        h.Replica.Receive("machine-2", new VoteRequest(3, 0, 0, PreVote: false));
        await h.TurnAsync();
        h.Transport.VoteResponses.Should().BeEmpty();
    }
}

// Throws on the next save, so a test can break the vote store at a chosen moment.
sealed class FailingVoteStore : IVoteStore {
    readonly InMemoryVoteStore _inner = new();

    public bool FailNextSave { get; set; }

    public ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default) =>
        _inner.LoadAsync(cancellationToken);

    public ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default) {
        if (FailNextSave) {
            FailNextSave = false;
            throw new IOException("Simulated vote store failure.");
        }

        return _inner.SaveAsync(term, votedFor, cancellationToken);
    }
}
