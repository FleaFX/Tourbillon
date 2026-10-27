using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaReplicationTests {
    [Fact]
    public async Task A_conflict_hint_moves_the_next_index_behind_the_conflicting_term() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 1, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 1, votedFor: "machine-1", entries: entries);
        await h.LoadAsync();
        await h.ElectAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(2UL);
        h.Log.LastIndex.Should().Be(3UL);  // the NoOp of the new term
        h.Log.GetTerm(3).Should().Be(2UL);

        // The follower conflicts at index 2, where its log holds term 1: the leader
        // holds one entry of that term too, so it resends right after it.
        h.Replica.Receive("machine-2", new AppendResponse(2, AppendResult.Mismatch, 2, 1));
        await h.TurnAsync();

        var sent = h.Transport.AppendRequests.Where(r => r.Peer == "machine-2").ToArray();
        sent.Should().HaveCount(2);
        var resent = sent[1].Message;
        resent.Term.Should().Be(2UL);
        resent.PrevIndex.Should().Be(2UL);
        resent.PrevTerm.Should().Be(1UL);
        resent.Entries.Length.Should().Be(1);
        resent.Entries.Span[0].Index.Should().Be(3UL);
        resent.Entries.Span[0].Kind.Should().Be(EntryKind.NoOp);
    }

    [Fact]
    public async Task A_conflict_without_a_term_moves_the_next_index_to_the_conflict_index() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 1, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 1, votedFor: "machine-1", entries: entries);
        await h.LoadAsync();
        await h.ElectAsync();

        // No conflict term: the leader resends from the conflict index itself.
        h.Replica.Receive("machine-2", new AppendResponse(2, AppendResult.Mismatch, 2, 0));
        await h.TurnAsync();

        var sent = h.Transport.AppendRequests.Where(r => r.Peer == "machine-2").ToArray();
        sent.Should().HaveCount(2);
        var resent = sent[1].Message;
        resent.PrevIndex.Should().Be(1UL);
        resent.PrevTerm.Should().Be(1UL);
        resent.Entries.Length.Should().Be(2);
        resent.Entries.Span[0].Index.Should().Be(2UL);
        resent.Entries.Span[0].Term.Should().Be(1UL);
        resent.Entries.Span[1].Index.Should().Be(3UL);
        resent.Entries.Span[1].Kind.Should().Be(EntryKind.NoOp);
    }

    [Fact]
    public async Task An_entry_of_an_earlier_term_commits_only_with_one_of_the_current_term() {
        var entries = new[] { new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }) };
        await using var h = await ReplicaHarness.MakeAsync(term: 1, votedFor: "machine-1", entries: entries);
        await h.LoadAsync();
        await h.ElectAsync();

        // A majority matches the entry of term 1, but an entry of an earlier term
        // commits only with one of the current term.
        h.StoreAt("machine-2", 1);
        h.StoreAt("machine-3", 1);
        await h.TurnAsync();
        h.Replica.CommitIndex.Should().Be(0UL);

        // The NoOp of the current term commits, and with it the entry before it.
        h.StoreAt("machine-2", 2);
        await h.TurnAsync();
        h.Replica.CommitIndex.Should().Be(2UL);
        h.Replica.IsReady.Should().BeTrue();
    }

    [Fact]
    public async Task The_leader_counts_itself_only_after_its_own_append() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();
        h.Replica.IsReady.Should().BeTrue();

        // The append of the proposal waits at the gate: the turn does not finish.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Log.AppendGate = gate;
        var proposal = h.Replica.ProposeAsync(new byte[] { 7 }, false, TestContext.Current.CancellationToken);
        var turn = h.Replica.RunTurnAsync();
        turn.IsCompletedSuccessfully.Should().BeFalse();

        // Both followers report the entry stored; the leader's own append has not
        // completed, so it cannot count itself and commits nothing.
        h.StoreAt("machine-2", 2);
        h.StoreAt("machine-3", 2);
        await h.TurnAsync();
        h.Replica.CommitIndex.Should().Be(1UL);

        gate.SetResult();
        var result = await proposal;
        await turn;
        result.Status.Should().Be(ProposalStatus.Committed);
        result.Index.Should().Be(2UL);
        h.Replica.CommitIndex.Should().Be(2UL);
    }

    [Fact]
    public async Task Many_proposals_arriving_together_cost_one_append_and_one_request_per_follower() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        // Both followers at the end of the log, so the batch goes to each in full.
        h.StoreAt("machine-3", 1);
        await h.TurnAsync();

        var appends = h.Log.AppendCount;
        var requests = h.Transport.AppendRequests.Count;

        var proposals = new List<ValueTask<ProposalResult>>();
        for (var i = 0; i < 100; i++)
            proposals.Add(h.Replica.ProposeAsync(new byte[] { (byte)i }, false, TestContext.Current.CancellationToken));

        await h.TurnAsync();

        h.Log.AppendCount.Should().Be(appends + 1);
        h.Transport.AppendRequests.Count.Should().Be(requests + 2);
        var round = h.Transport.AppendRequests.Skip(requests).ToArray();
        round.Should().HaveCount(2);
        foreach (var (peer, message) in round) {
            peer.Should().BeOneOf("machine-2", "machine-3");
            message.Entries.Length.Should().Be(100);
            message.Entries.Span[0].Index.Should().Be(2UL);
            message.Entries.Span[99].Index.Should().Be(101UL);
        }

        h.StoreAt("machine-2", 101);
        h.StoreAt("machine-3", 101);
        await h.TurnAsync();

        for (var i = 0; i < 100; i++) {
            var result = await proposals[i];
            result.Status.Should().Be(ProposalStatus.Committed);
            result.Index.Should().Be((ulong)(i + 2));
        }
    }

    [Fact]
    public async Task An_append_response_of_an_earlier_term_is_ignored() {
        await using var h = await ReplicaHarness.MakeAsync(term: 4, votedFor: "machine-1");
        await h.LoadAsync();
        await h.ElectAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(5UL);
        var sent = h.Transport.SendCount;

        h.Replica.Receive("machine-2", new AppendResponse(4, AppendResult.Stored, 3, 0));
        await h.TurnAsync();

        h.Replica.CommitIndex.Should().Be(0UL);
        h.Transport.SendCount.Should().Be(sent);
    }

    [Fact]
    public async Task A_stale_heartbeat_does_nothing() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        // A vote request of a higher term steps the leader down.
        h.Replica.Receive("machine-2", new VoteRequest(9, 0, 0, PreVote: false));
        await h.TurnAsync();
        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(9UL);
        var sent = h.Transport.SendCount;

        // The heartbeat timer armed before the step-down still fires once; its
        // generation no longer matches.
        h.Advance(TimeSpan.FromMilliseconds(50));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Transport.SendCount.Should().Be(sent);
    }
}
