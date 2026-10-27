using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaAppendTests {
    [Fact]
    public async Task A_stale_term_is_refused_with_the_own_term() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new AppendRequest(2, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Peer.Should().Be("machine-2");
        response.Message.Should().Be(new AppendResponse(3, AppendResult.Mismatch, 0, 0));
        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(3UL);
    }

    [Fact]
    public async Task A_candidate_becomes_follower_of_the_sender_on_an_equal_term() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        await h.TurnAsync();
        h.Replica.Role.Should().Be(ReplicaRole.Candidate);

        h.Replica.Receive("machine-2", new AppendRequest(h.Replica.Term, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Leader.Should().Be("machine-2");
        h.Replica.Term.Should().Be(1UL);
    }

    [Fact]
    public async Task A_higher_term_makes_a_follower_and_resets_the_vote() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3, votedFor: "machine-2");
        await h.LoadAsync();

        h.Replica.Receive("machine-1", new AppendRequest(6, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(6UL);
        h.Replica.VotedFor.Should().BeNull();
        h.Replica.Leader.Should().Be("machine-1");
        var (term, votedFor) = await h.Votes.LoadAsync(TestContext.Current.CancellationToken);
        term.Should().Be(6UL);
        votedFor.Should().BeNull();
    }

    [Fact]
    public async Task A_request_beyond_the_end_of_the_log_reports_the_next_free_index() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 1, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 2, entries: entries);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new AppendRequest(2, 5, 1, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Message.Should().Be(new AppendResponse(2, AppendResult.Mismatch, 3, 0));
    }

    [Fact]
    public async Task The_conflict_hint_points_before_the_whole_conflicting_term() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
            new Entry(3, 2, EntryKind.Command, EntryFlags.None, new byte[] { 3 }),
            new Entry(4, 2, EntryKind.Command, EntryFlags.None, new byte[] { 4 }),
            new Entry(5, 3, EntryKind.Command, EntryFlags.None, new byte[] { 5 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 3, entries: entries);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new AppendRequest(3, 3, 1, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Message.Should().Be(new AppendResponse(3, AppendResult.Mismatch, 2, 2));
    }

    [Fact]
    public async Task Entries_it_already_has_are_skipped_and_its_payload_stays() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 2, entries: entries);
        await h.LoadAsync();

        var offered = new[] { new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 9 }) };
        h.Replica.Receive("machine-2", new AppendRequest(2, 1, 1, 0, offered));
        await h.TurnAsync();

        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Message.Should().Be(new AppendResponse(2, AppendResult.Stored, 2, 0));

        var buffer = new byte[h.Log.GetRecordLength(2)];
        var kept = await h.Log.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
        kept.Payload.ToArray().Should().Equal(new byte[] { 2 });
    }

    [Fact]
    public async Task Only_a_real_conflict_truncates() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
            new Entry(3, 2, EntryKind.Command, EntryFlags.None, new byte[] { 3 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 2, entries: entries);
        await h.LoadAsync();

        var offered = new[] { new Entry(2, 3, EntryKind.Command, EntryFlags.None, new byte[] { 7 }) };
        h.Replica.Receive("machine-2", new AppendRequest(2, 1, 1, 0, offered));
        await h.TurnAsync();

        var response = h.Transport.AppendResponses.Should().ContainSingle().Which;
        response.Message.Should().Be(new AppendResponse(2, AppendResult.Stored, 2, 0));
        h.Log.LastIndex.Should().Be(2UL);
        h.Log.GetTerm(2).Should().Be(3UL);
        var buffer = new byte[h.Log.GetRecordLength(2)];
        var replaced = await h.Log.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
        replaced.Payload.ToArray().Should().Equal(new byte[] { 7 });
    }

    [Fact]
    public async Task Replacing_a_committed_entry_breaks_the_replica() {
        var entries = new[] {
            new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 2, entries: entries);
        await h.LoadAsync();

        // This request confirms both entries and commits up to index 2.
        h.Replica.Receive("machine-2", new AppendRequest(2, 0, 0, 2, entries));
        await h.TurnAsync();
        h.Replica.CommitIndex.Should().Be(2UL);

        var offered = new[] { new Entry(2, 3, EntryKind.Command, EntryFlags.None, new byte[] { 7 }) };
        h.Replica.Receive("machine-2", new AppendRequest(2, 1, 1, 2, offered));
        await h.TurnAsync();

        h.Replica.Fault.Should().BeAssignableTo<InvalidDataException>();
        h.Replica.Fault!.Message.Should().Contain("committed entry at index 2");
    }

    [Fact]
    public async Task APreVote_is_refused_while_a_leader_was_heard_and_granted_after_the_timeout() {
        var entries = new[] { new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }) };
        await using var h = await ReplicaHarness.MakeAsync(term: 2, entries: entries);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new AppendRequest(2, 1, 1, 0, ReadOnlyMemory<Entry>.Empty));
        await h.TurnAsync();

        h.Replica.Receive("machine-3", new VoteRequest(3, 1, 1, PreVote: true));
        await h.TurnAsync();

        h.Transport.VoteResponses.Should().ContainSingle().Which.Message.Should().Be(new VoteResponse(2, false, PreVote: true));

        h.Advance(TimeSpan.FromMilliseconds(150));
        h.Replica.Receive("machine-3", new VoteRequest(3, 1, 1, PreVote: true));
        await h.TurnAsync();

        h.Transport.VoteResponses.Should().HaveCount(2);
        h.Transport.VoteResponses[1].Message.Should().Be(new VoteResponse(2, true, PreVote: true));
    }
}
