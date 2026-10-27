using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaElectionTests {
    [Fact]
    public async Task A_one_replica_group_becomes_leader_on_its_own() {
        await using var h = await ReplicaHarness.MakeAsync(members: new[] { "machine-1" });
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(1UL);
        h.Replica.Leader.Should().Be("machine-1");
        h.Replica.IsReady.Should().BeTrue();  // the NoOp of a one-replica group commits in the same turn
        h.Transport.VoteRequests.Should().BeEmpty();  // no one to ask
    }

    [Fact]
    public async Task ACandidate_becomes_leader_on_a_majority() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(1UL);
        h.Replica.Leader.Should().Be("machine-1");
        h.Replica.IsReady.Should().BeFalse();  // not ready until the NoOp commits
    }

    [Fact]
    public async Task The_vote_request_carries_its_term_and_log_position_to_every_voter() {
        var entries = new[] {
            new Entry(1, 2, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 2, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
            new Entry(3, 4, EntryKind.Command, EntryFlags.None, new byte[] { 3 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(entries: entries);
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        h.GrantPreVote("machine-3");
        await h.TurnAsync();

        var real = h.Transport.VoteRequests.Where(r => !r.Message.PreVote).ToArray();
        real.Should().HaveCount(2);
        real.Should().OnlyContain(r => r.Peer != "machine-1");
        real.Should().OnlyContain(r =>
            r.Message.Term == 1UL
            && r.Message.LastIndex == 3UL
            && r.Message.LastTerm == 4UL);
    }

    [Fact]
    public async Task The_self_vote_is_stored_before_any_request_is_sent() {
        var transport = new RecordingTransport();
        var sequenced = new SequencedVoteStore(() => transport.VoteRequests.Any(r => !r.Message.PreVote));
        await using var h = await ReplicaHarness.MakeAsync(votes: sequenced, transport: transport);
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        h.GrantPreVote("machine-3");
        await h.TurnAsync();

        sequenced.Saves.Should().Be(1);
        sequenced.ProbeHits.Should().Be(0);  // no real request was sent at the save
        var (term, votedFor) = await h.Votes.LoadAsync(TestContext.Current.CancellationToken);
        term.Should().Be(1UL);
        votedFor.Should().Be("machine-1");
    }

    [Fact]
    public async Task AVote_response_of_an_earlier_term_is_ignored() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        h.GrantPreVote("machine-3");
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Candidate);

        // A grant of an earlier term arrives.
        h.Replica.Receive("machine-2", new VoteResponse(0, true, PreVote: false));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Candidate);
        h.Replica.Term.Should().Be(1UL);
    }

    [Fact]
    public async Task A_stale_election_timer_never_demotes_a_leader_nor_raises_the_term() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectReadyAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        var voteRequests = h.Transport.VoteRequests.Count;
        var voteResponses = h.Transport.VoteResponses.Count;

        // The election timer armed by the election fires after the win; its generation
        // no longer matches. The heartbeat and CheckQuorum timers fire within the same
        // advance; machine-2 answered, so the leader stays, and a leader sends no vote
        // traffic.
        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(1UL);
        h.Transport.VoteRequests.Should().HaveCount(voteRequests);
        h.Transport.VoteResponses.Should().HaveCount(voteResponses);
    }

    [Fact]
    public async Task Votes_from_the_same_peer_count_once() {
        await using var h = await ReplicaHarness.MakeAsync(members: new[] { "machine-1", "machine-2", "machine-3", "machine-4", "machine-5" });
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        h.GrantPreVote("machine-3");
        h.GrantPreVote("machine-4");
        await h.TurnAsync();

        // The same peer grants three times: itself and machine-2 make 2 of the quorum 3.
        h.GrantVote("machine-2");
        h.GrantVote("machine-2");
        h.GrantVote("machine-2");
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Candidate);

        h.GrantVote("machine-3");
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
    }

    [Fact]
    public async Task A_message_from_a_name_outside_the_group_is_dropped() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3, votedFor: "machine-2");
        await h.LoadAsync();

        h.Replica.Receive("machine-9", new VoteRequest(4, 0, 0, PreVote: false));
        h.Replica.Receive("machine-9", new VoteResponse(4, true, PreVote: false));
        h.Replica.Receive("machine-9", new AppendRequest(4, 0, 0, 0, ReadOnlyMemory<Entry>.Empty));
        h.Replica.Receive("machine-9", new AppendResponse(4, AppendResult.Stored, 0, 0));
        await h.TurnAsync();

        h.Transport.VoteResponses.Should().BeEmpty();
        h.Transport.AppendResponses.Should().BeEmpty();
        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(3UL);
        h.Replica.VotedFor.Should().Be("machine-2");
    }
}
