using AwesomeAssertions;

namespace Tourbillon.Raft;

public class ReplicaVoteTests {
    [Fact]
    public async Task Refuses_a_stale_term_with_its_own_term() {
        await using var h = await ReplicaHarness.MakeAsync(term: 5, votedFor: "machine-2");
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new VoteRequest(4, 10, 3, PreVote: false));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Peer.Should().Be("machine-2");
        response.Message.Term.Should().Be(5UL);
        response.Message.Granted.Should().BeFalse();
        response.Message.PreVote.Should().BeFalse();
        h.Replica.Term.Should().Be(5UL);
        h.Replica.VotedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task Grants_a_candidate_with_a_log_at_least_as_complete_and_becomes_follower_of_the_higher_term() {
        var transport = new RecordingTransport();
        var sequenced = new SequencedVoteStore(() => transport.VoteResponses.Count > 0);
        await using var h = await ReplicaHarness.MakeAsync(term: 2, votes: sequenced, transport: transport);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new VoteRequest(5, 3, 4, PreVote: false));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Peer.Should().Be("machine-2");
        response.Message.Granted.Should().BeTrue();
        response.Message.Term.Should().Be(5UL);
        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(5UL);
        h.Replica.VotedFor.Should().Be("machine-2");

        // The vote is stored before the answer is sent.
        sequenced.ProbeHits.Should().Be(0);
        var (term, votedFor) = await h.Votes.LoadAsync(TestContext.Current.CancellationToken);
        term.Should().Be(5UL);
        votedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task Grants_a_candidate_with_the_same_log_position() {
        var entries = new[] {
            new Entry(1, 3, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 3, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 3, entries: entries);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new VoteRequest(4, 2, 3, PreVote: false));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Message.Granted.Should().BeTrue();
        h.Replica.VotedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task Refuses_a_second_candidate_in_the_same_term() {
        await using var h = await ReplicaHarness.MakeAsync(term: 5, votedFor: "machine-2");
        await h.LoadAsync();

        h.Replica.Receive("machine-3", new VoteRequest(5, 10, 5, PreVote: false));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Peer.Should().Be("machine-3");
        response.Message.Granted.Should().BeFalse();
        response.Message.Term.Should().Be(5UL);
        h.Replica.VotedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task Refuses_a_candidate_whose_log_is_behind() {
        var entries = new[] {
            new Entry(1, 3, EntryKind.Command, EntryFlags.None, new byte[] { 1 }),
            new Entry(2, 3, EntryKind.Command, EntryFlags.None, new byte[] { 2 }),
            new Entry(3, 3, EntryKind.Command, EntryFlags.None, new byte[] { 3 }),
        };
        await using var h = await ReplicaHarness.MakeAsync(term: 3, entries: entries);
        await h.LoadAsync();

        // Same term, shorter log.
        h.Replica.Receive("machine-2", new VoteRequest(3, 2, 3, PreVote: false));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Message.Granted.Should().BeFalse();
        h.Replica.VotedFor.Should().BeNull();

        // Older term, longer log.
        h.Replica.Receive("machine-2", new VoteRequest(2, 10, 2, PreVote: false));
        await h.TurnAsync();

        h.Transport.VoteResponses.Should().HaveCount(2);
        h.Transport.VoteResponses[1].Message.Granted.Should().BeFalse();
        h.Replica.Term.Should().Be(3UL);
        h.Replica.VotedFor.Should().BeNull();
    }

    [Fact]
    public async Task Re_grants_to_the_same_candidate_without_storing_again() {
        var sequenced = new SequencedVoteStore(() => false);
        await using var h = await ReplicaHarness.MakeAsync(term: 5, votedFor: "machine-2", votes: sequenced);
        await h.LoadAsync();

        h.Replica.Receive("machine-2", new VoteRequest(5, 10, 5, PreVote: false));
        await h.TurnAsync();
        h.Replica.Receive("machine-2", new VoteRequest(5, 10, 5, PreVote: false));
        await h.TurnAsync();

        h.Transport.VoteResponses.Should().HaveCount(2);
        h.Transport.VoteResponses.Should().OnlyContain(r => r.Message.Granted);
        sequenced.Saves.Should().Be(1);  // the seed only: a re-grant stores nothing
        h.Replica.VotedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task APreVote_question_changes_nothing() {
        var answererVotes = new SequencedVoteStore(() => false);
        await using var answerer = await ReplicaHarness.MakeAsync(name: "machine-2", term: 3, votes: answererVotes);
        await using var asker = await ReplicaHarness.MakeAsync(name: "machine-1", term: 3, votedFor: "machine-3");
        await answerer.LoadAsync();
        await asker.LoadAsync();

        // The asker's election timer fires: it asks for term 4 and changes nothing.
        asker.AdvancePastElectionTimeout();
        await asker.TurnAsync();

        var request = asker.Transport.VoteRequests.Should().ContainSingle(r => r.Peer == "machine-2").Which;
        request.Message.PreVote.Should().BeTrue();
        request.Message.Term.Should().Be(4UL);

        // The answerer grants and changes nothing.
        answerer.Replica.Receive("machine-1", request.Message);
        await answerer.TurnAsync();

        var response = answerer.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Message.Granted.Should().BeTrue();
        response.Message.PreVote.Should().BeTrue();

        asker.Replica.Term.Should().Be(3UL);
        asker.Replica.VotedFor.Should().Be("machine-3");
        answerer.Replica.Term.Should().Be(3UL);
        answerer.Replica.VotedFor.Should().BeNull();
        answererVotes.Saves.Should().Be(1);  // the seed only: a PreVote stores nothing
    }

    [Fact]
    public async Task APreVote_question_is_refused_by_a_leader() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();
        await h.ElectAsync();

        h.Replica.Receive("machine-2", new VoteRequest(h.Replica.Term + 1, 0, 0, PreVote: true));
        await h.TurnAsync();

        var response = h.Transport.VoteResponses.Should().ContainSingle().Which;
        response.Message.Granted.Should().BeFalse();
        response.Message.PreVote.Should().BeTrue();
    }

    [Fact]
    public async Task APreVote_refused_by_a_majority_does_not_start_a_real_election() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3);
        await h.LoadAsync();

        // The trial for term 4; the other copies refuse with their own term.
        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        h.Replica.Receive("machine-2", new VoteResponse(3, false, PreVote: true));
        h.Replica.Receive("machine-3", new VoteResponse(3, false, PreVote: true));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(3UL);
        h.Transport.VoteRequests.Should().OnlyContain(r => r.Message.PreVote);
    }

    [Fact]
    public async Task ALate_PreVote_grant_does_not_count_for_the_real_election() {
        await using var h = await ReplicaHarness.MakeAsync();
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();
        h.GrantPreVote("machine-2");
        h.GrantPreVote("machine-3");
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Candidate);
        h.Replica.Term.Should().Be(1UL);

        // A grant of the finished trial arrives late.
        h.Replica.Receive("machine-2", new VoteResponse(0, true, PreVote: true));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Candidate);
        h.Replica.Term.Should().Be(1UL);

        h.GrantVote("machine-2");
        h.GrantVote("machine-3");
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Leader);
        h.Replica.Term.Should().Be(1UL);
    }

    [Fact]
    public async Task A_real_grant_does_not_count_for_a_trial() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3);
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        // A grant of a real vote, meant for another candidate, arrives.
        h.Replica.Receive("machine-2", new VoteResponse(3, true, PreVote: false));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(3UL);
        h.Transport.VoteRequests.Should().OnlyContain(r => r.Message.PreVote);
    }

    [Fact]
    public async Task A_real_vote_request_during_a_trial_makes_the_trier_a_follower_and_starts_no_election() {
        await using var h = await ReplicaHarness.MakeAsync(term: 3);
        await h.LoadAsync();

        h.AdvancePastElectionTimeout();
        await h.TurnAsync();

        // machine-2 starts the real election of term 4 first.
        h.Replica.Receive("machine-2", new VoteRequest(4, 0, 0, PreVote: false));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(4UL);
        h.Replica.VotedFor.Should().Be("machine-2");

        // The trial is over: a late PreVote grant starts nothing.
        h.Replica.Receive("machine-3", new VoteResponse(3, true, PreVote: true));
        await h.TurnAsync();

        h.Replica.Role.Should().Be(ReplicaRole.Follower);
        h.Replica.Term.Should().Be(4UL);
        h.Transport.VoteRequests.Should().OnlyContain(r => r.Message.PreVote);
    }
}
