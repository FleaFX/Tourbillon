namespace Tourbillon.Raft;

partial class Replica {
    ITimer? _electionTimer;
    ITimer? _heartbeatTimer;
    ITimer? _checkQuorumTimer;
    ulong _electionTimerGeneration;
    ulong _heartbeatTimerGeneration;
    ulong _checkQuorumTimerGeneration;

    ulong _preVoteTerm;
    readonly HashSet<string> _preVotes = new();
    readonly HashSet<string> _grantedVotes = new();

    /// <summary>
    /// Arms the election timer with a fresh random timeout between the minimum and the
    /// maximum; the previous timer is disposed and its deadline is void.
    /// </summary>
    void ArmElectionTimer() {
        _electionTimer?.Dispose();
        var generation = ++_electionTimerGeneration;
        _electionTimer = _timeProvider.CreateTimer(
            static state => {
                var (replica, generation) = ((Replica, ulong))state!;
                replica._mailbox.Writer.TryWrite(new ElectionTimeout(generation));
            },
            (this, generation),
            NextElectionTimeout(),
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Invalidates the election timer: a pending firing still posts its message, with a
    /// generation that no longer matches, and the turn ignores it.
    /// </summary>
    void InvalidateElectionTimer() =>
        _electionTimerGeneration++;

    /// <summary>
    /// Arms the heartbeat timer with the fixed interval; the previous timer is disposed
    /// and its deadline is void.
    /// </summary>
    void ArmHeartbeatTimer() {
        _heartbeatTimer?.Dispose();
        var generation = ++_heartbeatTimerGeneration;
        _heartbeatTimer = _timeProvider.CreateTimer(
            static state => {
                var (replica, generation) = ((Replica, ulong))state!;
                replica._mailbox.Writer.TryWrite(new Heartbeat(generation));
            },
            (this, generation),
            _options.HeartbeatInterval,
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Invalidates the heartbeat timer: a pending firing still posts its message, with a
    /// generation that no longer matches, and the turn ignores it.
    /// </summary>
    void InvalidateHeartbeatTimer() =>
        _heartbeatTimerGeneration++;

    /// <summary>
    /// Arms the CheckQuorum timer with the minimum election timeout; the previous timer
    /// is disposed and its deadline is void.
    /// </summary>
    void ArmCheckQuorumTimer() {
        _checkQuorumTimer?.Dispose();
        var generation = ++_checkQuorumTimerGeneration;
        _checkQuorumTimer = _timeProvider.CreateTimer(
            static state => {
                var (replica, generation) = ((Replica, ulong))state!;
                replica._mailbox.Writer.TryWrite(new CheckQuorum(generation));
            },
            (this, generation),
            _options.ElectionTimeoutMin,
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Invalidates the CheckQuorum timer: a pending firing still posts its message, with
    /// a generation that no longer matches, and the turn ignores it.
    /// </summary>
    void InvalidateCheckQuorumTimer() =>
        _checkQuorumTimerGeneration++;

    /// <summary>
    /// A random election timeout between the minimum and the maximum.
    /// </summary>
    TimeSpan NextElectionTimeout() =>
        TimeSpan.FromTicks(_random.NextInt64(_options.ElectionTimeoutMin.Ticks, _options.ElectionTimeoutMax.Ticks));

    /// <summary>
    /// The election timer fired: a follower or a candidate starts a PreVote trial for
    /// the next term — it asks every voter and changes nothing; a leader ignores the
    /// firing, and so does a stale one.
    /// </summary>
    async ValueTask HandleElectionTimeoutAsync(ElectionTimeout message) {
        if (message.Generation != _electionTimerGeneration)
            return;

        if (_role == ReplicaRole.Leader)
            return;

        _preVoteTerm = _term + 1;
        _preVotes.Clear();
        _preVotes.Add(_name);

        var request = new VoteRequest(_preVoteTerm, _log.LastIndex, LastTerm, PreVote: true);
        foreach (var member in _members) {
            if (member != _name)
                _transport.Send(member, request);
        }

        // Re-arm for the round after this one: an undecided trial resolves itself
        // through this timer firing again, with a fresh random timeout.
        ArmElectionTimer();

        // A one-replica group wins its trial on its own.
        if (_preVotes.Count >= Quorum)
            await StartElectionAsync();
    }

    /// <summary>
    /// Starts the real election of the trial's term: the term and the vote for this replica
    /// are stored before any request is sent, and a majority of the votes already
    /// gathered (the own one) makes a one-replica group leader at once.
    /// </summary>
    async ValueTask StartElectionAsync() {
        _term++;
        _votedFor = _name;
        await _votes.SaveAsync(_term, _votedFor);

        _role = ReplicaRole.Candidate;
        _leader = null;
        _isReady = false;
        _preVoteTerm = 0;
        _preVotes.Clear();
        _grantedVotes.Clear();
        _grantedVotes.Add(_name);

        // Re-arm for an election that stays undecided.
        ArmElectionTimer();

        if (_grantedVotes.Count >= Quorum) {
            BecomeLeader();
            return;
        }

        var request = new VoteRequest(_term, _log.LastIndex, LastTerm, PreVote: false);
        foreach (var member in _members) {
            if (member != _name)
                _transport.Send(member, request);
        }
    }

    /// <summary>
    /// Answers a vote request. A PreVote is granted when the asked term is above this
    /// replica's, its log is not behind, this replica is not a leader and it heard no leader
    /// within the minimum election timeout; it stores nothing and changes no timer. A
    /// real vote is granted as the paper says: one per term, for a log at least as
    /// complete, and stored before the answer is sent.
    /// </summary>
    async ValueTask HandleVoteRequestAsync(ReceivedVoteRequest message) {
        var request = message.Message;

        if (request.PreVote) {
            var grant = request.Term > _term
                && _role != ReplicaRole.Leader
                && !HeardLeaderWithin(_options.ElectionTimeoutMin)
                && IsAtLeastAsUpToDate(request.LastTerm, request.LastIndex, LastTerm, _log.LastIndex);

            _transport.Send(message.From, new VoteResponse(_term, grant, PreVote: true));
            return;
        }

        if (request.Term < _term) {
            _transport.Send(message.From, new VoteResponse(_term, false, PreVote: false));
            return;
        }

        if (request.Term > _term)
            await BecomeFollowerAsync(request.Term, leader: null);

        var granted = (_votedFor is null || _votedFor == message.From)
            && IsAtLeastAsUpToDate(request.LastTerm, request.LastIndex, LastTerm, _log.LastIndex);

        if (granted && _votedFor != message.From) {
            _votedFor = message.From;
            await _votes.SaveAsync(_term, _votedFor);
        }

        if (granted)
            ArmElectionTimer();

        _transport.Send(message.From, new VoteResponse(_term, granted, PreVote: false));
    }

    /// <summary>
    /// Counts a vote response: a higher term makes this replica a follower of that term; a
    /// PreVote grant counts only for the trial of the next term, a real grant only in
    /// the election of the current term; a majority starts the election or makes this
    /// replica the leader.
    /// </summary>
    async ValueTask HandleVoteResponseAsync(ReceivedVoteResponse message) {
        var response = message.Message;

        if (response.Term > _term) {
            await BecomeFollowerAsync(response.Term, leader: null);
            return;
        }

        if (response.PreVote) {
            if (!response.Granted || _preVoteTerm != _term + 1)
                return;

            _preVotes.Add(message.From);
            if (_preVotes.Count >= Quorum)
                await StartElectionAsync();

            return;
        }

        if (!response.Granted || _role != ReplicaRole.Candidate || response.Term != _term)
            return;

        _grantedVotes.Add(message.From);
        if (_grantedVotes.Count >= Quorum)
            BecomeLeader();
    }

    /// <summary>
    /// Makes this replica a follower of <paramref name="term"/> with <paramref name="leader"/>
    /// as its leader hint: a higher term is stored with the vote reset, the pending
    /// proposals end, and the election timer is armed.
    /// </summary>
    async ValueTask BecomeFollowerAsync(ulong term, string? leader) {
        if (term > _term) {
            _term = term;
            _votedFor = null;
            await _votes.SaveAsync(_term, _votedFor);
        }

        _role = ReplicaRole.Follower;
        _isReady = false;
        _leader = leader;
        _preVoteTerm = 0;
        _preVotes.Clear();
        _grantedVotes.Clear();
        _noOpPending = false;
        _followers.Clear();
        _quorumResponses.Clear();
        InvalidateHeartbeatTimer();
        InvalidateCheckQuorumTimer();
        FailPendingProposals(ProposalResult.Refused(RefusalReason.NotLeader, _leader));
        ArmElectionTimer();
    }

    /// <summary>
    /// Whether this replica heard a valid append request within <paramref name="within"/>.
    /// </summary>
    bool HeardLeaderWithin(TimeSpan within) =>
        _lastLeaderContact != default && _timeProvider.GetUtcNow() - _lastLeaderContact < within;

    /// <summary>
    /// Whether a candidate's log, at (<paramref name="candidateLastTerm"/>,
    /// <paramref name="candidateLastIndex"/>), is at least as complete as this replica's,
    /// at (<paramref name="ownLastTerm"/>, <paramref name="ownLastIndex"/>).
    /// </summary>
    static bool IsAtLeastAsUpToDate(ulong candidateLastTerm, ulong candidateLastIndex, ulong ownLastTerm, ulong ownLastIndex) =>
        candidateLastTerm > ownLastTerm || (candidateLastTerm == ownLastTerm && candidateLastIndex >= ownLastIndex);
}
