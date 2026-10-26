namespace Tourbillon.Raft;

partial class Replica {
    readonly Dictionary<string, FollowerState> _followers = new();
    readonly HashSet<string> _quorumResponses = new();
    bool _noOpPending;
    ulong _noOpIndex;

    /// <summary>
    /// Makes this copy the leader of its term: every follower starts at the end of the
    /// log with room, the NoOp of the new term is pending, the ready gate stays closed
    /// until that NoOp commits, and the election timer gives way to the heartbeat.
    /// </summary>
    void BecomeLeader() {
        _role = ReplicaRole.Leader;
        _leader = _name;
        _isReady = false;
        _noOpIndex = _log.LastIndex + 1;
        _noOpPending = true;
        _preVoteTerm = 0;
        _preVotes.Clear();
        _followers.Clear();
        foreach (var member in _members) {
            if (member != _name)
                _followers.Add(member, new FollowerState(_log.LastIndex + 1, 0, FollowerRoom.Free));
        }

        _quorumResponses.Clear();
        InvalidateElectionTimer();
        ArmHeartbeatTimer();
        ArmCheckQuorumTimer();
    }

    /// <summary>
    /// The heartbeat timer fired: re-arm it and send an append request to every
    /// follower, a heartbeat where there is nothing new.
    /// </summary>
    async ValueTask HandleHeartbeatAsync(Heartbeat message) {
        if (message.Generation != _heartbeatTimerGeneration)
            return;

        if (_role != ReplicaRole.Leader)
            return;

        ArmHeartbeatTimer();
        await SendAppendRequestsAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// The CheckQuorum timer fired: re-arm it, and step down when no majority answered
    /// in this term since the last check — a leader that cannot hear a majority is not
    /// one. The step-down is to a follower of the same term, leader unknown.
    /// </summary>
    async ValueTask HandleCheckQuorumAsync(CheckQuorum message) {
        if (message.Generation != _checkQuorumTimerGeneration)
            return;

        if (_role != ReplicaRole.Leader)
            return;

        ArmCheckQuorumTimer();
        if (_quorumResponses.Count + 1 < Quorum)
            await BecomeFollowerAsync(_term, leader: null);
        else
            _quorumResponses.Clear();
    }

    /// <summary>
    /// Whether a majority has room for an entry: this copy counts as free — its own log
    /// refuses on its own — and a follower counts when it is free, or reserve-only for
    /// an entry that may use the reserve. A proposal without room is refused before it
    /// is written.
    /// </summary>
    bool HasRoomFor(bool mayUseReserve) {
        var free = 1;  // this copy
        foreach (var follower in _followers.Values) {
            if (follower.Room == FollowerRoom.Free || (mayUseReserve && follower.Room == FollowerRoom.ReserveOnly))
                free++;
        }

        return free >= Quorum;
    }

    /// <summary>
    /// What a follower can still take: free, reserve-only, or nothing.
    /// </summary>
    enum FollowerRoom : byte {
        /// <summary>
        /// The follower takes any entry.
        /// </summary>
        Free = 0,

        /// <summary>
        /// The follower takes only entries that may use the reserve.
        /// </summary>
        ReserveOnly = 1,

        /// <summary>
        /// The follower takes nothing.
        /// </summary>
        None = 2,
    }

    /// <summary>
    /// The replication state of one follower: what to send next, how far it matches, and
    /// how much room it has.
    /// </summary>
    sealed class FollowerState {
        /// <summary>
        /// The state of one follower.
        /// </summary>
        public FollowerState(ulong next, ulong match, FollowerRoom room) {
            Next = next;
            Match = match;
            Room = room;
        }

        /// <summary>
        /// The index of the first entry to send.
        /// </summary>
        public ulong Next { get; set; }

        /// <summary>
        /// The highest index the follower matches.
        /// </summary>
        public ulong Match { get; set; }

        /// <summary>
        /// How much room the follower has.
        /// </summary>
        public FollowerRoom Room { get; set; }
    }
}
