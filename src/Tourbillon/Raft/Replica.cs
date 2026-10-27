using System.Threading.Channels;

namespace Tourbillon.Raft;

/// <summary>
/// One replica of a Raft group: a single-mailbox actor that owns all state of the replica.
/// Callers may invoke the public API concurrently without locking — every operation is
/// a message posted to the actor's mailbox, or a proposal in the actor's own queue, and
/// one turn at a time processes them in arrival order. An exception of the log, the
/// vote store or anything else in a turn breaks the replica: it stops its timers, drops
/// every message and refuses every proposal, and behaves as a crashed one for the
/// group.
/// </summary>
sealed partial class Replica(
    string name,
    IReadOnlyList<string> members,
    IRaftLog log,
    IVoteStore votes,
    ITransport transport,
    ReplicaOptions options,
    TimeProvider timeProvider,
    Random random
) : IAsyncDisposable {
    /// <summary>
    /// The most messages one turn handles before it flushes; a busy mailbox cannot
    /// starve the flush.
    /// </summary>
    const int MaxMessagesPerTurn = 1024;

    readonly string _name = name;
    readonly IReadOnlyList<string> _members = members;
    readonly HashSet<string> _memberSet = new(members);
    readonly IRaftLog _log = log;
    readonly IVoteStore _votes = votes;
    readonly ITransport _transport = transport;
    readonly ReplicaOptions _options = options;
    readonly TimeProvider _timeProvider = timeProvider;
    readonly Random _random = random;

    readonly Channel<Message> _mailbox = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions { SingleReader = true });
    readonly object _proposalsLock = new();
    readonly List<PendingProposal> _waitingProposals = new();
    readonly Dictionary<ulong, PendingProposal> _writtenProposals = new();

    ReplicaRole _role = ReplicaRole.Follower;
    ulong _term;
    string? _votedFor;
    ulong _commitIndex;
    string? _leader;
    bool _isReady;
    Exception? _fault;
    DateTimeOffset _lastLeaderContact;
    bool _disposed;
    CancellationTokenSource? _cts;
    Task? _loop;
    Feed? _feed;
    TaskCompletionSource<bool>? _feedReader;

    /// <summary>
    /// The role of this replica: follower, candidate or leader.
    /// </summary>
    public ReplicaRole Role => _role;

    /// <summary>
    /// The current term of this replica.
    /// </summary>
    public ulong Term => _term;

    /// <summary>
    /// The replica this replica voted for in the current term, or null.
    /// </summary>
    public string? VotedFor => _votedFor;

    /// <summary>
    /// The highest index this replica knows is committed.
    /// </summary>
    public ulong CommitIndex => Volatile.Read(ref _commitIndex);

    /// <summary>
    /// The replica this replica believes is the leader, or null during an election.
    /// </summary>
    public string? Leader => _leader;

    /// <summary>
    /// Whether this replica is the leader and the NoOp of its term is committed; only then
    /// may it write proposals.
    /// </summary>
    public bool IsReady => _isReady;

    /// <summary>
    /// The exception that broke this replica, or null while it runs; a broken replica stops
    /// its timers, drops every message and refuses every proposal.
    /// </summary>
    public Exception? Fault => Volatile.Read(ref _fault);

    /// <summary>
    /// The feed of the committed entries of this replica: from the index after the one
    /// the application named when it opened the group, in log order; null until the replica is
    /// loaded.
    /// </summary>
    public Feed? Feed => _feed;

    /// <summary>
    /// Whether this replica has stopped; a stopped replica ends its feed.
    /// </summary>
    public bool Stopped => Volatile.Read(ref _disposed);

    /// <summary>
    /// Proposes a payload to the group. The returned task completes with the outcome of
    /// the proposal: committed once a majority has the entry on disk, refused when it is certain
    /// the entry is not written by this replica, unknown when this replica lost track of a
    /// written entry. A broken replica refuses at once.
    /// </summary>
    public ValueTask<ProposalResult> ProposeAsync(
        ReadOnlyMemory<byte> payload,
        bool mayUseReserve,
        CancellationToken cancellationToken = default
    ) {
        if (_fault is not null)
            return new ValueTask<ProposalResult>(ProposalResult.Refused(RefusalReason.WriteFailed));

        var proposal = new PendingProposal(payload.ToArray(), mayUseReserve);

        // Registered before the proposal is queued: the turn that ends it disposes the
        // registration, so the registration must exist by then.
        if (cancellationToken.CanBeCanceled) {
            proposal.Cancellation = cancellationToken.Register(
                static state => {
                    var (replica, proposal) = ((Replica, PendingProposal))state!;
                    replica._mailbox.Writer.TryWrite(new ProposalCancelled(proposal));
                },
                (this, proposal));
        }

        lock (_proposalsLock) {
            // The first proposal of a turn posts one wake-up; the rest pile up behind it.
            var wasEmpty = _waitingProposals.Count == 0;
            _waitingProposals.Add(proposal);
            if (wasEmpty)
                _mailbox.Writer.TryWrite(new WakeUp());
        }

        return new ValueTask<ProposalResult>(proposal.Completion);
    }

    /// <summary>
    /// Receives a vote request from a member of the group; a message from a name outside
    /// the group, or any message of a broken replica, is dropped.
    /// </summary>
    public void Receive(string from, VoteRequest message) {
        if (_fault is not null || !_memberSet.Contains(from))
            return;

        _mailbox.Writer.TryWrite(new ReceivedVoteRequest(from, message));
    }

    /// <summary>
    /// Receives a vote response from a member of the group; a message from a name
    /// outside the group, or any message of a broken replica, is dropped.
    /// </summary>
    public void Receive(string from, VoteResponse message) {
        if (_fault is not null || !_memberSet.Contains(from))
            return;

        _mailbox.Writer.TryWrite(new ReceivedVoteResponse(from, message));
    }

    /// <summary>
    /// Receives an append request from a member of the group; a message from a name
    /// outside the group, or any message of a broken replica, is dropped. The entries are
    /// copied: the memory of a message is only valid during the call.
    /// </summary>
    public void Receive(string from, AppendRequest message) {
        if (_fault is not null || !_memberSet.Contains(from))
            return;

        var entries = new Entry[message.Entries.Length];
        for (var i = 0; i < entries.Length; i++) {
            var entry = message.Entries.Span[i];
            entries[i] = new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray());
        }

        _mailbox.Writer.TryWrite(new ReceivedAppendRequest(from, message.Term, message.PrevIndex, message.PrevTerm, message.Commit, entries));
    }

    /// <summary>
    /// Receives an append response from a member of the group; a message from a name
    /// outside the group, or any message of a broken replica, is dropped.
    /// </summary>
    public void Receive(string from, AppendResponse message) {
        if (_fault is not null || !_memberSet.Contains(from))
            return;

        _mailbox.Writer.TryWrite(new ReceivedAppendResponse(from, message));
    }

    /// <summary>
    /// Loads the term and vote, opens the feed at the index the application applied, arms the
    /// election timer and starts the loop that runs one turn per message.
    /// </summary>
    public async ValueTask StartAsync(ulong applied, CancellationToken cancellationToken = default) {
        await LoadAsync(applied, cancellationToken);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = RunLoopAsync(_cts.Token);
    }

    /// <summary>
    /// Loads the term and vote, opens the feed at the index the application applied — the
    /// entries above it are handed out, nothing at or below it is repeated — starts
    /// with nothing committed and no leader heard, and arms the election timer,
    /// without the loop; the simulation runs the turns.
    /// </summary>
    public async ValueTask LoadAsync(ulong applied, CancellationToken cancellationToken = default) {
        if (applied > _log.LastIndex)
            throw new ArgumentOutOfRangeException(nameof(applied), applied, $"The application applied up to index {applied}, past the last index {_log.LastIndex} of this log: an applied entry was committed, and a replica only applies what it has on disk, so the application's state is ahead of this log (lost or wrong files).");
        if (applied + 1 < _log.FirstIndex)
            throw new ArgumentOutOfRangeException(nameof(applied), applied, $"The application applied up to index {applied}, before the first index {_log.FirstIndex} of this log: the log let go of entries the application still needs.");

        var (term, votedFor) = await _votes.LoadAsync(cancellationToken);
        _term = term;
        _votedFor = votedFor;
        _role = ReplicaRole.Follower;
        _commitIndex = 0;
        _lastLeaderContact = default;
        _feed = new Feed(this, _log, applied, _options.PoisonFeed);
        ArmElectionTimer();
    }

    /// <summary>
    /// Runs one turn: the messages in the mailbox, at most
    /// <see cref="MaxMessagesPerTurn"/>, then the flush of the waiting proposals. A
    /// broken replica drops every message; an exception of the log, the vote store or
    /// anything else breaks the replica.
    /// </summary>
    /// <returns>Whether the turn did any work.</returns>
    public async ValueTask<bool> RunTurnAsync() {
        if (_fault is not null) {
            while (_mailbox.Reader.TryRead(out _))
                ;

            return false;
        }

        try {
            var handled = 0;
            while (handled < MaxMessagesPerTurn && _mailbox.Reader.TryRead(out var message)) {
                await HandleAsync(message);
                handled++;
            }

            var flushed = await FlushAsync();
            return handled > 0 || flushed;
        } catch (Exception exception) {
            Break(exception);
            return true;
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync() {
        if (Volatile.Read(ref _disposed))
            return;

        Volatile.Write(ref _disposed, true);
        _cts?.Cancel();
        if (_loop is { } loop)
            await loop;
        _electionTimer?.Dispose();
        _heartbeatTimer?.Dispose();
        _checkQuorumTimer?.Dispose();
        _cts?.Dispose();
        FailPendingProposals(ProposalResult.Refused(RefusalReason.NotLeader, _leader));
        Volatile.Read(ref _feedReader)?.TrySetResult(false);
        _mailbox.Writer.TryComplete();
    }

    /// <summary>
    /// The number of votes that makes a majority, this replica included.
    /// </summary>
    int Quorum => _members.Count / 2 + 1;

    /// <summary>
    /// The term of the last entry of the log; 0 when the log is empty.
    /// </summary>
    ulong LastTerm => _log.GetTerm(_log.LastIndex);

    async Task RunLoopAsync(CancellationToken cancellationToken) {
        try {
            while (await _mailbox.Reader.WaitToReadAsync(cancellationToken))
                await RunTurnAsync();
        } catch (OperationCanceledException) {
        }
    }

    async ValueTask HandleAsync(Message message) {
        switch (message) {
            case ElectionTimeout timeout:
                await HandleElectionTimeoutAsync(timeout);
                break;
            case ReceivedVoteRequest request:
                await HandleVoteRequestAsync(request);
                break;
            case ReceivedVoteResponse response:
                await HandleVoteResponseAsync(response);
                break;
            case ProposalCancelled cancelled:
                HandleProposalCancelled(cancelled);
                break;
            case Heartbeat heartbeat:
                await HandleHeartbeatAsync(heartbeat);
                break;
            case CheckQuorum checkQuorum:
                await HandleCheckQuorumAsync(checkQuorum);
                break;
            case ReceivedAppendRequest request:
                await HandleAppendRequestAsync(request);
                break;
            case ReceivedAppendResponse response:
                await HandleAppendResponseAsync(response);
                break;
            // WakeUp needs no handler of its own: the flush at the end of the turn
            // takes the proposals.
        }
    }

    /// <summary>
    /// Breaks the replica: <see cref="Fault"/> holds the exception, every timer stops,
    /// every message is dropped, and the pending proposals end — the waiting ones
    /// refused, the written and the in-flight ones unknown. A broken replica behaves as
    /// a crashed one for the group.
    /// </summary>
    void Break(Exception fault) {
        if (Volatile.Read(ref _fault) is null)
            Volatile.Write(ref _fault, fault);
        _electionTimer?.Dispose();
        _heartbeatTimer?.Dispose();
        _checkQuorumTimer?.Dispose();
        _electionTimer = null;
        _heartbeatTimer = null;
        _checkQuorumTimer = null;
        FailPendingProposals(ProposalResult.Refused(RefusalReason.WriteFailed));
        // The feed throws the fault: a waiting reader is woken to see it.
        Volatile.Read(ref _feedReader)?.TrySetResult(true);
    }

    /// <summary>
    /// Registers the feed's waiting reader: the next commit, stop or fault completes
    /// it. A reader that is not waiting costs a commit nothing.
    /// </summary>
    public void RegisterFeedReader(TaskCompletionSource<bool> reader) =>
        Volatile.Write(ref _feedReader, reader);

    /// <summary>
    /// Withdraws the feed's waiting reader, when it is still the one registered.
    /// </summary>
    public void UnregisterFeedReader(TaskCompletionSource<bool> reader) {
        if (Volatile.Read(ref _feedReader) == reader)
            Volatile.Write(ref _feedReader, null);
    }

    /// <summary>
    /// Ends every pending proposal of a replica that stops, steps down or breaks: the
    /// waiting ones (never written) complete with <paramref name="refusal"/>, and the
    /// written and the in-flight ones end unknown.
    /// </summary>
    void FailPendingProposals(ProposalResult refusal) {
        List<PendingProposal>? waiting = null;
        List<PendingProposal>? written = null;
        List<PendingProposal?>? appending = null;
        lock (_proposalsLock) {
            if (_waitingProposals.Count > 0) {
                waiting = new List<PendingProposal>(_waitingProposals);
                _waitingProposals.Clear();
            }

            if (_writtenProposals.Count > 0) {
                written = new List<PendingProposal>(_writtenProposals.Values);
                _writtenProposals.Clear();
            }

            if (_appending is { } batch)
                appending = new List<PendingProposal?>(batch);
        }

        if (waiting is not null)
            foreach (var proposal in waiting)
                proposal.Complete(refusal);
        if (written is not null)
            foreach (var proposal in written)
                proposal.Complete(ProposalResult.Unknown);
        if (appending is not null)
            foreach (var proposal in appending)
                proposal?.Complete(ProposalResult.Unknown);
    }

    /// <summary>
    /// Ends a proposal whose caller cancelled: unknown. A proposal that already has an
    /// outcome is left alone; its index may belong to another proposal by now.
    /// </summary>
    void HandleProposalCancelled(ProposalCancelled message) {
        var proposal = message.Proposal;
        if (proposal.IsCompleted)
            return;

        lock (_proposalsLock) {
            if (proposal.Index != 0)
                _writtenProposals.Remove(proposal.Index);
            else
                _waitingProposals.Remove(proposal);
        }

        proposal.Complete(ProposalResult.Unknown);
    }

    /// <summary>
    /// One proposal waiting for its outcome: the payload copied at the call, the task the
    /// caller awaits, and the index once the entry is written.
    /// </summary>
    sealed class PendingProposal {
        readonly TaskCompletionSource<ProposalResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// A proposal with its payload copied; the index is 0 until the entry is written.
        /// </summary>
        public PendingProposal(byte[] payload, bool mayUseReserve) {
            Payload = payload;
            MayUseReserve = mayUseReserve;
        }

        /// <summary>
        /// The payload, copied at the call.
        /// </summary>
        public byte[] Payload { get; }

        /// <summary>
        /// Whether the entry may use the storage reserve.
        /// </summary>
        public bool MayUseReserve { get; }

        /// <summary>
        /// The task the caller awaits.
        /// </summary>
        public Task<ProposalResult> Completion => _completion.Task;

        /// <summary>
        /// The index of the written entry; 0 while the proposal is not written.
        /// </summary>
        public ulong Index { get; set; }

        /// <summary>
        /// The registration of the caller's cancellation token.
        /// </summary>
        public CancellationTokenRegistration Cancellation { get; set; }

        /// <summary>
        /// Whether the proposal has an outcome.
        /// </summary>
        public bool IsCompleted => _completion.Task.IsCompleted;

        /// <summary>
        /// Ends the proposal; the first outcome wins.
        /// </summary>
        public void Complete(ProposalResult result) {
            Cancellation.Dispose();
            _completion.TrySetResult(result);
        }
    }

    /// <summary>
    /// A message posted to the mailbox: a timer firing, a received message, a wake-up or
    /// a cancelled proposal. Every mutation of the replica's state flows through here,
    /// one at a time, in arrival order.
    /// </summary>
    abstract record Message;

    /// <summary>
    /// The election timer fired; the generation tells a current firing from a stale one.
    /// </summary>
    sealed record ElectionTimeout(ulong Generation) : Message;

    /// <summary>
    /// The heartbeat timer fired; the generation tells a current firing from a stale one.
    /// </summary>
    sealed record Heartbeat(ulong Generation) : Message;

    /// <summary>
    /// The CheckQuorum timer fired; the generation tells a current firing from a stale
    /// one.
    /// </summary>
    sealed record CheckQuorum(ulong Generation) : Message;

    /// <summary>
    /// The first proposal of a turn; the flush at the end of the turn takes the
    /// proposals.
    /// </summary>
    sealed record WakeUp : Message;

    /// <summary>
    /// A vote request received from a member.
    /// </summary>
    sealed record ReceivedVoteRequest(string From, VoteRequest Message) : Message;

    /// <summary>
    /// A vote response received from a member.
    /// </summary>
    sealed record ReceivedVoteResponse(string From, VoteResponse Message) : Message;

    /// <summary>
    /// An append request received from a member; the entries are copied at the receive.
    /// </summary>
    sealed record ReceivedAppendRequest(string From, ulong Term, ulong PrevIndex, ulong PrevTerm, ulong Commit, Entry[] Entries) : Message;

    /// <summary>
    /// An append response received from a member.
    /// </summary>
    sealed record ReceivedAppendResponse(string From, AppendResponse Message) : Message;

    /// <summary>
    /// The caller of a proposal cancelled it.
    /// </summary>
    sealed record ProposalCancelled(PendingProposal Proposal) : Message;
}
