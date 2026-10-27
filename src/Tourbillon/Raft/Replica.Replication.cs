using System.Buffers;

namespace Tourbillon.Raft;

partial class Replica {
    List<PendingProposal?>? _appending;

    /// <summary>
    /// The flush at the end of a turn. Not the leader: every waiting proposal is
    /// refused, with the leader's name when known. The leader: the NoOp of its term and
    /// the waiting proposals go out as one batch, one append, one replication round.
    /// While the ready gate is closed the proposals wait: they are not written. A
    /// proposal a majority has no room for is refused before it is written, storage
    /// full. A NoOp the own log has no room for refuses the waiting proposals, storage
    /// full: they are in no log, and the gate stays closed. The NoOp waits for the next
    /// turn, the heartbeat at the latest; a flush that wrote nothing and ended nothing
    /// reports no work. An append that fails without a fault refuses the proposals of
    /// the batch — the entries are in no log — and the NoOp stays pending; a faulted
    /// log breaks the replica: the in-flight batch ends unknown.
    /// </summary>
    /// <returns>Whether the flush did any work.</returns>
    async ValueTask<bool> FlushAsync() {
        if (_role != ReplicaRole.Leader)
            return RefuseWaitingProposals(ProposalResult.Refused(RefusalReason.NotLeader, _leader));

        // A flush is in flight: the proposals pile up for the next one.
        if (_appending is not null)
            return false;

        List<PendingProposal?>? batch = null;
        List<PendingProposal>? noRoom = null;
        lock (_proposalsLock) {
            if (_noOpPending) {
                // The NoOp of the new term goes out first, alone while the ready gate is
                // closed: the proposals wait until it commits.
                batch = new List<PendingProposal?> { null };
            }

            if (_isReady) {
                var index = _noOpPending ? _noOpIndex + 1 : _log.LastIndex + 1;
                foreach (var proposal in _waitingProposals) {
                    if (proposal.IsCompleted)
                        continue;  // cancelled while waiting: never written

                    // Refused before writing when a majority has no room for the entry.
                    if (!HasRoomFor(proposal.MayUseReserve)) {
                        (noRoom ??= new List<PendingProposal>()).Add(proposal);
                        continue;
                    }

                    proposal.Index = index++;
                    (batch ??= new List<PendingProposal?>()).Add(proposal);
                }

                _waitingProposals.Clear();
            }
        }

        if (noRoom is not null)
            foreach (var proposal in noRoom)
                proposal.Complete(ProposalResult.Refused(RefusalReason.StorageFull));

        if (batch is null || batch.Count == 0)
            return noRoom is not null;

        // One append of the batch, in a pooled array: the entries beyond the count the
        // log takes are refused, storage full.
        lock (_proposalsLock)
            _appending = batch;

        var rented = ArrayPool<Entry>.Shared.Rent(batch.Count);
        try {
            for (var i = 0; i < batch.Count; i++) {
                if (batch[i] is { } proposal) {
                    rented[i] = new Entry(
                        proposal.Index,
                        _term,
                        EntryKind.Command,
                        proposal.MayUseReserve ? EntryFlags.MayUseReserve : EntryFlags.None,
                        proposal.Payload);
                } else {
                    rented[i] = new Entry(_noOpIndex, _term, EntryKind.NoOp, EntryFlags.None, ReadOnlyMemory<byte>.Empty);
                }
            }

            int appended;
            try {
                appended = await _log.AppendAsync(rented.AsMemory(0, batch.Count)).ConfigureAwait(false);
            } catch (IOException) {
                // The write was cut back: the entries of the batch are in no log, so
                // the proposals are refused and nothing is sent; the NoOp stays
                // pending. A faulted log broke the replica: the in-flight batch ends
                // unknown here — it is in no list — and the exception breaks the turn.
                lock (_proposalsLock)
                    _appending = null;
                if (_log.IsFaulted) {
                    foreach (var proposal in batch)
                        proposal?.Complete(ProposalResult.Unknown);
                    throw;
                }

                var ended = false;
                foreach (var proposal in batch) {
                    if (proposal is { } failed) {
                        failed.Index = 0;
                        failed.Complete(ProposalResult.Refused(RefusalReason.WriteFailed));
                        ended = true;
                    }
                }

                return ended;
            }

            List<PendingProposal>? written = null;
            List<PendingProposal>? refused = null;
            lock (_proposalsLock) {
                _appending = null;

                for (var i = 0; i < batch.Count; i++) {
                    if (batch[i] is not { } proposal)
                        continue;

                    if (i < appended) {
                        if (!proposal.IsCompleted) {
                            (written ??= new List<PendingProposal>()).Add(proposal);
                            _writtenProposals.Add(proposal.Index, proposal);
                        }
                    } else {
                        proposal.Index = 0;
                        (refused ??= new List<PendingProposal>()).Add(proposal);
                    }
                }
            }

            var refusedWaiting = false;
            if (_noOpPending && appended > 0)
                _noOpPending = false;
            else if (_noOpPending)
                refusedWaiting = RefuseWaitingProposals(ProposalResult.Refused(RefusalReason.StorageFull));

            if (refused is not null)
                foreach (var proposal in refused)
                    proposal.Complete(ProposalResult.Refused(RefusalReason.StorageFull));

            // The leader counts itself only now: its own append has completed.
            if (_role == ReplicaRole.Leader) {
                TryCommit();
                await SendAppendRequestsAsync().ConfigureAwait(false);
            }

            // Work is what the turn consumed: entries written, or proposals ended.
            return appended > 0 || noRoom is not null || refused is not null || refusedWaiting;
        } finally {
            ArrayPool<Entry>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Refuses every waiting proposal with <paramref name="result"/>: the entries are in
    /// no log, and a refusal is final.
    /// </summary>
    /// <returns>Whether any proposal was refused.</returns>
    bool RefuseWaitingProposals(ProposalResult result) {
        List<PendingProposal>? refused = null;
        lock (_proposalsLock) {
            if (_waitingProposals.Count > 0) {
                refused = new List<PendingProposal>(_waitingProposals);
                _waitingProposals.Clear();
            }
        }

        if (refused is null)
            return false;

        foreach (var proposal in refused)
            proposal.Complete(result);

        return true;
    }

    /// <summary>
    /// Tries to commit, as Figure 8: the highest index with a majority of the followers
    /// matching at or beyond it, of the current term. The loop is bounded by the last
    /// index of the log, so the leader counts itself only after its own append
    /// completed.
    /// </summary>
    void TryCommit() {
        for (var candidate = _log.LastIndex; candidate > _commitIndex; candidate--) {
            var replicated = 1;  // this replica
            foreach (var follower in _followers.Values) {
                if (follower.Match >= candidate)
                    replicated++;
            }

            if (replicated < Quorum)
                continue;

            if (_log.GetTerm(candidate) != _term)
                return;  // an entry of an earlier term commits only with one of this term

            Commit(candidate);
            return;
        }
    }

    /// <summary>
    /// Commits up to <paramref name="index"/>: the ready gate opens when the NoOp of the
    /// term is committed, and the written proposals up to the index complete as committed. The
    /// commit index is published for the feed, which wakes a waiting reader; a reader
    /// that is not waiting costs the commit nothing.
    /// </summary>
    void Commit(ulong index) {
        if (index <= _commitIndex)
            return;

        Volatile.Write(ref _commitIndex, index);
        if (_role == ReplicaRole.Leader && !_isReady && index >= _noOpIndex)
            _isReady = true;

        List<PendingProposal>? committed = null;
        lock (_proposalsLock) {
            foreach (var (proposalIndex, proposal) in _writtenProposals) {
                if (proposalIndex <= index)
                    (committed ??= new List<PendingProposal>()).Add(proposal);
            }

            if (committed is not null)
                foreach (var proposal in committed)
                    _writtenProposals.Remove(proposal.Index);
        }

        if (committed is not null)
            foreach (var proposal in committed)
                proposal.Complete(ProposalResult.Committed(proposal.Index));

        Volatile.Read(ref _feedReader)?.TrySetResult(true);
    }

    /// <summary>
    /// One replication round: an append request to every follower.
    /// </summary>
    async ValueTask SendAppendRequestsAsync() {
        foreach (var follower in _followers.Keys)
            await SendAppendRequestAsync(follower).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends one append request to a follower: the entries from its next index up to the
    /// last index, read through the log into a pooled buffer, or a heartbeat when there
    /// are none. The transport copies during the send.
    /// </summary>
    async ValueTask SendAppendRequestAsync(string follower) {
        if (!_followers.TryGetValue(follower, out var state))
            return;

        var prevIndex = Math.Min(state.Next - 1, _log.LastIndex);
        var prevTerm = _log.GetTerm(prevIndex);
        var count = state.Next > _log.LastIndex ? 0UL : _log.LastIndex - state.Next + 1;
        if (count == 0) {
            _transport.Send(follower, new AppendRequest(_term, prevIndex, prevTerm, _commitIndex, ReadOnlyMemory<Entry>.Empty));
            return;
        }

        // One pooled buffer for the whole request, the records back to back, and one
        // pooled array of entries whose payloads point into it.
        var total = 0;
        for (var index = state.Next; index <= _log.LastIndex; index++)
            total += _log.GetRecordLength(index);

        var buffer = ArrayPool<byte>.Shared.Rent(total);
        var entries = ArrayPool<Entry>.Shared.Rent((int)count);
        try {
            var offset = 0;
            var position = 0;
            for (var index = state.Next; index <= _log.LastIndex; index++) {
                var length = _log.GetRecordLength(index);
                var entry = await _log.ReadAsync(index, buffer.AsMemory(offset, length)).ConfigureAwait(false);
                entries[position++] = entry;
                offset += length;
            }

            _transport.Send(follower, new AppendRequest(_term, prevIndex, prevTerm, _commitIndex, entries.AsMemory(0, position)));
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
            ArrayPool<Entry>.Shared.Return(entries);
        }
    }

    /// <summary>
    /// Handles an append request. A stale term is refused with this replica's term; the same
    /// or a higher term makes this replica a follower of the sender and re-arms the election
    /// timer, and the time of the request counts as "heard a leader". The consistency
    /// check reports a conflict with a hint; entries this replica already has are skipped;
    /// a real conflict truncates — never at or below the commit index, a broken rule
    /// throws — and the new entries are appended as one batch. A write that fails
    /// without a fault leaves this replica silent: the leader sends again with the next
    /// heartbeat. Then the commit index moves to the minimum of the leader's and the
    /// last index this request confirmed.
    /// </summary>
    async ValueTask HandleAppendRequestAsync(ReceivedAppendRequest message) {
        if (message.Term < _term) {
            _transport.Send(message.From, new AppendResponse(_term, AppendResult.Mismatch, 0, 0));
            return;
        }

        if (message.Term > _term || _role != ReplicaRole.Follower)
            await BecomeFollowerAsync(message.Term, message.From);
        else {
            _leader = message.From;
            ArmElectionTimer();
        }

        _lastLeaderContact = _timeProvider.GetUtcNow();

        // The consistency check, with the conflict hint of paper §5.3.
        if (message.PrevIndex > _log.LastIndex) {
            _transport.Send(message.From, new AppendResponse(_term, AppendResult.Mismatch, _log.LastIndex + 1, 0));
            return;
        }

        if (_log.GetTerm(message.PrevIndex) != message.PrevTerm) {
            var conflictTerm = _log.GetTerm(message.PrevIndex);
            var conflictIndex = message.PrevIndex;
            while (conflictIndex > _log.FirstIndex && _log.GetTerm(conflictIndex - 1) == conflictTerm)
                conflictIndex--;

            _transport.Send(message.From, new AppendResponse(_term, AppendResult.Mismatch, conflictIndex, conflictTerm));
            return;
        }

        // Skip the entries this replica already has; truncate only on a real conflict.
        var position = 0;
        while (position < message.Entries.Length && message.PrevIndex + 1 + (ulong)position <= _log.LastIndex) {
            var index = message.PrevIndex + 1 + (ulong)position;
            if (_log.GetTerm(index) != message.Entries[position].Term)
                break;

            position++;
        }

        if (position < message.Entries.Length) {
            var index = message.PrevIndex + 1 + (ulong)position;
            if (index <= _commitIndex)
                throw new InvalidDataException($"The leader {message.From} asks this replica to replace the committed entry at index {index}.");
        }

        // A write failure at a follower is no fault of this replica: no response, and the
        // leader sends again with the next heartbeat. A faulted log breaks the replica.
        var result = AppendResult.Stored;
        var confirmed = message.PrevIndex + (ulong)message.Entries.Length;
        try {
            if (position < message.Entries.Length)
                _log.TruncateFrom(message.PrevIndex + 1 + (ulong)position);

            if (position < message.Entries.Length) {
                var appended = await _log.AppendAsync(message.Entries.AsMemory(position)).ConfigureAwait(false);
                if (appended < message.Entries.Length - position)
                    result = AppendResult.Full;

                confirmed = message.PrevIndex + (ulong)position + (ulong)appended;
            }
        } catch (IOException) {
            if (_log.IsFaulted)
                throw;

            return;
        }

        Commit(Math.Min(message.Commit, confirmed));
        _transport.Send(message.From, new AppendResponse(_term, result, confirmed, 0));
    }

    /// <summary>
    /// Handles an append response. A higher term makes this replica a follower of that term;
    /// a response of an earlier term, or from a follower it does not know, is ignored.
    /// Any response of this term counts as hearing the follower. Stored sets the room
    /// back to free and Full from the first entry the follower did not take; both
    /// advance the match and try to commit. Mismatch moves the next index from the
    /// conflict hint — never at or below the match — and sends again at once.
    /// </summary>
    async ValueTask HandleAppendResponseAsync(ReceivedAppendResponse message) {
        var response = message.Message;

        if (response.Term > _term) {
            await BecomeFollowerAsync(response.Term, leader: null);
            return;
        }

        if (response.Term < _term || _role != ReplicaRole.Leader)
            return;

        if (!_followers.TryGetValue(message.From, out var state))
            return;

        _quorumResponses.Add(message.From);

        switch (response.Result) {
            case AppendResult.Stored:
                if (response.Index > state.Match) {
                    state.Match = response.Index;
                    state.Next = response.Index + 1;
                    state.Room = FollowerRoom.Free;
                }

                TryCommit();
                break;
            case AppendResult.Full:
                state.Match = Math.Max(state.Match, response.Index);
                state.Next = state.Match + 1;
                state.Room = await RoomOfEntryAsync(response.Index + 1).ConfigureAwait(false);
                TryCommit();
                break;
            case AppendResult.Mismatch:
                state.Next = Math.Max(NextFromConflictHint(response), state.Match + 1);
                await SendAppendRequestAsync(message.From).ConfigureAwait(false);
                break;
        }
    }

    /// <summary>
    /// The room of a follower, from the first entry it did not take at
    /// <paramref name="index"/>: an unflagged one leaves it reserve-only, a flagged one
    /// nothing. Past the end of the log it has room.
    /// </summary>
    async ValueTask<FollowerRoom> RoomOfEntryAsync(ulong index) {
        if (index > _log.LastIndex)
            return FollowerRoom.Free;

        var length = _log.GetRecordLength(index);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try {
            var entry = await _log.ReadAsync(index, buffer.AsMemory(0, length)).ConfigureAwait(false);
            return (entry.Flags & EntryFlags.MayUseReserve) != 0 ? FollowerRoom.None : FollowerRoom.ReserveOnly;
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// The next index to send after a conflict, from the hint of paper §5.3: when the
    /// leader holds entries of the conflicting term, right after its last one; otherwise
    /// the follower's first index of that term.
    /// </summary>
    ulong NextFromConflictHint(AppendResponse response) {
        var next = response.Index;
        if (response.ConflictTerm != 0) {
            for (var index = Math.Min(response.Index, _log.LastIndex); index >= _log.FirstIndex; index--) {
                var term = _log.GetTerm(index);
                if (term == response.ConflictTerm) {
                    next = index + 1;
                    break;
                }

                if (term < response.ConflictTerm)
                    break;  // terms never go down: the conflicting term is not in this log
            }
        }

        return next;
    }
}
