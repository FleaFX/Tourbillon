namespace Tourbillon.Raft;

sealed class RecordingTransport : ITransport {
    public List<(string Peer, VoteRequest Message)> VoteRequests { get; } = new();

    public List<(string Peer, VoteResponse Message)> VoteResponses { get; } = new();

    public List<(string Peer, AppendRequest Message)> AppendRequests { get; } = new();

    public List<(string Peer, AppendResponse Message)> AppendResponses { get; } = new();

    public int SendCount =>
        VoteRequests.Count + VoteResponses.Count + AppendRequests.Count + AppendResponses.Count;

    public void Send(string peer, VoteRequest message) =>
        VoteRequests.Add((peer, message));

    public void Send(string peer, VoteResponse message) =>
        VoteResponses.Add((peer, message));

    public void Send(string peer, AppendRequest message) {
        // The transport copies what it keeps: the payloads of the entries.
        var entries = new Entry[message.Entries.Length];
        for (var i = 0; i < entries.Length; i++) {
            var entry = message.Entries.Span[i];
            entries[i] = new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray());
        }

        AppendRequests.Add((peer, new AppendRequest(message.Term, message.PrevIndex, message.PrevTerm, message.Commit, entries)));
    }

    public void Send(string peer, AppendResponse message) =>
        AppendResponses.Add((peer, message));
}
