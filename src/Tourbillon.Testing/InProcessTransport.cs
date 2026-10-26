using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// A transport that delivers in process: Send calls Receive of the target, which
/// copies the entries of a message. For replicas with real loops in one process — the
/// slow-reader test and the benchmark; the simulation does not use it.
/// </summary>
sealed class InProcessTransport : ITransport {
    readonly string _from;
    readonly Dictionary<string, Replica> _replicas;

    /// <summary>
    /// The transport of one copy: <paramref name="from"/> is the sender every send
    /// starts from, and <paramref name="replicas"/> maps every copy's name to its
    /// replica.
    /// </summary>
    public InProcessTransport(string from, Dictionary<string, Replica> replicas) {
        _from = from;
        _replicas = replicas;
    }

    /// <inheritdoc/>
    public void Send(string peer, VoteRequest message) =>
        _replicas[peer].Receive(_from, message);

    /// <inheritdoc/>
    public void Send(string peer, VoteResponse message) =>
        _replicas[peer].Receive(_from, message);

    /// <inheritdoc/>
    public void Send(string peer, AppendRequest message) =>
        _replicas[peer].Receive(_from, message);

    /// <inheritdoc/>
    public void Send(string peer, AppendResponse message) =>
        _replicas[peer].Receive(_from, message);
}
