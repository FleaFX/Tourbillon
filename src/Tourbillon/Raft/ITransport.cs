namespace Tourbillon.Raft;

/// <summary>
/// The network as one replica of one group sees it. Sending never waits and may lose the
/// message; the transport copies what it keeps, the memory of a message is only valid
/// during the call.
/// </summary>
interface ITransport {
    /// <summary>
    /// Sends a vote request to <paramref name="peer"/>.
    /// </summary>
    void Send(string peer, VoteRequest message);

    /// <summary>
    /// Sends a vote response to <paramref name="peer"/>.
    /// </summary>
    void Send(string peer, VoteResponse message);

    /// <summary>
    /// Sends an append request to <paramref name="peer"/>.
    /// </summary>
    void Send(string peer, AppendRequest message);

    /// <summary>
    /// Sends an append response to <paramref name="peer"/>.
    /// </summary>
    void Send(string peer, AppendResponse message);
}
