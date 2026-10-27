using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// One replica's view of the network of a <see cref="SimulatedCluster"/>: every send goes
/// to the cluster's <see cref="SimulatedNetwork"/>.
/// </summary>
sealed class SimulatedTransport(string from, SimulatedNetwork network) : ITransport {
    /// <inheritdoc/>
    public void Send(string peer, VoteRequest message) =>
        network.Send(from, peer, message);

    /// <inheritdoc/>
    public void Send(string peer, VoteResponse message) =>
        network.Send(from, peer, message);

    /// <inheritdoc/>
    public void Send(string peer, AppendRequest message) =>
        network.Send(from, peer, message);

    /// <inheritdoc/>
    public void Send(string peer, AppendResponse message) =>
        network.Send(from, peer, message);
}
