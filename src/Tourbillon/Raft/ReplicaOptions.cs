namespace Tourbillon.Raft;

/// <summary>
/// The settings of one replica.
/// </summary>
sealed record ReplicaOptions {
    /// <summary>
    /// How often a leader sends an append request to every follower; 50 ms.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// The shortest election timeout; 150 ms.
    /// </summary>
    public TimeSpan ElectionTimeoutMin { get; init; } = TimeSpan.FromMilliseconds(150);

    /// <summary>
    /// The longest election timeout; 300 ms.
    /// </summary>
    public TimeSpan ElectionTimeoutMax { get; init; } = TimeSpan.FromMilliseconds(300);
}
