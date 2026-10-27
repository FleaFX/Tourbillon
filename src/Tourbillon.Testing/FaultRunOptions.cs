namespace Tourbillon.Testing;

/// <summary>
/// The faults of one seeded fault run: how the network of its cluster behaves.
/// </summary>
sealed record FaultRunOptions {
    /// <summary>
    /// The chance a message is dropped.
    /// </summary>
    public double DropChance { get; init; } = 0.1;

    /// <summary>
    /// The shortest delay of a message.
    /// </summary>
    public TimeSpan MinDelay { get; init; } = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// The longest delay of a message.
    /// </summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMilliseconds(40);

    /// <summary>
    /// Whether messages between one pair of replicas may arrive out of order.
    /// </summary>
    public bool AllowReorder { get; init; } = true;

    /// <summary>
    /// Whether the replicas run on a segmented log and vote store on a simulated file
    /// system: a crash can then lose what was not flushed, during an operation or at
    /// once.
    /// </summary>
    public bool Disk { get; init; }
}
