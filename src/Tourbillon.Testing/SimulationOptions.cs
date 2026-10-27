namespace Tourbillon.Testing;

/// <summary>
/// Per-run knobs for a <see cref="SimulatedCluster"/>, all driven by the cluster's
/// shared seed.
/// </summary>
/// <param name="DropChance">The chance per message that the network loses it.</param>
/// <param name="MinDelay">The shortest delay of a message in flight.</param>
/// <param name="MaxDelay">The longest delay of a message in flight.</param>
/// <param name="AllowReorder">Whether messages between the same two replicas may arrive out of order.</param>
/// <param name="Disk">Whether every machine gets a simulated file system, a segmented log and a vote store on it, instead of the in-memory ones.</param>
sealed record SimulationOptions(
    double DropChance = 0,
    TimeSpan MinDelay = default,
    TimeSpan MaxDelay = default,
    bool AllowReorder = false,
    bool Disk = false);
