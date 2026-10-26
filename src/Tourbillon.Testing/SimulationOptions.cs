namespace Tourbillon.Testing;

/// <summary>
/// Per-run knobs for a <see cref="SimulatedCluster"/>, all driven by the cluster's
/// shared seed.
/// </summary>
/// <param name="DropChance">The chance per message that the network loses it.</param>
/// <param name="MinDelay">The shortest delay of a message in flight.</param>
/// <param name="MaxDelay">The longest delay of a message in flight.</param>
/// <param name="AllowReorder">Whether messages between the same two copies may arrive out of order.</param>
sealed record SimulationOptions(
    double DropChance = 0,
    TimeSpan MinDelay = default,
    TimeSpan MaxDelay = default,
    bool AllowReorder = false);
