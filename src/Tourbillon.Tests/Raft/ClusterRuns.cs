using Tourbillon.Testing;

namespace Tourbillon.Raft;

// Steps a cluster forward until a condition holds or the step budget runs out — the
// simulation-driven equivalent of the polling the layer-1 tests do in real time.
static class ClusterRuns {
    // A reliable network: no drops, a short delay, reordering on.
    public static SimulationOptions Reliable =>
        new(MinDelay: TimeSpan.FromMilliseconds(1), MaxDelay: TimeSpan.FromMilliseconds(5), AllowReorder: true);

    public static async Task<bool> UntilAsync(SimulatedCluster cluster, Func<bool> condition, int maxSteps = 500, TimeSpan? step = null) {
        var increment = step ?? TimeSpan.FromMilliseconds(20);
        for (var i = 0; i < maxSteps; i++) {
            if (condition())
                return true;

            await cluster.StepAsync(increment);
        }

        return condition();
    }

    // The name of the current leader, or null while the group has none; a crashed copy
    // has no say.
    public static string? Leader(SimulatedCluster cluster) =>
        cluster.Machines.Where(cluster.IsRunning).FirstOrDefault(machine => cluster.Replica(machine).Role == ReplicaRole.Leader);

    // Entries 1..count of one term, one payload byte each.
    public static Entry[] Entries(int count, ulong term) =>
        Enumerable.Range(1, count)
            .Select(index => new Entry((ulong)index, term, EntryKind.Command, EntryFlags.None, new byte[] { (byte)(index % 251) }))
            .ToArray();
}
