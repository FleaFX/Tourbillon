using Microsoft.Extensions.Time.Testing;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

/// <summary>
/// The network of a <see cref="SimulatedCluster"/>: a queue of deliveries ordered by
/// arrival time and sequence number, with a seeded drop chance and delay per message,
/// and cuts — directed or into islands — checked when a message is sent and again when
/// it arrives. Every send is recorded in <see cref="Sent"/>.
/// </summary>
sealed class SimulatedNetwork {
    /// <summary>
    /// One message in flight: when it arrives, in which send order, and what it is.
    /// </summary>
    readonly record struct Delivery(DateTimeOffset Arrival, long Sequence, string From, string To, object Message);

    readonly FakeTimeProvider _clock;
    readonly Random _random;
    readonly SimulationOptions _options;
    readonly string[] _machines;
    readonly List<Delivery> _queue = new();
    readonly HashSet<(string From, string To)> _cuts = new();
    readonly Dictionary<(string From, string To), DateTimeOffset> _lastArrival = new();
    long _sequence;

    string[][] _islands;

    /// <summary>
    /// A network over <paramref name="machines"/> on <paramref name="clock"/>, driven by
    /// <paramref name="random"/> with <paramref name="options"/>.
    /// </summary>
    public SimulatedNetwork(FakeTimeProvider clock, Random random, SimulationOptions options, IReadOnlyList<string> machines) {
        _clock = clock;
        _random = random;
        _options = options;
        _machines = machines.ToArray();
        _islands = [_machines];
    }

    /// <summary>
    /// Every message a copy handed to the network, in send order: who sent what to
    /// whom. The entries of an append request are copied at the send.
    /// </summary>
    public List<(string From, string To, object Message)> Sent { get; } = new();

    /// <summary>
    /// Sends a vote request from one copy to another.
    /// </summary>
    public void Send(string from, string to, VoteRequest message) =>
        Enqueue(from, to, message);

    /// <summary>
    /// Sends a vote response from one copy to another.
    /// </summary>
    public void Send(string from, string to, VoteResponse message) =>
        Enqueue(from, to, message);

    /// <summary>
    /// Sends an append request from one copy to another; the entries are copied, the
    /// memory of the message is only valid during the call.
    /// </summary>
    public void Send(string from, string to, AppendRequest message) =>
        Enqueue(from, to, Copy(message));

    /// <summary>
    /// Sends an append response from one copy to another.
    /// </summary>
    public void Send(string from, string to, AppendResponse message) =>
        Enqueue(from, to, message);

    /// <summary>
    /// Cuts the directed link from one copy to another: messages that way are dropped,
    /// in flight or not yet sent.
    /// </summary>
    public void Cut(string from, string to) =>
        _cuts.Add((from, to));

    /// <summary>
    /// Opens the directed link from one copy to another again.
    /// </summary>
    public void Heal(string from, string to) =>
        _cuts.Remove((from, to));

    /// <summary>
    /// Splits the copies into islands; messages between islands are dropped, in flight
    /// or not yet sent.
    /// </summary>
    public void Partition(params string[][] islands) =>
        _islands = islands;

    /// <summary>
    /// Removes every cut and partition: every copy can reach every other copy again.
    /// </summary>
    public void Heal() {
        _islands = [_machines];
        _cuts.Clear();
    }

    /// <summary>
    /// Drops every message in flight to <paramref name="machine"/>: a crash takes what
    /// was on its way to the copy with it.
    /// </summary>
    public void DropInFlightTo(string machine) =>
        _queue.RemoveAll(delivery => delivery.To == machine);

    /// <summary>
    /// Delivers every message whose arrival time has come, in (arrival, sequence) order,
    /// to <paramref name="deliver"/>; a message whose link was cut while in flight is
    /// dropped.
    /// </summary>
    /// <returns>How many messages were delivered.</returns>
    public int DeliverDue(DateTimeOffset now, Action<string, string, object> deliver) {
        var delivered = 0;
        while (_queue.Count > 0 && _queue[0].Arrival <= now) {
            var delivery = _queue[0];
            _queue.RemoveAt(0);
            if (!CanReach(delivery.From, delivery.To))
                continue;

            deliver(delivery.From, delivery.To, delivery.Message);
            delivered++;
        }

        return delivered;
    }

    void Enqueue(string from, string to, object message) {
        Sent.Add((from, to, message));
        if (!CanReach(from, to))
            return;

        if (_random.NextDouble() < _options.DropChance)
            return;

        var arrival = _clock.GetUtcNow() + SampleDelay();
        if (!_options.AllowReorder)
            arrival = ClampToArrivalOrder(from, to, arrival);

        Insert(new Delivery(arrival, _sequence++, from, to, message));
    }

    TimeSpan SampleDelay() {
        var minTicks = _options.MinDelay.Ticks;
        var maxTicks = _options.MaxDelay.Ticks;
        return minTicks >= maxTicks
            ? _options.MinDelay
            : TimeSpan.FromTicks(_random.NextInt64(minTicks, maxTicks));
    }

    // Reordering off: delivery between one pair of copies stays in send order, whatever
    // the sampled delays say.
    DateTimeOffset ClampToArrivalOrder(string from, string to, DateTimeOffset arrival) {
        var key = (from, to);
        if (_lastArrival.TryGetValue(key, out var previous) && previous >= arrival)
            arrival = previous + TimeSpan.FromTicks(1);

        _lastArrival[key] = arrival;
        return arrival;
    }

    void Insert(Delivery delivery) {
        var index = _queue.Count;
        while (index > 0 && ComesBefore(delivery, _queue[index - 1]))
            index--;

        _queue.Insert(index, delivery);
    }

    /// <summary>
    /// Whether one delivery comes before another in the queue: by arrival time, then
    /// by send order.
    /// </summary>
    static bool ComesBefore(Delivery first, Delivery second) =>
        first.Arrival < second.Arrival
        || (first.Arrival == second.Arrival && first.Sequence < second.Sequence);

    bool CanReach(string from, string to) =>
        from == to
        || (_islands.Any(island => island.Contains(from) && island.Contains(to)) && !_cuts.Contains((from, to)));

    static AppendRequest Copy(AppendRequest message) {
        var entries = new Entry[message.Entries.Length];
        for (var i = 0; i < entries.Length; i++) {
            var entry = message.Entries.Span[i];
            entries[i] = new Entry(entry.Index, entry.Term, entry.Kind, entry.Flags, entry.Payload.ToArray());
        }

        return new AppendRequest(message.Term, message.PrevIndex, message.PrevTerm, message.Commit, entries);
    }
}
