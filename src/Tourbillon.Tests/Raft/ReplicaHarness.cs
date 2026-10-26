using Microsoft.Extensions.Time.Testing;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

// One replica with its in-memory log, vote store, recording transport and fake clock,
// driven by turns: advance the clock to fire a timer, run a turn to process what the
// firing posted.
sealed class ReplicaHarness : IAsyncDisposable {
    ReplicaHarness(
        Replica replica,
        InMemoryLog log,
        IVoteStore votes,
        RecordingTransport transport,
        FakeTimeProvider time,
        string name,
        IReadOnlyList<string> members
    ) {
        Replica = replica;
        Log = log;
        Votes = votes;
        Transport = transport;
        Time = time;
        Name = name;
        Members = members;
    }

    public Replica Replica { get; }

    public InMemoryLog Log { get; }

    public IVoteStore Votes { get; }

    public RecordingTransport Transport { get; }

    public FakeTimeProvider Time { get; }

    public string Name { get; }

    public IReadOnlyList<string> Members { get; }

    public static async Task<ReplicaHarness> MakeAsync(
        string name = "machine-1",
        IReadOnlyList<string>? members = null,
        ulong term = 0,
        string? votedFor = null,
        Entry[]? entries = null,
        IVoteStore? votes = null,
        RecordingTransport? transport = null,
        int seed = 1,
        InMemoryLog? log = null
    ) {
        members ??= new[] { "machine-1", "machine-2", "machine-3" };
        var inMemoryLog = log ?? new InMemoryLog();
        if (entries is not null)
            await inMemoryLog.AppendAsync(entries);
        var voteStore = votes ?? new InMemoryVoteStore();
        if (term != 0 || votedFor is not null)
            await voteStore.SaveAsync(term, votedFor, TestContext.Current.CancellationToken);
        var recording = transport ?? new RecordingTransport();
        var time = new FakeTimeProvider();
        var replica = new Replica(name, members, inMemoryLog, voteStore, recording, new ReplicaOptions(), time, new Random(seed));
        return new ReplicaHarness(replica, inMemoryLog, voteStore, recording, time, name, members);
    }

    public async Task LoadAsync() =>
        await Replica.LoadAsync(TestContext.Current.CancellationToken);

    public async Task<bool> TurnAsync() =>
        await Replica.RunTurnAsync();

    public void Advance(TimeSpan by) =>
        Time.Advance(by);

    // The election timeout is drawn between 150 ms and 300 ms; advancing 300 ms fires any
    // pending election timer.
    public void AdvancePastElectionTimeout() =>
        Time.Advance(TimeSpan.FromMilliseconds(300));

    public void GrantPreVote(string from) =>
        Replica.Receive(from, new VoteResponse(0, true, PreVote: true));

    public void GrantVote(string from) =>
        Replica.Receive(from, new VoteResponse(Replica.Term, true, PreVote: false));

    public int Quorum => Members.Count / 2 + 1;

    // Reports a stored append response from a member, at the given index.
    public void StoreAt(string from, ulong index) =>
        Replica.Receive(from, new AppendResponse(Replica.Term, AppendResult.Stored, index, 0));

    // Drives this copy to leader: fire the election timer, win the trial, win the real
    // election.
    public async Task ElectAsync() {
        AdvancePastElectionTimeout();
        await TurnAsync();
        foreach (var member in Members) {
            if (member != Name)
                GrantPreVote(member);
        }

        await TurnAsync();
        foreach (var member in Members) {
            if (member != Name)
                GrantVote(member);
        }

        await TurnAsync();
    }

    // Drives this copy to a ready leader: the election above, then the NoOp of the new
    // term stored by a quorum of the followers, so the ready gate opens.
    public async Task ElectReadyAsync() {
        await ElectAsync();
        var stored = 0;
        foreach (var member in Members) {
            if (member == Name || stored >= Quorum - 1)
                continue;

            StoreAt(member, Log.LastIndex);
            stored++;
        }

        await TurnAsync();
    }

    public async ValueTask DisposeAsync() =>
        await Replica.DisposeAsync();
}
