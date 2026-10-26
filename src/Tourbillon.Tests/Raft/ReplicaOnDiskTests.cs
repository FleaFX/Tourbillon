using AwesomeAssertions;

using Tourbillon.Log;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class ReplicaOnDiskTests {
    [Fact]
    public async Task A_one_copy_group_elects_itself_accepts_proposals_and_reopens_with_the_same_term_vote_and_entries() {
        await using var group = await DiskGroup.StartAsync(1);
        var replica = group.Replica("machine-1");
        var feed = replica.Feed!;

        // The NoOp of the first term commits on its own: the copy elects itself.
        (await feed.WaitToReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
        var noOp = await feed.ReadAsync(TestContext.Current.CancellationToken);
        noOp.Length.Should().Be(1);
        noOp.Span[0].Kind.Should().Be(EntryKind.NoOp);
        replica.Role.Should().Be(ReplicaRole.Leader);
        replica.IsReady.Should().BeTrue();

        var accepted = await replica.ProposeAsync(new byte[] { 42 }, false, TestContext.Current.CancellationToken);
        accepted.Should().Be(ProposalResult.Accepted(2));
        var command = await feed.ReadAsync(TestContext.Current.CancellationToken);
        command.Length.Should().Be(1);
        command.Span[0].Index.Should().Be(2UL);
        command.Span[0].Payload.ToArray().Should().Equal(new byte[] { 42 });

        var term = replica.Term;
        var votedFor = replica.VotedFor;
        term.Should().Be(1UL);
        votedFor.Should().Be("machine-1");

        // Stop and reopen on the same directory, without a loop: the state is on disk.
        await group.ReopenAsync("machine-1", applied: 2, startLoop: false);
        var reopened = group.Replica("machine-1");
        reopened.Term.Should().Be(term);
        reopened.VotedFor.Should().Be(votedFor);
        reopened.Feed!.Delivered.Should().Be(2UL);
        (await reopened.Feed!.ReadAsync(TestContext.Current.CancellationToken)).Length.Should().Be(0);

        var log = group.Log("machine-1");
        log.LastIndex.Should().Be(2UL);
        var firstBuffer = new byte[log.GetRecordLength(1)];
        var first = await log.ReadAsync(1, firstBuffer, TestContext.Current.CancellationToken);
        first.Kind.Should().Be(EntryKind.NoOp);
        var buffer = new byte[log.GetRecordLength(2)];
        var entry = await log.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
        entry.Index.Should().Be(2UL);
        entry.Kind.Should().Be(EntryKind.Command);
        entry.Payload.ToArray().Should().Equal(new byte[] { 42 });
    }

    [Fact]
    public async Task Three_copies_on_their_own_directories_accept_proposals_and_have_them_on_disk() {
        await using var group = await DiskGroup.StartAsync(3);

        // Wait for a leader: every copy's feed wakes when its NoOp commits.
        await Task.WhenAll(group.Machines.Select(machine => group.Feed(machine).WaitToReadAsync(TestContext.Current.CancellationToken).AsTask()));
        var leader = group.Machines.Select(machine => group.Replica(machine)).Single(replica => replica.IsReady);

        var payload = new byte[] { 7, 7, 7 };
        var accepted = await leader.ProposeAsync(payload, false, TestContext.Current.CancellationToken);
        accepted.Should().Be(ProposalResult.Accepted(2));

        foreach (var machine in group.Machines)
            await DiskGroup.ReadUpToAsync(group.Replica(machine), 2, new List<Entry>());

        // Every copy has the entry on disk.
        foreach (var machine in group.Machines) {
            var log = group.Log(machine);
            log.LastIndex.Should().Be(2UL);
            var buffer = new byte[log.GetRecordLength(2)];
            var entry = await log.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
            entry.Index.Should().Be(2UL);
            entry.Kind.Should().Be(EntryKind.Command);
            entry.Payload.ToArray().Should().Equal(payload);
        }
    }

    [Fact]
    public async Task A_users_D_above_the_last_index_refuses_the_open() {
        using var directory = new TempDirectory();
        using var log = await SegmentedLog.OpenAsync(DiskFileSystem.Instance, directory.Path, DiskGroup.SegmentSize, DiskGroup.UnlimitedStorage(), TestContext.Current.CancellationToken);
        var appended = await log.AppendAsync(new[] { new Entry(1, 1, EntryKind.Command, EntryFlags.None, new byte[] { 1 }) });
        appended.Should().Be(1);
        var votes = new FileVoteStore(DiskFileSystem.Instance, directory.Path);
        var replica = new Replica(
            "machine-1",
            new[] { "machine-1" },
            log,
            votes,
            new InProcessTransport("machine-1", new Dictionary<string, Replica>()),
            new ReplicaOptions(),
            TimeProvider.System,
            new Random(1));

        var open = async () => await replica.LoadAsync(2, TestContext.Current.CancellationToken);
        await open.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>()
            .WithParameterName("applied")
            .WithMessage("*past the last index*");

        await replica.DisposeAsync();
    }
}
