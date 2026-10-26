using AwesomeAssertions;

using Tourbillon.Log;
using Tourbillon.Testing;

namespace Tourbillon.Raft;

public class FileVoteStoreTests {
    [Fact]
    public async Task A_saved_term_and_vote_load_back() {
        using var directory = new TempDirectory();
        var store = new FileVoteStore(DiskFileSystem.Instance, directory.Path);

        await store.SaveAsync(5, "machine-2", TestContext.Current.CancellationToken);
        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(5UL);
        votedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task A_saved_term_without_a_vote_loads_back_without_one() {
        using var directory = new TempDirectory();
        var store = new FileVoteStore(DiskFileSystem.Instance, directory.Path);

        await store.SaveAsync(7, null, TestContext.Current.CancellationToken);
        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(7UL);
        votedFor.Should().BeNull();
    }

    [Fact]
    public async Task A_non_ascii_name_loads_back() {
        using var directory = new TempDirectory();
        var store = new FileVoteStore(DiskFileSystem.Instance, directory.Path);

        await store.SaveAsync(3, "màchine-Ω", TestContext.Current.CancellationToken);
        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(3UL);
        votedFor.Should().Be("màchine-Ω");
    }

    [Fact]
    public async Task A_missing_file_loads_as_term_zero_and_no_vote() {
        using var directory = new TempDirectory();
        var store = new FileVoteStore(DiskFileSystem.Instance, directory.Path);

        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(0UL);
        votedFor.Should().BeNull();
    }

    [Fact]
    public async Task A_damaged_file_refuses() {
        using var directory = new TempDirectory();
        var store = new FileVoteStore(DiskFileSystem.Instance, directory.Path);
        await store.SaveAsync(5, "machine-2", TestContext.Current.CancellationToken);

        // A flipped byte in the content: the check number no longer matches.
        var path = directory.GetFilePath("vote.state");
        var raw = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        raw[10] ^= 0xFF;
        await File.WriteAllBytesAsync(path, raw, TestContext.Current.CancellationToken);

        var load = async () => await store.LoadAsync(TestContext.Current.CancellationToken);
        await load.Should().ThrowExactlyAsync<InvalidDataException>();
    }

    [Fact]
    public async Task A_crash_at_every_operation_of_a_save_leaves_the_old_or_the_new() {
        var sawOld = 0;
        var sawNew = 0;
        var sawNone = 0;
        for (var seed = 1; seed <= 100; seed++) {
            for (var crashAfter = 1; crashAfter <= 6; crashAfter++) {
                // A save over an old version.
                {
                    var fs = new SimulatedFileSystem(new Random(seed));
                    var store = new FileVoteStore(fs, "/logs");

                    await store.SaveAsync(1, "machine-1", TestContext.Current.CancellationToken);
                    fs.CrashAfter(crashAfter);
                    var save = async () => await store.SaveAsync(2, "machine-2", TestContext.Current.CancellationToken);
                    await save.Should().ThrowAsync<IOException>();
                    fs.Restart();

                    var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);
                    ((term, votedFor) == (1UL, "machine-1") || (term, votedFor) == (2UL, "machine-2")).Should().BeTrue();
                    if ((term, votedFor) == (1UL, "machine-1"))
                        sawOld++;
                    else
                        sawNew++;
                }

                // A first save: no old version.
                {
                    var fs = new SimulatedFileSystem(new Random(seed));
                    var store = new FileVoteStore(fs, "/logs");

                    fs.CrashAfter(crashAfter);
                    var save = async () => await store.SaveAsync(2, "machine-2", TestContext.Current.CancellationToken);
                    await save.Should().ThrowAsync<IOException>();
                    fs.Restart();

                    var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);
                    ((term, votedFor) == (0UL, null) || (term, votedFor) == (2UL, "machine-2")).Should().BeTrue();
                    if (term == 0)
                        sawNone++;
                    else
                        sawNew++;
                }
            }
        }

        sawOld.Should().BeGreaterThan(0);
        sawNew.Should().BeGreaterThan(0);
        sawNone.Should().BeGreaterThan(0);
    }
}
