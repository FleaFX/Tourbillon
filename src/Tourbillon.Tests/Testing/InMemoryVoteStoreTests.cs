using AwesomeAssertions;
using Tourbillon.Raft;

namespace Tourbillon.Testing;

public class InMemoryVoteStoreTests {
    [Fact]
    public async Task An_empty_store_loads_term_zero_and_no_vote() {
        var store = new InMemoryVoteStore();

        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(0UL);
        votedFor.Should().BeNull();
    }

    [Fact]
    public async Task A_saved_term_and_vote_load_back() {
        var store = new InMemoryVoteStore();

        await store.SaveAsync(3, "machine-2", TestContext.Current.CancellationToken);
        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(3UL);
        votedFor.Should().Be("machine-2");
    }

    [Fact]
    public async Task A_new_vote_replaces_the_old_one() {
        var store = new InMemoryVoteStore();

        await store.SaveAsync(3, "machine-2", TestContext.Current.CancellationToken);
        await store.SaveAsync(4, null, TestContext.Current.CancellationToken);
        var (term, votedFor) = await store.LoadAsync(TestContext.Current.CancellationToken);

        term.Should().Be(4UL);
        votedFor.Should().BeNull();
    }
}
