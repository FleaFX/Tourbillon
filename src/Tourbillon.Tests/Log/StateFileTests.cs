using System.Buffers.Binary;
using AwesomeAssertions;

namespace Tourbillon.Log;

public class StateFileTests {
    [Fact]
    public async Task StateFile_round_trip() {
        using var directory = new TempDirectory();
        var content = new byte[] { 10, 20, 30, 40, 50 };

        await StateFile.WriteAsync(directory.Path, "term", content, TestContext.Current.CancellationToken);

        // On disk: the length of the content u32, its crc32c u32, then the content.
        var raw = await File.ReadAllBytesAsync(directory.GetFilePath("term"), TestContext.Current.CancellationToken);
        raw.Should().HaveCount(8 + content.Length);
        BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(0, 4)).Should().Be((uint)content.Length);
        BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(4, 4)).Should().Be(Crc32C.ComputeHash(content));
        raw.AsSpan(8).ToArray().Should().Equal(content);

        var read = await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        read!.Should().Equal(content);
    }

    [Fact]
    public async Task Missing_file_reads_null() {
        using var directory = new TempDirectory();

        var read = await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);

        read.Should().BeNull();
    }

    [Fact]
    public async Task A_leftover_temp_file_is_ignored_and_overwritten() {
        using var directory = new TempDirectory();
        var content = new byte[] { 42 };

        // A crash left a temp file behind: reading the state file ignores it.
        await File.WriteAllBytesAsync(directory.GetFilePath("term.tmp"), new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
        var missing = await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        missing.Should().BeNull();

        // Writing overwrites the leftover instead of failing.
        await StateFile.WriteAsync(directory.Path, "term", content, TestContext.Current.CancellationToken);

        var read = await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        read!.Should().Equal(content);
        File.Exists(directory.GetFilePath("term.tmp")).Should().BeFalse();
    }

    [Fact]
    public async Task A_damaged_state_file_is_refused() {
        using var directory = new TempDirectory();
        var content = new byte[] { 1, 2, 3, 4, 5 };
        await StateFile.WriteAsync(directory.Path, "term", content, TestContext.Current.CancellationToken);
        var path = directory.GetFilePath("term");

        // A flipped byte in the content: the check number no longer matches.
        var raw = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        raw[8 + content.Length / 2] ^= 0xFF;
        await File.WriteAllBytesAsync(path, raw, TestContext.Current.CancellationToken);

        var flipped = async () => await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        await flipped.Should().ThrowExactlyAsync<InvalidDataException>();

        // Cut short: the length no longer matches the content.
        using (var file = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None))
            file.SetLength(7);

        var cut = async () => await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        await cut.Should().ThrowExactlyAsync<InvalidDataException>();
    }

    [Fact]
    public async Task Overwrite_keeps_one_file() {
        using var directory = new TempDirectory();

        await StateFile.WriteAsync(directory.Path, "term", new byte[] { 1 }, TestContext.Current.CancellationToken);
        await StateFile.WriteAsync(directory.Path, "term", new byte[] { 2, 3 }, TestContext.Current.CancellationToken);

        // No temp file is left behind.
        var files = Directory.GetFiles(directory.Path);
        files.Should().HaveCount(1);
        files[0].Should().Be(directory.GetFilePath("term"));

        var read = await StateFile.ReadAsync(directory.Path, "term", TestContext.Current.CancellationToken);
        read!.Should().Equal(new byte[] { 2, 3 });
    }
}
