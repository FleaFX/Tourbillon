using AwesomeAssertions;

namespace Tourbillon.Log;

public class DiskFileTests {
    [Fact]
    public async Task Writes_at_a_position_and_reads_back() {
        using var directory = new TempDirectory();
        var path = directory.GetFilePath("file");

        using (var file = DiskFile.Open(path)) {
            await file.WriteAsync(new byte[] { 1, 2, 3 }, 0);
            await file.WriteAsync(new byte[] { 4, 5 }, 10);

            file.Length.Should().Be(12);

            var buffer = new byte[3];
            var read = await file.ReadAsync(buffer, 0, TestContext.Current.CancellationToken);
            read.Should().Be(3);
            buffer.Should().Equal(new byte[] { 1, 2, 3 });

            var tail = new byte[2];
            read = await file.ReadAsync(tail, 10, TestContext.Current.CancellationToken);
            read.Should().Be(2);
            tail.Should().Equal(new byte[] { 4, 5 });

            // Past the end: nothing.
            read = await file.ReadAsync(tail, 12, TestContext.Current.CancellationToken);
            read.Should().Be(0);
        }

        // On disk: the bytes between the writes are zeros.
        (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken))
            .Should().Equal(new byte[] { 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 4, 5 });
    }

    [Fact]
    public async Task SetLength_cuts_and_extends_with_zeros() {
        using var directory = new TempDirectory();
        var path = directory.GetFilePath("file");

        using (var file = DiskFile.Open(path)) {
            await file.WriteAsync(new byte[] { 1, 2, 3, 4, 5 }, 0);
            file.Length.Should().Be(5);

            file.SetLength(2);
            file.Length.Should().Be(2);

            file.SetLength(4);
            file.Length.Should().Be(4);
        }

        (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken))
            .Should().Equal(new byte[] { 1, 2, 0, 0 });
    }

    [Fact]
    public async Task Flush_to_disk_is_called_once_per_call() {
        using var directory = new TempDirectory();
        var path = directory.GetFilePath("file");
        using var file = new CountingFile(path);

        file.FlushToDisk();
        file.FlushToDisk();

        file.FlushToDiskCount.Should().Be(2);

        await file.WriteAsync(new byte[] { 1 }, 0);
        file.WriteCount.Should().Be(1);
    }
}
