using AwesomeAssertions;

namespace Tourbillon.Log;

public class DirectorySyncTests {
    [Fact]
    public void Flush_an_existing_directory() {
        using var directory = new TempDirectory();

        var flush = () => DirectorySync.Flush(directory.Path);

        flush.Should().NotThrow();
    }

    [Fact]
    public void Flush_a_missing_directory_throws() {
        if (OperatingSystem.IsWindows())
            Assert.Skip("On Windows a directory flush does nothing.");

        using var directory = new TempDirectory();
        var missing = Path.Combine(directory.Path, "missing");

        var flush = () => DirectorySync.Flush(missing);

        flush.Should().Throw<IOException>();
    }
}
