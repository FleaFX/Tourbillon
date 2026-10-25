namespace Tourbillon.Log;

sealed class TempDirectory : IDisposable {
    public TempDirectory() {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"tourbillon-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public string GetFilePath(string fileName) => System.IO.Path.Combine(Path, fileName);

    public void Dispose() {
        try {
            Directory.Delete(Path, recursive: true);
        } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) {
            // A stream still holding a file of this directory must not fail the test.
        }
    }
}
