namespace Tourbillon.Log;

/// <summary>
/// The file system of the disk, the one <see cref="IFileSystem"/> the production
/// code uses. Stateless: every call goes to the disk, no state is kept.
/// </summary>
sealed class DiskFileSystem : IFileSystem {
    /// <summary>
    /// The one instance.
    /// </summary>
    public static readonly DiskFileSystem Instance = new();

    /// <inheritdoc/>
    public bool DirectoryExists(string path) => Directory.Exists(path);

    /// <inheritdoc/>
    public void CreateDirectory(string path) => Directory.CreateDirectory(path);

    /// <inheritdoc/>
    public IReadOnlyList<string> GetFiles(string directory) => Directory.GetFiles(directory);

    /// <inheritdoc/>
    public bool FileExists(string path) => File.Exists(path);

    /// <inheritdoc/>
    public IFile OpenFile(string path) => DiskFile.Open(path);

    /// <inheritdoc/>
    public void DeleteFile(string path) => File.Delete(path);

    /// <inheritdoc/>
    public void MoveFile(string source, string destination) => File.Move(source, destination, overwrite: true);

    /// <inheritdoc/>
    public void FlushDirectory(string path) => DirectorySync.Flush(path);
}
