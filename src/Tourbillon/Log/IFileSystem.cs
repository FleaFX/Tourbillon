namespace Tourbillon.Log;

/// <summary>
/// The files of the logs and their state files: the disk, or in tests a simulated
/// file system that loses on a crash what was not flushed.
/// </summary>
interface IFileSystem {
    /// <summary>
    /// Whether the directory at <paramref name="path"/> exists.
    /// </summary>
    bool DirectoryExists(string path);

    /// <summary>
    /// Creates the directory at <paramref name="path"/>.
    /// </summary>
    void CreateDirectory(string path);

    /// <summary>
    /// The files directly in <paramref name="directory"/>, as full paths, in no
    /// particular order.
    /// </summary>
    IReadOnlyList<string> GetFiles(string directory);

    /// <summary>
    /// Whether the file at <paramref name="path"/> exists.
    /// </summary>
    bool FileExists(string path);

    /// <summary>
    /// Opens or creates the file at <paramref name="path"/> for reading and
    /// writing; others may read it.
    /// </summary>
    IFile OpenFile(string path);

    /// <summary>
    /// Deletes the file at <paramref name="path"/>.
    /// </summary>
    void DeleteFile(string path);

    /// <summary>
    /// Moves the file at <paramref name="source"/> to
    /// <paramref name="destination"/>, replacing the destination.
    /// </summary>
    void MoveFile(string source, string destination);

    /// <summary>
    /// Makes the creates, renames and deletes in <paramref name="path"/> durable.
    /// </summary>
    void FlushDirectory(string path);
}
