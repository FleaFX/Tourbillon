using System.Buffers.Binary;
using System.Text;
using Tourbillon.Log;

namespace Tourbillon.Raft;

/// <summary>
/// The term and vote of one copy on disk, in the state file vote.state next to the
/// segments of its log: the term u64, the length of the name u16 (0: no vote) and
/// the name in UTF-8. A missing file loads as term 0 and no vote; a damaged one
/// refuses.
/// </summary>
sealed class FileVoteStore : IVoteStore {
    // The name of the state file, next to the segments of the log.
    const string FileName = "vote.state";

    readonly IFileSystem _fs;
    readonly string _directory;

    /// <summary>
    /// The vote store of the copy whose log lives in <paramref name="directory"/>,
    /// through <paramref name="fileSystem"/>.
    /// </summary>
    public FileVoteStore(IFileSystem fileSystem, string directory) {
        _fs = fileSystem;
        _directory = directory;
    }

    /// <inheritdoc/>
    public async ValueTask<(ulong Term, string? VotedFor)> LoadAsync(CancellationToken cancellationToken = default) {
        var content = await StateFile.ReadAsync(_fs, _directory, FileName, cancellationToken).ConfigureAwait(false);
        if (content is null)
            return (0, null);

        // On disk: the term u64, the length of the name u16, the name in UTF-8.
        if (content.Length < 10)
            throw new InvalidDataException($"The vote store in '{_directory}' is damaged: it holds less than a term and a name length.");

        var term = BinaryPrimitives.ReadUInt64LittleEndian(content.AsSpan(0, 8));
        var nameLength = BinaryPrimitives.ReadUInt16LittleEndian(content.AsSpan(8, 2));
        if (nameLength != content.Length - 10)
            throw new InvalidDataException($"The vote store in '{_directory}' is damaged: its name length does not match its content.");

        var votedFor = nameLength == 0 ? null : Encoding.UTF8.GetString(content.AsSpan(10));
        return (term, votedFor);
    }

    /// <inheritdoc/>
    public async ValueTask SaveAsync(ulong term, string? votedFor, CancellationToken cancellationToken = default) {
        var nameLength = votedFor is null ? 0 : Encoding.UTF8.GetByteCount(votedFor);
        if (nameLength > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(votedFor), votedFor, "The name is too long to store.");

        var content = new byte[10 + nameLength];
        BinaryPrimitives.WriteUInt64LittleEndian(content.AsSpan(0, 8), term);
        BinaryPrimitives.WriteUInt16LittleEndian(content.AsSpan(8, 2), (ushort)nameLength);
        if (votedFor is not null)
            Encoding.UTF8.GetBytes(votedFor, content.AsSpan(10));

        await StateFile.WriteAsync(_fs, _directory, FileName, content, cancellationToken).ConfigureAwait(false);
    }
}
