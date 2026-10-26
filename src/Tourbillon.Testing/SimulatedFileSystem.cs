using Tourbillon.Log;

namespace Tourbillon.Testing;

/// <summary>
/// A file system in memory, one per simulated machine: every call completes
/// synchronously. A crash loses what was not flushed, the way the book describes:
/// per file, the 512-byte sectors of its unflushed bytes are kept or lost by the
/// seeded random (the entries of one write may reach the disk in any order, and a
/// lost sector before a kept one reads as zeros), an unflushed truncate is applied
/// or not, and every change of the names in a directory not yet flushed is applied
/// or not. After a crash the file system is usable again, as a disk after a reboot.
/// </summary>
sealed class SimulatedFileSystem : IFileSystem {
    // The size of the sectors a crash keeps or loses whole.
    const int SectorSize = 512;

    readonly Random _random;
    readonly Dictionary<string, FileEntry> _files = new(StringComparer.Ordinal);
    readonly Dictionary<string, DirectoryEntry> _directories = new(StringComparer.Ordinal);
    int? _crashAfter;
    bool _crashed;

    /// <summary>
    /// An empty file system with its root and /logs, on the random
    /// <paramref name="random"/> a crash during an operation draws its coins from.
    /// </summary>
    public SimulatedFileSystem(Random random) {
        _random = random;
        _directories["/"] = new DirectoryEntry(durable: true, current: true);
        _directories["/logs"] = new DirectoryEntry(durable: true, current: true);
    }

    /// <summary>
    /// The free space the storage limit of a log on this file system measures.
    /// </summary>
    public long FreeSpace { get; set; } = long.MaxValue;

    /// <summary>
    /// Whether the machine is down: a crash during an operation happened, and every
    /// call throws until <see cref="Restart"/>.
    /// </summary>
    public bool IsCrashed => _crashed;

    /// <summary>
    /// Whether a crash during an operation is armed and has not happened yet.
    /// </summary>
    public bool IsArmed => _crashAfter is not null;

    /// <inheritdoc/>
    public bool DirectoryExists(string path) {
        ThrowIfCrashed();
        return _directories.TryGetValue(Normalize(path), out var entry) && entry.Current;
    }

    /// <inheritdoc/>
    public void CreateDirectory(string path) {
        BeforeChange();
        var current = "/";
        foreach (var name in Normalize(path).Split('/')) {
            if (name.Length == 0)
                continue;

            current = current == "/" ? "/" + name : current + "/" + name;
            if (_files.ContainsKey(current))
                throw new IOException($"The path '{path}' holds a file where a directory was expected.");

            if (!_directories.ContainsKey(current))
                _directories[current] = new DirectoryEntry(durable: false, current: true);
        }
    }

    /// <inheritdoc/>
    public IReadOnlyList<string> GetFiles(string directory) {
        ThrowIfCrashed();
        var normalized = Normalize(directory);
        if (!_directories.TryGetValue(normalized, out var entry) || !entry.Current)
            throw new IOException($"The directory '{directory}' does not exist.");

        var files = new List<string>();
        foreach (var child in DirectChildren(normalized)) {
            if (_files.TryGetValue(child, out var file) && file.Current is not null)
                files.Add(child);
        }

        return files;
    }

    /// <inheritdoc/>
    public bool FileExists(string path) {
        ThrowIfCrashed();
        return _files.TryGetValue(Normalize(path), out var entry) && entry.Current is not null;
    }

    /// <inheritdoc/>
    public IFile OpenFile(string path) {
        BeforeChange();
        var normalized = Normalize(path);
        if (_directories.ContainsKey(normalized))
            throw new IOException($"The path '{path}' is a directory, not a file.");

        if (!_files.TryGetValue(normalized, out var entry)) {
            entry = new FileEntry();
            _files[normalized] = entry;
        }

        // A file deleted but not yet flushed comes back empty: the delete is not
        // durable, but the file is gone until then.
        entry.Current ??= new FileContent();
        return new SimulatedFile(this, normalized, entry.Current);
    }

    /// <inheritdoc/>
    public void DeleteFile(string path) {
        BeforeChange();
        var normalized = Normalize(path);
        if (!_files.TryGetValue(normalized, out var entry) || entry.Current is null)
            throw new IOException($"The file '{path}' does not exist.");

        // The delete changes the current set of names; FlushDirectory makes it
        // durable.
        entry.Current = null;
        entry.Change = null;
    }

    /// <inheritdoc/>
    public void MoveFile(string source, string destination) {
        BeforeChange();
        var from = Normalize(source);
        var to = Normalize(destination);
        if (from == to)
            throw new IOException($"The file '{source}' cannot be moved onto itself.");

        if (!_files.TryGetValue(from, out var sourceEntry) || sourceEntry.Current is null)
            throw new IOException($"The file '{source}' does not exist.");

        if (_directories.ContainsKey(to))
            throw new IOException($"The path '{destination}' is a directory, not a file.");

        if (!_files.TryGetValue(to, out var destinationEntry)) {
            destinationEntry = new FileEntry();
            _files[to] = destinationEntry;
        }

        // A rename is one change over two names: the crash applies or reverts it
        // whole, never half.
        var change = new NameChange();
        var content = sourceEntry.Current;
        sourceEntry.Current = null;
        sourceEntry.Change = change;
        destinationEntry.Current = content;
        destinationEntry.Change = change;
    }

    /// <inheritdoc/>
    public void FlushDirectory(string path) {
        BeforeChange();
        var normalized = Normalize(path);
        if (!_directories.TryGetValue(normalized, out var entry) || !entry.Current)
            throw new IOException($"The directory '{path}' does not exist.");

        // The creates, renames and deletes of the direct children become durable.
        foreach (var child in DirectChildren(normalized)) {
            if (_files.TryGetValue(child, out var file)) {
                file.Durable = file.Current;
                file.Change = null;
                if (file.Durable is null)
                    _files.Remove(child);
            } else if (_directories.TryGetValue(child, out var directory)) {
                directory.Durable = directory.Current;
            }
        }
    }

    /// <summary>
    /// Crashes the machine: every file goes back to its durable state plus, of its
    /// unflushed bytes, the sectors the <paramref name="random"/> keeps; an
    /// unflushed truncate is applied or not; every change of the names in a
    /// directory not yet flushed is applied or not. After a crash the file system
    /// is usable again, as a disk after a reboot.
    /// </summary>
    public void Crash(Random random) {
        // The names first, in path order: every change not yet flushed is applied
        // or reverted, one coin each.
        foreach (var path in AllPaths()) {
            if (_files.TryGetValue(path, out var file)) {
                if (ReferenceEquals(file.Durable, file.Current))
                    continue;

                var apply = file.Change is { } change
                    ? change.Applied ??= random.Next(2) == 0
                    : random.Next(2) == 0;
                if (apply)
                    file.Durable = file.Current;
                else
                    file.Current = file.Durable;

                file.Change = null;
            } else if (_directories.TryGetValue(path, out var directory)) {
                if (directory.Durable == directory.Current)
                    continue;

                if (random.Next(2) == 0)
                    directory.Durable = directory.Current;
                else
                    directory.Current = directory.Durable;
            }
        }

        // What the coins decided: a delete that became durable and a create that
        // was reverted leave no file, and a directory whose creation was reverted
        // takes its whole subtree with it.
        foreach (var path in AllPaths()) {
            if (_files.TryGetValue(path, out var file)) {
                if (file.Durable is null && file.Current is null)
                    _files.Remove(path);
            } else if (_directories.TryGetValue(path, out var directory) && !directory.Current) {
                RemoveSubtree(path);
            }
        }

        // Then the bytes of every file that is left, in path order: the sectors of
        // its unflushed bytes are kept or lost, one coin each.
        foreach (var path in AllPaths()) {
            if (_files.TryGetValue(path, out var file) && file.Durable is { } content)
                CrashContent(content, random);
        }

        // The disk is back, as after a reboot.
        _crashed = false;
        _crashAfter = null;
    }

    /// <summary>
    /// Arms a crash during a file operation: the <paramref name="operations"/>-th
    /// following change of the file system crashes it first and throws
    /// <see cref="SimulatedCrashException"/>; every later call throws it too, until
    /// <see cref="Restart"/>. Reads do not count, so an armed crash fires in a turn
    /// that changes files, not in a read.
    /// </summary>
    public void CrashAfter(int operations) {
        ArgumentOutOfRangeException.ThrowIfLessThan(operations, 1);
        _crashAfter = operations;
    }

    /// <summary>
    /// Restarts the machine: the file system answers again. The crash itself
    /// already took its model; nothing is applied a second time.
    /// </summary>
    public void Restart() {
        _crashed = false;
        _crashAfter = null;
    }

    // Every change of the file system goes through here: a crashed machine throws,
    // and an armed crash during an operation fires on the countdown's last
    // operation, before the operation itself.
    void BeforeChange() {
        if (_crashed)
            throw new SimulatedCrashException();

        if (_crashAfter is { } remaining) {
            if (remaining <= 1) {
                _crashAfter = null;
                Crash(_random);
                _crashed = true;
                throw new SimulatedCrashException();
            }

            _crashAfter = remaining - 1;
        }
    }

    void ThrowIfCrashed() {
        if (_crashed)
            throw new SimulatedCrashException();
    }

    // The bytes of one file after a crash: the durable bytes, plus of the unflushed
    // ones the sectors the random keeps; an unflushed truncate is applied or not,
    // never half.
    static void CrashContent(FileContent content, Random random) {
        var durable = content.Durable;
        var current = content.Current;
        var durableLength = durable.Length;

        var result = (byte[])durable.Clone();
        var length = durableLength;

        var sectors = (current.Length + SectorSize - 1) / SectorSize;
        for (var sector = 0; sector < sectors; sector++) {
            var start = sector * SectorSize;
            var end = Math.Min(start + SectorSize, current.Length);

            // A sector is dirty when it holds unflushed bytes: beyond the durable
            // length, or different from the durable bytes.
            var dirty = end > durableLength;
            if (!dirty) {
                for (var i = start; i < end; i++) {
                    if (current[i] != durable[i]) {
                        dirty = true;
                        break;
                    }
                }
            }

            if (!dirty)
                continue;

            if (random.Next(2) == 0) {
                // Kept: the sector's current bytes reach the disk whole.
                if (end > result.Length) {
                    var bigger = new byte[end];
                    Array.Copy(result, bigger, result.Length);
                    result = bigger;
                }

                Array.Copy(current, start, result, start, end - start);
                length = Math.Max(length, end);
            }

            // Lost: the durable bytes stay, zeros beyond the durable length.
        }

        if (content.LengthDirty)
            length = random.Next(2) == 0 ? current.Length : durableLength;

        if (length != result.Length) {
            var resized = new byte[length];
            Array.Copy(result, resized, Math.Min(result.Length, length));
            result = resized;
        }

        content.Durable = result;
        content.Current = (byte[])result.Clone();
        content.LengthDirty = false;
    }

    // The paths of every file and directory, in path order.
    List<string> AllPaths() {
        var all = new List<string>(_files.Count + _directories.Count);
        all.AddRange(_files.Keys);
        all.AddRange(_directories.Keys);
        all.Sort(StringComparer.Ordinal);
        return all;
    }

    // The direct children of a directory: the paths one level below it, files and
    // directories, in path order.
    List<string> DirectChildren(string directory) {
        var prefix = directory == "/" ? "/" : directory + "/";
        var children = new List<string>();
        Collect(_files.Keys, prefix, children);
        Collect(_directories.Keys, prefix, children);
        children.Sort(StringComparer.Ordinal);
        return children;
    }

    static void Collect(IEnumerable<string> paths, string prefix, List<string> children) {
        foreach (var path in paths) {
            if (path.Length > prefix.Length
                && path.StartsWith(prefix, StringComparison.Ordinal)
                && path.IndexOf('/', prefix.Length) < 0)
                children.Add(path);
        }
    }

    // Removes a directory and its whole subtree.
    void RemoveSubtree(string path) {
        var prefix = path + "/";
        _directories.Remove(path);
        foreach (var key in _directories.Keys.ToArray()) {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                _directories.Remove(key);
        }

        foreach (var key in _files.Keys.ToArray()) {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                _files.Remove(key);
        }
    }

    // The paths are virtual: the separators are normalized to '/', and a drive
    // prefix is dropped, so the same tree is addressed the same way on every host.
    static string Normalize(string path) {
        var normalized = path.Replace('\\', '/');
        if (normalized.Length > 2 && normalized[1] == ':' && char.IsAsciiLetter(normalized[0]))
            normalized = normalized[2..];

        return normalized;
    }

    // One file: the version that is durable (null when the file does not exist
    // durably) and the current one (null when it does not exist). The two are the
    // same object while no create, delete or rename waits for a directory flush.
    sealed class FileEntry {
        public FileContent? Durable;
        public FileContent? Current;
        public NameChange? Change;
    }

    // One directory: whether it exists durably and whether it exists now.
    sealed class DirectoryEntry {
        public DirectoryEntry(bool durable, bool current) {
            Durable = durable;
            Current = current;
        }

        public bool Durable { get; set; }
        public bool Current { get; set; }
    }

    // The bytes of one version of a file: the durable bytes, as of its last flush
    // to disk, and the current ones. The array lengths are the file lengths.
    sealed class FileContent {
        public byte[] Durable = [];
        public byte[] Current = [];
        public bool LengthDirty;
    }

    // One change of the names in a directory that no flush has made durable yet.
    // A rename is one change over two entries, so the crash applies or reverts it
    // whole.
    sealed class NameChange {
        public bool? Applied;
    }

    // One open file of this file system, written and read at explicit positions;
    // every call completes synchronously. Reads never count for a crash during an
    // operation, and closing a file is not a change.
    sealed class SimulatedFile : IFile {
        readonly SimulatedFileSystem _fs;
        readonly string _path;
        readonly FileContent _content;

        public SimulatedFile(SimulatedFileSystem fs, string path, FileContent content) {
            _fs = fs;
            _path = path;
            _content = content;
        }

        /// <inheritdoc/>
        public string Path => _path;

        /// <inheritdoc/>
        public long Length => _content.Current.Length;

        /// <inheritdoc/>
        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long position) {
            _fs.BeforeChange();
            ArgumentOutOfRangeException.ThrowIfNegative(position);

            var end = position + data.Length;
            if (end > _content.Current.Length) {
                var bigger = new byte[end];
                Array.Copy(_content.Current, bigger, _content.Current.Length);
                _content.Current = bigger;
            }

            data.Span.CopyTo(_content.Current.AsSpan((int)position));
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public void FlushToDisk() {
            _fs.BeforeChange();
            _content.Durable = (byte[])_content.Current.Clone();
            _content.LengthDirty = false;
        }

        /// <inheritdoc/>
        public void SetLength(long length) {
            _fs.BeforeChange();
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            if (length == _content.Current.Length)
                return;

            var current = new byte[length];
            Array.Copy(_content.Current, current, Math.Min(_content.Current.Length, length));
            _content.Current = current;
            _content.LengthDirty = true;
        }

        /// <inheritdoc/>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, long position, CancellationToken cancellationToken = default) {
            ArgumentOutOfRangeException.ThrowIfNegative(position);
            var current = _content.Current;
            if (position >= current.Length)
                return new ValueTask<int>(0);

            var count = (int)Math.Min(buffer.Length, current.Length - position);
            current.AsSpan((int)position, count).CopyTo(buffer.Span);
            return new ValueTask<int>(count);
        }

        /// <inheritdoc/>
        public void Dispose() {
        }
    }
}
