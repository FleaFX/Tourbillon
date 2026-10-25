using System.Buffers.Binary;

namespace Tourbillon.Log;

/// <summary>
/// Encodes an entry into one on-disk record and decodes it again, without allocating.
/// </summary>
static class RecordFormat {
    /// <summary>
    /// The number of bytes before the payload: length (4), check number (4), version
    /// (1), kind (1), flags (1), term (8) and index (8).
    /// </summary>
    public const int HeaderLength = 27;

    const int LengthOffset = 0;
    const int LengthFieldSize = 4;
    const int CrcOffset = 4;
    const int CrcFieldSize = 4;
    const int VersionOffset = 8;
    const int KindOffset = 9;
    const int FlagsOffset = 10;
    const int TermOffset = 11;
    const int IndexOffset = 19;
    const int PayloadOffset = 27;
    const byte CurrentVersion = 1;

    // The bit in the flags byte that marks the first record of a write. It belongs to
    // the record, not to the entry: an entry knows nothing of how it was written.
    const byte FirstOfWriteBit = 0x80;

    // The entry flags this version knows; any other bit is damage.
    const EntryFlags KnownEntryFlags = EntryFlags.MayUseReserve;

    // The smallest valid value of the length field: the check number and the fixed
    // fields, without a payload.
    const uint MinLengthField = 23;

    /// <summary>
    /// The number of bytes a record with a payload of <paramref name="payloadLength"/>
    /// bytes takes.
    /// </summary>
    public static int GetLength(int payloadLength) => HeaderLength + payloadLength;

    /// <summary>
    /// Writes <paramref name="entry"/> as one record at the start of
    /// <paramref name="destination"/>, marked as the first record of a write when
    /// <paramref name="firstOfWrite"/> is set.
    /// </summary>
    /// <returns>The number of bytes written.</returns>
    public static int Write(in Entry entry, Span<byte> destination, bool firstOfWrite = false) {
        if (entry.Index == 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Index, "The index of an entry starts at 1.");

        if (entry.Term == 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Term, "A term starts at 1.");

        if (!Enum.IsDefined(entry.Kind))
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Kind, "The kind is not defined.");

        if ((entry.Flags & ~KnownEntryFlags) != 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Flags, "The flags contain an unknown bit.");

        if (entry.Payload.Length > Array.MaxLength - HeaderLength)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Payload.Length, "The payload is too long.");

        var recordLength = GetLength(entry.Payload.Length);

        if (destination.Length < recordLength)
            throw new ArgumentException($"The destination must hold at least {recordLength} bytes.", nameof(destination));

        var flags = (byte)entry.Flags;
        if (firstOfWrite)
            flags |= FirstOfWriteBit;

        destination[VersionOffset] = CurrentVersion;
        destination[KindOffset] = (byte)entry.Kind;
        destination[FlagsOffset] = flags;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(TermOffset, 8), entry.Term);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(IndexOffset, 8), entry.Index);
        entry.Payload.Span.CopyTo(destination.Slice(PayloadOffset, entry.Payload.Length));

        var crc = Crc32C.ComputeHash(destination.Slice(VersionOffset, recordLength - VersionOffset));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(CrcOffset, CrcFieldSize), crc);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(LengthOffset, LengthFieldSize), (uint)(recordLength - LengthFieldSize));

        return recordLength;
    }

    /// <summary>
    /// Reads the record at the start of <paramref name="source"/>.
    /// </summary>
    public static RecordStatus TryRead(ReadOnlySpan<byte> source, out RecordHeader header, out int length) {
        header = default;
        length = default;

        if (source.Length < CrcOffset + CrcFieldSize)
            return RecordStatus.Incomplete;

        var lengthField = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(LengthOffset, LengthFieldSize));

        // A zeroed region reads as length 0, so as damage, not as incomplete.
        if (lengthField < MinLengthField || lengthField > Array.MaxLength - LengthFieldSize)
            return RecordStatus.Damaged;

        var recordLength = (int)lengthField + LengthFieldSize;

        if (source.Length < recordLength)
            return RecordStatus.Incomplete;

        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(CrcOffset, CrcFieldSize));
        var crc = Crc32C.ComputeHash(source.Slice(VersionOffset, recordLength - VersionOffset));

        if (crc != storedCrc)
            return RecordStatus.Damaged;

        // With a correct check number, an unknown version, kind or flag bit, an index 0
        // or a term 0 means a writer of another version, or a bug; never something to
        // read on.
        var version = source[VersionOffset];
        var kind = (EntryKind)source[KindOffset];
        var isFirstOfWrite = (source[FlagsOffset] & FirstOfWriteBit) != 0;
        var flags = (EntryFlags)(source[FlagsOffset] & ~FirstOfWriteBit);
        var term = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(TermOffset, 8));
        var index = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(IndexOffset, 8));

        if (version != CurrentVersion
            || !Enum.IsDefined(kind)
            || (flags & ~KnownEntryFlags) != 0
            || index == 0
            || term == 0)
            return RecordStatus.Damaged;

        header = new RecordHeader(index, term, kind, flags, isFirstOfWrite, (int)lengthField - (int)MinLengthField);
        length = recordLength;

        return RecordStatus.Complete;
    }

    /// <summary>
    /// Reads the number of bytes the record at the start of <paramref name="source"/>
    /// claims to take, from its length field alone, without checking anything else.
    /// </summary>
    /// <returns>Whether <paramref name="source"/> holds the whole length field.</returns>
    public static bool TryReadDeclaredLength(ReadOnlySpan<byte> source, out long recordLength) {
        if (source.Length < LengthFieldSize) {
            recordLength = default;
            return false;
        }

        recordLength = (long)BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(LengthOffset, LengthFieldSize)) + LengthFieldSize;
        return true;
    }

    /// <summary>
    /// Whether a record marked as the first of a write may start at the start of
    /// <paramref name="source"/>, judged by its fixed bytes alone: a cheap check for a
    /// search byte by byte, before <see cref="TryRead"/> computes the check number.
    /// </summary>
    /// <param name="source">At least <see cref="HeaderLength"/> bytes.</param>
    /// <param name="bytesLeft">The number of bytes from the start of <paramref name="source"/> to the end of the file.</param>
    public static bool MayBeFirstOfWrite(ReadOnlySpan<byte> source, long bytesLeft) {
        if (source.Length < HeaderLength)
            throw new ArgumentException($"The source must hold at least {HeaderLength} bytes.", nameof(source));

        var lengthField = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(LengthOffset, LengthFieldSize));
        var flags = source[FlagsOffset];

        return source[VersionOffset] == CurrentVersion
            && Enum.IsDefined((EntryKind)source[KindOffset])
            && (flags & FirstOfWriteBit) != 0
            && ((EntryFlags)(flags & ~FirstOfWriteBit) & ~KnownEntryFlags) == 0
            && lengthField >= MinLengthField
            && (long)lengthField + LengthFieldSize <= bytesLeft;
    }
}
