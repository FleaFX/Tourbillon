using System.Buffers.Binary;

namespace Tourbillon.Log;

/// <summary>
/// Encodes an entry into one on-disk record and decodes it again, without allocating.
/// </summary>
internal static class RecordFormat {
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
    /// <paramref name="destination"/>.
    /// </summary>
    /// <returns>The number of bytes written.</returns>
    public static int Write(in Entry entry, Span<byte> destination) {
        if (entry.Index == 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Index, "The index of an entry starts at 1.");

        if (entry.Term == 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Term, "A term starts at 1.");

        if (!Enum.IsDefined(entry.Kind))
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Kind, "The kind is not defined.");

        if ((entry.Flags & ~EntryFlags.MayUseReserve) != 0)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Flags, "The flags contain an unknown bit.");

        if (entry.Payload.Length > Array.MaxLength - HeaderLength)
            throw new ArgumentOutOfRangeException(nameof(entry), entry.Payload.Length, "The payload is too long.");

        var recordLength = GetLength(entry.Payload.Length);

        if (destination.Length < recordLength)
            throw new ArgumentException($"The destination must hold at least {recordLength} bytes.", nameof(destination));

        destination[VersionOffset] = CurrentVersion;
        destination[KindOffset] = (byte)entry.Kind;
        destination[FlagsOffset] = (byte)entry.Flags;
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
        var flags = (EntryFlags)source[FlagsOffset];
        var term = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(TermOffset, 8));
        var index = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(IndexOffset, 8));

        if (version != CurrentVersion
            || !Enum.IsDefined(kind)
            || (flags & ~EntryFlags.MayUseReserve) != 0
            || index == 0
            || term == 0)
            return RecordStatus.Damaged;

        header = new RecordHeader(index, term, kind, flags, (int)lengthField - (int)MinLengthField);
        length = recordLength;

        return RecordStatus.Complete;
    }
}
