using System.Buffers.Binary;
using AwesomeAssertions;

namespace Tourbillon.Log;

public class RecordFormatTests {
    static readonly EntryKind[] AllKinds = [EntryKind.Command, EntryKind.NoOp, EntryKind.Config];
    static readonly EntryFlags[] AllFlags = [EntryFlags.None, EntryFlags.MayUseReserve];

    static Entry MakeEntry(EntryKind kind, EntryFlags flags, int payloadLength, ulong index, ulong term) {
        var payload = new byte[payloadLength];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i * 31 + 7);

        return new Entry(index, term, kind, flags, payload);
    }

    static byte[] WriteValidRecord() {
        var entry = MakeEntry(EntryKind.Command, EntryFlags.None, 10, 1, 1);
        var record = new byte[RecordFormat.GetLength(10)];
        RecordFormat.Write(in entry, record);
        return record;
    }

    // The check number covers the bytes after its own field: from offset 8, the length
    // field minus its own 4 bytes. After changing a field by hand it is computed again,
    // so that only the field check can catch the change.
    static void RecomputeCrc(byte[] record) {
        var lengthField = BinaryPrimitives.ReadUInt32LittleEndian(record);
        var crc = Crc32C.ComputeHash(record.AsSpan(8, (int)lengthField - 4));
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4, 4), crc);
    }

    public class Write {
        public static IEnumerable<object[]> InvalidEntries {
            get {
                yield return new object[] { 0ul, 1ul, (int)EntryKind.Command, (int)EntryFlags.None }; // index 0
                yield return new object[] { 1ul, 0ul, (int)EntryKind.Command, (int)EntryFlags.None }; // term 0
                yield return new object[] { 1ul, 1ul, 0, (int)EntryFlags.None }; // kind not defined
                yield return new object[] { 1ul, 1ul, 4, (int)EntryFlags.None }; // kind not defined
                yield return new object[] { 1ul, 1ul, (int)EntryKind.Command, 2 }; // unknown flag bit
                yield return new object[] { 1ul, 1ul, (int)EntryKind.Command, 0x80 }; // unknown flag bit
            }
        }

        [Theory]
        [MemberData(nameof(InvalidEntries))]
        public void Refuses_invalid_entries(ulong index, ulong term, int kind, int flags) {
            var entry = new Entry(index, term, (EntryKind)kind, (EntryFlags)flags, ReadOnlyMemory<byte>.Empty);
            var destination = new byte[RecordFormat.HeaderLength];

            var write = () => RecordFormat.Write(entry, destination);

            write.Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Refuses_too_short_destination() {
            var entry = MakeEntry(EntryKind.Command, EntryFlags.None, 10, 1, 1);
            var destination = new byte[RecordFormat.GetLength(10) - 1];

            var write = () => RecordFormat.Write(entry, destination);

            write.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Does_not_allocate() {
            var entry = MakeEntry(EntryKind.Command, EntryFlags.None, 100, 1, 1);
            var destination = new byte[RecordFormat.GetLength(100)];

            // Warm-up: the JIT and tiered compilation settle before measuring.
            for (var i = 0; i < 100; i++) {
                RecordFormat.Write(entry, destination);
                RecordFormat.TryRead(destination, out _, out _);
            }

            var before = GC.GetAllocatedBytesForCurrentThread();

            for (var i = 0; i < 1000; i++) {
                RecordFormat.Write(entry, destination);
                RecordFormat.TryRead(destination, out _, out _);
            }

            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            allocated.Should().Be(0);
        }
    }

    public class TryRead {
        public static IEnumerable<object[]> RoundTripEntries {
            get {
                foreach (var kind in AllKinds)
                foreach (var flags in AllFlags)
                foreach (var payloadLength in new[] { 0, 1, 100, 1024 * 1024 })
                foreach (var place in new[] { 1ul, ulong.MaxValue })
                    yield return new object[] { (int)kind, (int)flags, payloadLength, place };
            }
        }

        [Theory]
        [MemberData(nameof(RoundTripEntries))]
        public void Round_trip(int kind, int flags, int payloadLength, ulong place) {
            var entry = MakeEntry((EntryKind)kind, (EntryFlags)flags, payloadLength, place, place);
            var record = new byte[RecordFormat.GetLength(payloadLength)];

            var written = RecordFormat.Write(entry, record);

            written.Should().Be(record.Length);
            RecordFormat.TryRead(record, out var header, out var length).Should().Be(RecordStatus.Complete);
            length.Should().Be(RecordFormat.GetLength(payloadLength));
            header.Index.Should().Be(place);
            header.Term.Should().Be(place);
            header.Kind.Should().Be((EntryKind)kind);
            header.Flags.Should().Be((EntryFlags)flags);
            header.PayloadLength.Should().Be(payloadLength);
            record.AsSpan(RecordFormat.HeaderLength, payloadLength).SequenceEqual(entry.Payload.Span).Should().BeTrue();
        }

        [Fact]
        public void Reads_two_records_after_each_other() {
            var first = MakeEntry(EntryKind.Command, EntryFlags.None, 10, 1, 1);
            var second = MakeEntry(EntryKind.NoOp, EntryFlags.MayUseReserve, 20, 2, 1);
            var record = new byte[RecordFormat.GetLength(10) + RecordFormat.GetLength(20)];

            RecordFormat.Write(first, record);
            var secondLength = RecordFormat.Write(second, record.AsSpan(RecordFormat.GetLength(10)));

            RecordFormat.TryRead(record, out var firstHeader, out var firstLength).Should().Be(RecordStatus.Complete);
            firstLength.Should().Be(RecordFormat.GetLength(10));
            firstHeader.Index.Should().Be(1);

            RecordFormat.TryRead(record.AsSpan(firstLength), out var secondHeader, out var length).Should().Be(RecordStatus.Complete);
            length.Should().Be(secondLength);
            secondHeader.Index.Should().Be(2);
            secondHeader.Term.Should().Be(1);
            secondHeader.Kind.Should().Be(EntryKind.NoOp);
            secondHeader.Flags.Should().Be(EntryFlags.MayUseReserve);
            secondHeader.PayloadLength.Should().Be(20);
        }

        public static IEnumerable<object[]> FlippedRecords {
            get {
                foreach (var kind in AllKinds)
                foreach (var payloadLength in new[] { 0, 1, 100, 4096 })
                    yield return new object[] { (int)kind, payloadLength };
            }
        }

        [Theory]
        [MemberData(nameof(FlippedRecords))]
        public void Every_single_byte_flip_is_damage(int kind, int payloadLength) {
            var entry = MakeEntry((EntryKind)kind, EntryFlags.None, payloadLength, 1, 1);
            var recordLength = RecordFormat.GetLength(payloadLength);
            var original = new byte[recordLength];
            RecordFormat.Write(in entry, original);

            foreach (var offset in Enumerable.Range(0, recordLength)) {
                foreach (var mask in new byte[] { 0x01, 0x80, 0xFF }) {
                    var flipped = (byte[])original.Clone();
                    flipped[offset] ^= mask;

                    var status = RecordFormat.TryRead(flipped, out _, out _);

                    status.Should().NotBe(RecordStatus.Complete);

                    // A flip in the check number or the fields breaks the check number; a
                    // flip in the length field either breaks the length or points past
                    // the end of the input.
                    if (offset >= 4)
                        status.Should().Be(RecordStatus.Damaged);
                    else
                        status.Should().BeOneOf(RecordStatus.Damaged, RecordStatus.Incomplete);
                }
            }
        }

        [Fact]
        public void Short_input_is_incomplete() {
            var entry = MakeEntry(EntryKind.Command, EntryFlags.None, 100, 1, 1);
            var record = new byte[RecordFormat.GetLength(100)];
            RecordFormat.Write(in entry, record);

            for (var length = 0; length < record.Length; length++)
                RecordFormat.TryRead(record.AsSpan(0, length), out _, out _)
                    .Should().Be(RecordStatus.Incomplete);
        }

        [Theory]
        [InlineData(8)]
        [InlineData(27)]
        [InlineData(64)]
        [InlineData(1000)]
        public void Zeroed_input_is_damage(int length) {
            var zeros = new byte[length];

            RecordFormat.TryRead(zeros, out _, out _).Should().Be(RecordStatus.Damaged);
        }

        // The offsets of the fields in the record: version 8, kind 9, flags 10, term 11,
        // index 19.
        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        [InlineData(255)]
        public void Unknown_version_is_damage(byte version) {
            var record = WriteValidRecord();
            record[8] = version;
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out _, out _).Should().Be(RecordStatus.Damaged);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(4)]
        public void Unknown_kind_is_damage(byte kind) {
            var record = WriteValidRecord();
            record[9] = kind;
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out _, out _).Should().Be(RecordStatus.Damaged);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(0x40)]
        public void Unknown_flag_is_damage(byte flags) {
            var record = WriteValidRecord();
            record[10] = flags;
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out _, out _).Should().Be(RecordStatus.Damaged);
        }

        [Fact]
        public void Index_zero_is_damage() {
            var record = WriteValidRecord();
            record.AsSpan(19, 8).Clear();
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out _, out _).Should().Be(RecordStatus.Damaged);
        }

        [Fact]
        public void Term_zero_is_damage() {
            var record = WriteValidRecord();
            record.AsSpan(11, 8).Clear();
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out _, out _).Should().Be(RecordStatus.Damaged);
        }
    }

    public class FirstOfWrite {
        [Fact]
        public void Round_trip_with_the_bit_set() {
            var entry = MakeEntry(EntryKind.Command, EntryFlags.MayUseReserve, 100, 5, 3);
            var record = new byte[RecordFormat.GetLength(100)];

            var written = RecordFormat.Write(in entry, record, firstOfWrite: true);

            written.Should().Be(record.Length);
            record[10].Should().Be((byte)0x81);
            RecordFormat.TryRead(record, out var header, out var length).Should().Be(RecordStatus.Complete);
            length.Should().Be(record.Length);
            header.IsFirstOfWrite.Should().BeTrue();
            header.Flags.Should().Be(EntryFlags.MayUseReserve);
            header.Index.Should().Be(5);
            header.Term.Should().Be(3);
            header.Kind.Should().Be(EntryKind.Command);
            header.PayloadLength.Should().Be(100);
            record.AsSpan(RecordFormat.HeaderLength, 100).SequenceEqual(entry.Payload.Span).Should().BeTrue();
        }

        [Fact]
        public void Round_trip_without_the_bit() {
            var entry = MakeEntry(EntryKind.NoOp, EntryFlags.None, 0, 1, 1);
            var record = new byte[RecordFormat.GetLength(0)];

            var written = RecordFormat.Write(in entry, record, firstOfWrite: false);

            written.Should().Be(record.Length);
            record[10].Should().Be((byte)0x00);
            RecordFormat.TryRead(record, out var header, out _).Should().Be(RecordStatus.Complete);
            header.IsFirstOfWrite.Should().BeFalse();
            header.Flags.Should().Be(EntryFlags.None);
        }

        [Fact]
        public void Without_the_argument_the_record_is_not_marked() {
            var entry = MakeEntry(EntryKind.Config, EntryFlags.MayUseReserve, 10, 2, 2);
            var record = new byte[RecordFormat.GetLength(10)];

            RecordFormat.Write(in entry, record);

            record[10].Should().Be((byte)0x01);
            RecordFormat.TryRead(record, out var header, out _).Should().Be(RecordStatus.Complete);
            header.IsFirstOfWrite.Should().BeFalse();
            header.Flags.Should().Be(EntryFlags.MayUseReserve);
        }

        [Fact]
        public void TryRead_accepts_the_bit_and_masks_it_off() {
            var record = WriteValidRecord();
            record[10] |= 0x80;
            RecomputeCrc(record);

            RecordFormat.TryRead(record, out var header, out _).Should().Be(RecordStatus.Complete);

            header.IsFirstOfWrite.Should().BeTrue();
            header.Flags.Should().Be(EntryFlags.None);
        }
    }

    public class TryReadDeclaredLength {
        [Fact]
        public void Reads_the_length_of_the_whole_record() {
            var record = WriteValidRecord();

            RecordFormat.TryReadDeclaredLength(record.AsSpan(0, 4), out var length).Should().BeTrue();

            length.Should().Be(record.Length);
        }

        [Fact]
        public void Needs_the_whole_length_field() =>
            RecordFormat.TryReadDeclaredLength(new byte[3], out _).Should().BeFalse();
    }

    public class MayBeFirstOfWrite {
        static byte[] WriteFirstOfWrite(EntryFlags flags = EntryFlags.None) {
            var entry = MakeEntry(EntryKind.Command, flags, 10, 1, 1);
            var record = new byte[RecordFormat.GetLength(10)];
            RecordFormat.Write(in entry, record, firstOfWrite: true);
            return record;
        }

        [Theory]
        [InlineData((int)EntryFlags.None)]
        [InlineData((int)EntryFlags.MayUseReserve)]
        public void Passes_a_record_marked_first_of_a_write(int flags) {
            var record = WriteFirstOfWrite((EntryFlags)flags);

            RecordFormat.MayBeFirstOfWrite(record, record.Length).Should().BeTrue();
        }

        [Fact]
        public void Rejects_a_record_without_the_mark() =>
            RecordFormat.MayBeFirstOfWrite(WriteValidRecord(), RecordFormat.GetLength(10)).Should().BeFalse();

        [Fact]
        public void Rejects_a_record_longer_than_the_bytes_left() {
            var record = WriteFirstOfWrite();

            RecordFormat.MayBeFirstOfWrite(record, record.Length - 1).Should().BeFalse();
        }

        [Theory]
        [InlineData(8, 2)] // version
        [InlineData(9, 0)] // kind
        [InlineData(10, 0xC0)] // unknown flag bit next to the mark
        [InlineData(0, 0)] // length below the fixed fields
        public void Rejects_a_bad_fixed_byte(int offset, byte value) {
            var record = WriteFirstOfWrite();
            record[offset] = value;

            RecordFormat.MayBeFirstOfWrite(record, record.Length).Should().BeFalse();
        }

        [Fact]
        public void Refuses_less_than_a_header() {
            var check = () => RecordFormat.MayBeFirstOfWrite(new byte[RecordFormat.HeaderLength - 1], 100);

            check.Should().Throw<ArgumentException>();
        }
    }
}
