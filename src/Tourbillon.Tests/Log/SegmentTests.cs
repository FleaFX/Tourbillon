using AwesomeAssertions;

namespace Tourbillon.Log;

public class SegmentTests {
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    static byte[] MakePayload(ulong index, int length, int seed = 0) {
        var payload = new byte[length];
        for (var i = 0; i < length; i++)
            payload[i] = (byte)(index * 31 + (ulong)seed * 17 + (ulong)i * 7 + 3);

        return payload;
    }

    static Entry MakeEntry(ulong index, ulong term = 1, int payloadLength = 16, int seed = 0) =>
        new(index, term, EntryKind.Command, EntryFlags.None, MakePayload(index, payloadLength, seed));

    static Entry[] MakeEntries(ulong firstIndex, int count, ulong term = 1, int payloadLength = 16, int seed = 0) {
        var entries = new Entry[count];
        for (var i = 0; i < count; i++)
            entries[i] = MakeEntry(firstIndex + (ulong)i, term, payloadLength, seed);

        return entries;
    }

    static List<(ulong Index, byte[] Payload)> DecodeRecords(byte[] fileBytes) {
        var records = new List<(ulong Index, byte[] Payload)>();
        var offset = 0;
        while (offset < fileBytes.Length) {
            RecordFormat.TryRead(fileBytes.AsSpan(offset), out var header, out var length).Should().Be(RecordStatus.Complete);
            records.Add((header.Index, fileBytes.AsSpan(offset + RecordFormat.HeaderLength, header.PayloadLength).ToArray()));
            offset += length;
        }

        return records;
    }

    static long[] RecordPositions(byte[] fileBytes) {
        var positions = new List<long>();
        var offset = 0;
        while (offset < fileBytes.Length) {
            positions.Add(offset);
            RecordFormat.TryRead(fileBytes.AsSpan(offset), out _, out var length).Should().Be(RecordStatus.Complete);
            offset += length;
        }

        return positions.ToArray();
    }

    static byte[] EncodeRecords(params (Entry Entry, bool FirstOfWrite)[] records) {
        var total = 0;
        foreach (var (entry, _) in records)
            total += RecordFormat.GetLength(entry.Payload.Length);

        var bytes = new byte[total];
        var offset = 0;
        foreach (var (entry, firstOfWrite) in records)
            offset += RecordFormat.Write(in entry, bytes.AsSpan(offset), firstOfWrite);

        return bytes;
    }

    static async Task<byte[]> WriteWritesAsync(string path, params Entry[][] writes) {
        using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
            foreach (var write in writes)
                await segment.AppendAsync(write);
        }

        return await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
    }

    public class AppendAsync {
        [Fact]
        public async Task Does_not_complete_before_the_batch_is_fsynced() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var stream = new GatedFlushFile(path);
            using var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken);

            var append = Task.Run(async () => await segment.AppendAsync(MakeEntries(1, 3)), TestContext.Current.CancellationToken);

            await stream.FlushStarted.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
            append.IsCompleted.Should().BeFalse();
            segment.LastIndex.Should().Be(0);

            stream.FlushGate.TrySetResult();
            await append.WaitAsync(Timeout, TestContext.Current.CancellationToken);

            segment.LastIndex.Should().Be(3);
        }

        [Fact]
        public async Task A_batch_costs_one_write_and_one_fsync() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            var stream = new CountingFile(path);
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken))
                await segment.AppendAsync(MakeEntries(1, 1000));

            stream.WriteCount.Should().Be(1);
            stream.FlushToDiskCount.Should().Be(1);

            // Read after the segment is closed: on Windows its open file cannot be
            // opened again for reading.
            var records = DecodeRecords(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            records.Should().HaveCount(1000);
            records[0].Index.Should().Be(1ul);
            records[999].Index.Should().Be(1000ul);
            records[999].Payload.Should().Equal(MakePayload(1000, 16));
        }

        [Fact]
        public async Task An_empty_batch_writes_nothing() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var stream = new CountingFile(path);
            using var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken);

            await segment.AppendAsync(Array.Empty<Entry>());

            stream.WriteCount.Should().Be(0);
            stream.FlushToDiskCount.Should().Be(0);
            segment.LastIndex.Should().Be(0);
            segment.Length.Should().Be(0);
        }

        [Fact]
        public async Task Refuses_a_batch_that_does_not_follow_on() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);

            await segment.AppendAsync(MakeEntries(1, 2));
            await segment.AppendAsync(new[] { MakeEntry(3, term: 2) });
            var lengthBefore = segment.Length;

            var wrongFirst = async () => await segment.AppendAsync(MakeEntries(5, 2));
            await wrongFirst.Should().ThrowExactlyAsync<ArgumentException>();

            var gap = async () => await segment.AppendAsync(new[] { MakeEntry(4), MakeEntry(6) });
            await gap.Should().ThrowExactlyAsync<ArgumentException>();

            var lowerTerm = async () => await segment.AppendAsync(new[] { MakeEntry(4, term: 1) });
            await lowerTerm.Should().ThrowExactlyAsync<ArgumentException>();

            var dropping = async () => await segment.AppendAsync(new[] { MakeEntry(4, term: 5), MakeEntry(5, term: 4) });
            await dropping.Should().ThrowExactlyAsync<ArgumentException>();

            segment.LastIndex.Should().Be(3);
            segment.Length.Should().Be(lengthBefore);
            new FileInfo(path).Length.Should().Be(lengthBefore);

            await segment.AppendAsync(MakeEntries(4, 2, term: 2));
            segment.LastIndex.Should().Be(5);
        }

        [Fact]
        public async Task Round_trip_of_every_kind_and_flag() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);

            var kinds = new[] { EntryKind.Command, EntryKind.NoOp, EntryKind.Config };
            var allFlags = new[] { EntryFlags.None, EntryFlags.MayUseReserve };
            var entries = new List<Entry>();
            var index = 1ul;
            foreach (var kind in kinds) {
                foreach (var flags in allFlags) {
                    entries.Add(new Entry(index, 1, kind, flags, MakePayload(index, 10 + (int)index)));
                    index++;
                }
            }

            await segment.AppendAsync(entries.ToArray());

            foreach (var expected in entries) {
                var buffer = new byte[segment.GetRecordLength(expected.Index)];
                var entry = await segment.ReadAsync(expected.Index, buffer, TestContext.Current.CancellationToken);

                entry.Index.Should().Be(expected.Index);
                entry.Term.Should().Be(expected.Term);
                entry.Kind.Should().Be(expected.Kind);
                entry.Flags.Should().Be(expected.Flags);
                entry.Payload.Span.SequenceEqual(expected.Payload.Span).Should().BeTrue();

                segment.GetTerm(expected.Index).Should().Be(expected.Term);
                segment.GetRecordLength(expected.Index).Should().Be(RecordFormat.GetLength(expected.Payload.Length));
            }
        }
    }

    public class Failures {
        [Fact]
        public async Task A_failed_fsync_leaves_no_bytes_of_the_batch() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var stream = new FaultyFile(path))
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 3));
                var lengthAfterGoodBatch = segment.Length;

                stream.FailNextFlushToDisk = true;
                var failed = async () => await segment.AppendAsync(MakeEntries(4, 3));
                await failed.Should().ThrowAsync<IOException>();

                segment.LastIndex.Should().Be(3);
                new FileInfo(path).Length.Should().Be(lengthAfterGoodBatch);

                await segment.AppendAsync(MakeEntries(4, 2, seed: 1));
                segment.LastIndex.Should().Be(5);
            }

            // A later batch does not make the failed one count: the file holds
            // exactly 1–5, with the payloads of the second attempt.
            var records = DecodeRecords(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            records.Should().HaveCount(5);
            for (var i = 0; i < records.Count; i++) {
                records[i].Index.Should().Be((ulong)(i + 1));
                records[i].Payload.Should().Equal(MakePayload((ulong)(i + 1), 16, seed: i < 3 ? 0 : 1));
            }

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(5);
                for (var index = 1ul; index <= 5; index++) {
                    var buffer = new byte[segment.GetRecordLength(index)];
                    var entry = await segment.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                    entry.Payload.Span.SequenceEqual(MakePayload(index, 16, seed: index < 4 ? 0 : 1)).Should().BeTrue();
                }
            }
        }

        [Fact]
        public async Task A_failed_write_is_cut_back_the_same_way() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var stream = new FaultyFile(path))
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 3));
                var lengthAfterGoodBatch = segment.Length;

                stream.FailNextWrite = true;
                var failed = async () => await segment.AppendAsync(MakeEntries(4, 3));
                await failed.Should().ThrowAsync<IOException>();

                segment.LastIndex.Should().Be(3);
                new FileInfo(path).Length.Should().Be(lengthAfterGoodBatch);

                await segment.AppendAsync(MakeEntries(4, 2, seed: 1));
                segment.LastIndex.Should().Be(5);
            }

            var records = DecodeRecords(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            records.Should().HaveCount(5);
            for (var i = 0; i < records.Count; i++) {
                records[i].Index.Should().Be((ulong)(i + 1));
                records[i].Payload.Should().Equal(MakePayload((ulong)(i + 1), 16, seed: i < 3 ? 0 : 1));
            }

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(5);
                for (var index = 1ul; index <= 5; index++) {
                    var buffer = new byte[segment.GetRecordLength(index)];
                    var entry = await segment.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                    entry.Payload.Span.SequenceEqual(MakePayload(index, 16, seed: index < 4 ? 0 : 1)).Should().BeTrue();
                }
            }
        }

        [Fact]
        public async Task A_failed_cut_back_faults_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var stream = new FaultyFile(path))
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 3));
                var recordLength = segment.GetRecordLength(1);

                stream.FailNextFlushToDisk = true;
                stream.FailNextSetLength = true;

                var failed = async () => await segment.AppendAsync(MakeEntries(4, 3));
                var fault = (await failed.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which;
                fault.Message.Should().Contain("cut back");
                fault.InnerException.Should().BeOfType<AggregateException>()
                    .Which.InnerExceptions.Should().HaveCount(2);

                var appendAgain = async () => await segment.AppendAsync(MakeEntries(4, 1));
                (await appendAgain.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);

                var buffer = new byte[recordLength];
                var readAgain = async () => await segment.ReadAsync(1, buffer, TestContext.Current.CancellationToken);
                (await readAgain.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);
            }
        }

        [Fact]
        public async Task A_failed_fsync_of_the_cut_faults_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var stream = new FaultyFile(path))
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 3));
                var recordLength = segment.GetRecordLength(1);

                stream.FailNextFlushToDisk = true;
                stream.FailFlushAfterSetLength = true;

                var failed = async () => await segment.AppendAsync(MakeEntries(4, 3));
                var fault = (await failed.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which;
                fault.Message.Should().Contain("cut back");
                fault.InnerException.Should().BeOfType<AggregateException>()
                    .Which.InnerExceptions.Should().HaveCount(2);

                var appendAgain = async () => await segment.AppendAsync(MakeEntries(4, 1));
                (await appendAgain.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);

                var buffer = new byte[recordLength];
                var readAgain = async () => await segment.ReadAsync(1, buffer, TestContext.Current.CancellationToken);
                (await readAgain.Should().ThrowAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);
            }
        }
    }
    public class Recovery {
        [Fact]
        public async Task Torn_tail_at_every_byte_offset() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 1, payloadLength: 40));
            var startOfThird = RecordFormat.GetLength(10) * 2;
            var endOfThird = startOfThird + RecordFormat.GetLength(40);

            for (var length = startOfThird; length < endOfThird; length++) {
                await File.WriteAllBytesAsync(path, fileBytes.AsMemory(0, length), TestContext.Current.CancellationToken);

                using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                segment.LastIndex.Should().Be(2);
                segment.Length.Should().Be(startOfThird);
                new FileInfo(path).Length.Should().Be(startOfThird);

                foreach (var expected in MakeEntries(1, 2, payloadLength: 10)) {
                    var buffer = new byte[segment.GetRecordLength(expected.Index)];
                    var entry = await segment.ReadAsync(expected.Index, buffer, TestContext.Current.CancellationToken);
                    entry.Payload.Span.SequenceEqual(expected.Payload.Span).Should().BeTrue();
                }
            }
        }

        [Fact]
        public async Task A_flipped_byte_in_the_last_record_cuts_it() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 1, payloadLength: 40));
            var startOfThird = RecordFormat.GetLength(10) * 2;
            var thirdLength = RecordFormat.GetLength(40);

            for (var offset = 0; offset < thirdLength; offset++) {
                var flipped = (byte[])fileBytes.Clone();
                flipped[startOfThird + offset] ^= 0xFF;
                await File.WriteAllBytesAsync(path, flipped, TestContext.Current.CancellationToken);

                using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                segment.LastIndex.Should().Be(2);
                segment.Length.Should().Be(startOfThird);
                new FileInfo(path).Length.Should().Be(startOfThird);
            }
        }

        [Fact]
        public async Task A_zeroed_tail_is_cut() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10));
            var withZeros = new byte[fileBytes.Length + 4096];
            Array.Copy(fileBytes, withZeros, fileBytes.Length);
            await File.WriteAllBytesAsync(path, withZeros, TestContext.Current.CancellationToken);

            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            segment.LastIndex.Should().Be(2);
            segment.Length.Should().Be(fileBytes.Length);
            new FileInfo(path).Length.Should().Be(fileBytes.Length);
        }

        [Fact]
        public async Task A_bad_record_in_the_last_write_cuts_the_rest_of_it() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 5, payloadLength: 12));
            var positions = RecordPositions(fileBytes);
            var recordLength = RecordFormat.GetLength(12);

            for (var offset = 0; offset < recordLength; offset++) {
                var flipped = (byte[])fileBytes.Clone();
                flipped[positions[3] + offset] ^= 0xFF;
                await File.WriteAllBytesAsync(path, flipped, TestContext.Current.CancellationToken);

                using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                segment.LastIndex.Should().Be(3);
                segment.Length.Should().Be(positions[3]);
                new FileInfo(path).Length.Should().Be(positions[3]);
            }
        }

        [Fact]
        public async Task A_bad_record_before_a_later_write_is_damage() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path,
                MakeEntries(1, 3, payloadLength: 10),
                MakeEntries(4, 3, payloadLength: 12),
                MakeEntries(7, 3, payloadLength: 14));
            var positions = RecordPositions(fileBytes);

            // A flipped byte in record 2 or in record 5: the first record of a later
            // write proves the damaged record was already on disk, so it is damage,
            // not a torn tail.
            foreach (var (record, payloadLength) in new[] { (1, 10), (4, 12) }) {
                var recordLength = RecordFormat.GetLength(payloadLength);
                for (var offset = 0; offset < recordLength; offset++) {
                    var flipped = (byte[])fileBytes.Clone();
                    flipped[positions[record] + offset] ^= 0xFF;
                    await File.WriteAllBytesAsync(path, flipped, TestContext.Current.CancellationToken);

                    var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                    await open.Should().ThrowExactlyAsync<InvalidDataException>();

                    // Nothing is cut: the file is unchanged.
                    new FileInfo(path).Length.Should().Be(flipped.Length);
                    (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(flipped);
                }
            }
        }

        [Fact]
        public async Task A_bad_length_field_before_a_later_write_is_damage() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path,
                MakeEntries(1, 3, payloadLength: 10),
                MakeEntries(4, 3, payloadLength: 12),
                MakeEntries(7, 3, payloadLength: 14));
            var positions = RecordPositions(fileBytes);

            // The length field of record 5 made larger, so that the record reads as
            // incomplete up to the end of the file.
            var damaged = (byte[])fileBytes.Clone();
            damaged[positions[4]] = 0xFF;
            await File.WriteAllBytesAsync(path, damaged, TestContext.Current.CancellationToken);

            var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await open.Should().ThrowExactlyAsync<InvalidDataException>();

            // Nothing is cut: the file is unchanged.
            new FileInfo(path).Length.Should().Be(damaged.Length);
            (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(damaged);
        }

        [Fact]
        public async Task The_first_record_of_a_write_damaged_is_found_by_the_next() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path,
                MakeEntries(1, 3, payloadLength: 10),
                MakeEntries(4, 3, payloadLength: 12),
                MakeEntries(7, 3, payloadLength: 14));
            var positions = RecordPositions(fileBytes);
            var recordLength = RecordFormat.GetLength(12);

            // Record 4 is itself the first record of its write; the damage is found
            // through record 7, the first record of the write after it.
            for (var offset = 0; offset < recordLength; offset++) {
                var flipped = (byte[])fileBytes.Clone();
                flipped[positions[3] + offset] ^= 0xFF;
                await File.WriteAllBytesAsync(path, flipped, TestContext.Current.CancellationToken);

                var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                await open.Should().ThrowExactlyAsync<InvalidDataException>();

                // Nothing is cut: the file is unchanged.
                new FileInfo(path).Length.Should().Be(flipped.Length);
                (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(flipped);
            }
        }

        [Fact]
        public async Task The_case_that_stays_a_torn_tail() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path,
                MakeEntries(1, 3, payloadLength: 10),
                MakeEntries(4, 3, payloadLength: 12),
                MakeEntries(7, 3, payloadLength: 14));
            var positions = RecordPositions(fileBytes);
            var fifthLength = RecordFormat.GetLength(12);
            var seventhLength = RecordFormat.GetLength(14);

            // Record 5 damaged and record 7, the first record of the last write, torn:
            // the damage looks like a torn tail and is cut, as the book says.
            foreach (var torn in new[] { 1, seventhLength / 2, seventhLength - 1 }) {
                for (var offset = 0; offset < fifthLength; offset++) {
                    var damaged = (byte[])fileBytes.Clone();
                    damaged[positions[4] + offset] ^= 0xFF;
                    await File.WriteAllBytesAsync(path, damaged.AsMemory(0, (int)(positions[6] + torn)), TestContext.Current.CancellationToken);

                    using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
                    segment.LastIndex.Should().Be(4);
                    segment.Length.Should().Be(positions[4]);
                    new FileInfo(path).Length.Should().Be(positions[4]);

                    for (var index = 1ul; index <= 4; index++) {
                        var buffer = new byte[segment.GetRecordLength(index)];
                        var entry = await segment.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                        entry.Payload.Span.SequenceEqual(MakePayload(index, index <= 3 ? 10 : 12)).Should().BeTrue();
                    }
                }
            }
        }

        [Fact]
        public async Task A_record_with_the_wrong_index_is_cut() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            // Records 1, 2 and 4 in one write, with valid check numbers.
            var bytes = EncodeRecords(
                (MakeEntry(1), true),
                (MakeEntry(2), false),
                (MakeEntry(4), false));
            await File.WriteAllBytesAsync(path, bytes, TestContext.Current.CancellationToken);

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(2);
                segment.Length.Should().Be(RecordFormat.GetLength(16) * 2);
            }

            // In a separate write behind them, a record 5 marked first: the record
            // with the wrong index was already on disk, so it is damage.
            var withLaterWrite = EncodeRecords(
                (MakeEntry(1), true),
                (MakeEntry(2), false),
                (MakeEntry(4), false),
                (MakeEntry(5), true));
            await File.WriteAllBytesAsync(path, withLaterWrite, TestContext.Current.CancellationToken);

            var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await open.Should().ThrowExactlyAsync<InvalidDataException>();

            // Nothing is cut: the file is unchanged.
            new FileInfo(path).Length.Should().Be(withLaterWrite.Length);
            (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(withLaterWrite);
        }

        [Fact]
        public async Task Records_are_marked_first_of_their_write() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 1));
                await segment.AppendAsync(MakeEntries(2, 3));
                await segment.AppendAsync(MakeEntries(5, 5));
            }

            var fileBytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            var expectedFirst = new[] { true, true, false, false, true, false, false, false, false };
            var offset = 0;
            var index = 1ul;
            foreach (var isFirst in expectedFirst) {
                RecordFormat.TryRead(fileBytes.AsSpan(offset), out var header, out var length).Should().Be(RecordStatus.Complete);
                header.IsFirstOfWrite.Should().Be(isFirst);
                header.Index.Should().Be(index);
                offset += length;
                index++;
            }

            offset.Should().Be(fileBytes.Length);
        }

        [Fact]
        public async Task An_empty_or_new_file_is_an_empty_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(0);
                segment.Length.Should().Be(0);
                await segment.AppendAsync(MakeEntries(1, 2));
                segment.LastIndex.Should().Be(2);
            }

            var otherPath = directory.GetFilePath("other.seg");
            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, otherPath, 1001, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(1000);
                segment.Length.Should().Be(0);
                await segment.AppendAsync(MakeEntries(1001, 2));
                segment.LastIndex.Should().Be(1002);
                segment.GetTerm(1002).Should().Be(1);
            }
        }

        [Fact]
        public async Task Refuses_first_index_zero() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 0, false, TestContext.Current.CancellationToken);

            await open.Should().ThrowAsync<ArgumentOutOfRangeException>();
            File.Exists(path).Should().BeFalse();
        }

        [Fact]
        public async Task Append_after_recovery_then_recover_again() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 2, payloadLength: 20));
            var tornLength = RecordFormat.GetLength(10) * 2 + 7;
            await File.WriteAllBytesAsync(path, fileBytes.AsMemory(0, tornLength), TestContext.Current.CancellationToken);

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(2);
                await segment.AppendAsync(MakeEntries(3, 2, payloadLength: 20, seed: 1));
            }

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(4);

                long sum = 0;
                for (var index = 1ul; index <= 4; index++) {
                    var buffer = new byte[segment.GetRecordLength(index)];
                    var entry = await segment.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                    entry.Index.Should().Be(index);
                    entry.Payload.Span.SequenceEqual(MakePayload(index, index <= 2 ? 10 : 20, seed: index <= 2 ? 0 : 1)).Should().BeTrue();
                    sum += segment.GetRecordLength(index);
                }

                sum.Should().Be(segment.Length);
                new FileInfo(path).Length.Should().Be(segment.Length);
            }
        }

        [Fact]
        public async Task A_large_record_is_recovered() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            const int bigPayloadLength = 1024 * 1024;

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 1, payloadLength: 8));
                await segment.AppendAsync(new[] { MakeEntry(2, payloadLength: bigPayloadLength) });
                await segment.AppendAsync(MakeEntries(3, 1, payloadLength: 9));
            }

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(3);

                var buffer = new byte[segment.GetRecordLength(2)];
                buffer.Length.Should().Be(RecordFormat.GetLength(bigPayloadLength));
                var entry = await segment.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
                entry.Payload.Span.SequenceEqual(MakePayload(2, bigPayloadLength)).Should().BeTrue();
            }
        }
    }
    public class Sealed {
        [Fact]
        public async Task A_sealed_segment_refuses_a_torn_tail() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 1, payloadLength: 40));
            var startOfThird = RecordFormat.GetLength(10) * 2;
            var endOfThird = startOfThird + RecordFormat.GetLength(40);

            // Every torn length of the third record, from its first byte to one byte
            // before its end: a sealed segment was complete before it was sealed.
            for (var length = startOfThird + 1; length < endOfThird; length++) {
                var torn = fileBytes.AsMemory(0, length).ToArray();
                await File.WriteAllBytesAsync(path, torn, TestContext.Current.CancellationToken);

                var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, isSealed: true, TestContext.Current.CancellationToken);
                await open.Should().ThrowExactlyAsync<InvalidDataException>();

                // Nothing is cut: the file is unchanged.
                new FileInfo(path).Length.Should().Be(length);
                (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(torn);
            }
        }

        [Fact]
        public async Task A_sealed_segment_refuses_a_flipped_byte() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            var fileBytes = await WriteWritesAsync(path, MakeEntries(1, 2, payloadLength: 10), MakeEntries(3, 1, payloadLength: 40));
            var startOfThird = RecordFormat.GetLength(10) * 2;
            var thirdLength = RecordFormat.GetLength(40);

            for (var offset = 0; offset < thirdLength; offset++) {
                var flipped = (byte[])fileBytes.Clone();
                flipped[startOfThird + offset] ^= 0xFF;
                await File.WriteAllBytesAsync(path, flipped, TestContext.Current.CancellationToken);

                var open = async () => await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, isSealed: true, TestContext.Current.CancellationToken);
                await open.Should().ThrowExactlyAsync<InvalidDataException>();

                // Nothing is cut: the file is unchanged.
                new FileInfo(path).Length.Should().Be(flipped.Length);
                (await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken)).Should().Equal(flipped);
            }
        }
    }
    public class TruncateFrom {
        [Fact]
        public async Task TruncateFrom_cuts_and_fsyncs() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            var stream = new CountingFile(path);

            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 10));
                var flushesBefore = stream.FlushToDiskCount;
                var lengthOfOneToFive = RecordFormat.GetLength(16) * 5;

                segment.TruncateFrom(6);

                stream.FlushToDiskCount.Should().Be(flushesBefore + 1);
                segment.LastIndex.Should().Be(5);
                segment.Length.Should().Be(lengthOfOneToFive);
                new FileInfo(path).Length.Should().Be(lengthOfOneToFive);

                await segment.AppendAsync(MakeEntries(6, 2, seed: 1));
                segment.LastIndex.Should().Be(7);
            }

            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken)) {
                segment.LastIndex.Should().Be(7);
                for (var index = 1ul; index <= 7; index++) {
                    var buffer = new byte[segment.GetRecordLength(index)];
                    var entry = await segment.ReadAsync(index, buffer, TestContext.Current.CancellationToken);
                    entry.Payload.Span.SequenceEqual(MakePayload(index, 16, seed: index < 6 ? 0 : 1)).Should().BeTrue();
                }
            }
        }

        [Fact]
        public async Task TruncateFrom_the_first_index_empties_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);

            await segment.AppendAsync(MakeEntries(1, 5));

            segment.TruncateFrom(1);

            segment.LastIndex.Should().Be(0);
            segment.Length.Should().Be(0);
            new FileInfo(path).Length.Should().Be(0);

            await segment.AppendAsync(MakeEntries(1, 2, seed: 1));
            segment.LastIndex.Should().Be(2);
        }

        [Fact]
        public async Task TruncateFrom_past_the_end_does_nothing() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            var stream = new CountingFile(path);

            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 5));
                var flushesBefore = stream.FlushToDiskCount;
                var lengthBefore = segment.Length;

                segment.TruncateFrom(6);

                stream.FlushToDiskCount.Should().Be(flushesBefore);
                segment.LastIndex.Should().Be(5);
                segment.Length.Should().Be(lengthBefore);
                new FileInfo(path).Length.Should().Be(lengthBefore);
            }

            // On an empty segment its first index is one past its last: also nothing.
            var emptyPath = directory.GetFilePath("empty.seg");
            using (var segment = await Segment.OpenAsync(DiskFileSystem.Instance, emptyPath, 1, false, TestContext.Current.CancellationToken)) {
                segment.TruncateFrom(1);

                segment.LastIndex.Should().Be(0);
                segment.Length.Should().Be(0);
                new FileInfo(emptyPath).Length.Should().Be(0);
            }
        }

        [Fact]
        public async Task TruncateFrom_refuses_an_index_outside_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 5));

            var tooLow = () => segment.TruncateFrom(0);
            tooLow.Should().ThrowExactly<ArgumentOutOfRangeException>();

            var tooHigh = () => segment.TruncateFrom(7);
            tooHigh.Should().ThrowExactly<ArgumentOutOfRangeException>();

            // The segment is still usable.
            segment.LastIndex.Should().Be(5);
            await segment.AppendAsync(MakeEntries(6, 1));
            segment.LastIndex.Should().Be(6);
        }

        [Fact]
        public async Task A_failed_truncate_faults_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            using (var stream = new FaultyFile(path))
            using (var segment = await Segment.OpenAsync(stream, 1, false, TestContext.Current.CancellationToken)) {
                await segment.AppendAsync(MakeEntries(1, 3));
                var recordLength = segment.GetRecordLength(1);

                stream.FailNextSetLength = true;

                var truncate = () => segment.TruncateFrom(2);
                var fault = truncate.Should().ThrowExactly<IOException>().Which;

                var appendAgain = async () => await segment.AppendAsync(MakeEntries(3, 1));
                (await appendAgain.Should().ThrowExactlyAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);

                var buffer = new byte[recordLength];
                var readAgain = async () => await segment.ReadAsync(1, buffer, TestContext.Current.CancellationToken);
                (await readAgain.Should().ThrowExactlyAsync<IOException>().WaitAsync(Timeout, TestContext.Current.CancellationToken)).Which.Should().BeSameAs(fault);

                var truncateAgain = () => segment.TruncateFrom(3);
                truncateAgain.Should().ThrowExactly<IOException>().Which.Should().BeSameAs(fault);
            }
        }
    }
    public class ReadAsync {
        [Fact]
        public async Task Refuses_an_index_outside_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 2));

            var buffer = new byte[RecordFormat.GetLength(16)];

            var tooLow = async () => await segment.ReadAsync(0, buffer, TestContext.Current.CancellationToken);
            await tooLow.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();

            var tooHigh = async () => await segment.ReadAsync(3, buffer, TestContext.Current.CancellationToken);
            await tooHigh.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task Refuses_a_too_short_buffer() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 1));

            var buffer = new byte[RecordFormat.GetLength(16) - 1];
            var act = async () => await segment.ReadAsync(1, buffer, TestContext.Current.CancellationToken);

            await act.Should().ThrowExactlyAsync<ArgumentException>();
        }

        [Fact]
        public async Task Reports_damage_after_recovery() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            // Opened with a share for writing, so the other handle below can damage
            // the file behind the segment's back.
            using var file = new DiskFile(File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, FileOptions.Asynchronous), path);
            using var segment = await Segment.OpenAsync(file, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 3));

            var recordLength = RecordFormat.GetLength(16);
            using (var other = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
                other.Position = recordLength + RecordFormat.HeaderLength;
                var original = other.ReadByte();
                other.Position = recordLength + RecordFormat.HeaderLength;
                other.WriteByte((byte)(original ^ 0xFF));
                other.Flush(flushToDisk: true);
            }

            var buffer = new byte[segment.GetRecordLength(2)];
            var act = async () => await segment.ReadAsync(2, buffer, TestContext.Current.CancellationToken);
            await act.Should().ThrowExactlyAsync<InvalidDataException>();

            var first = new byte[segment.GetRecordLength(1)];
            var entry1 = await segment.ReadAsync(1, first, TestContext.Current.CancellationToken);
            entry1.Payload.Span.SequenceEqual(MakePayload(1, 16)).Should().BeTrue();

            var third = new byte[segment.GetRecordLength(3)];
            var entry3 = await segment.ReadAsync(3, third, TestContext.Current.CancellationToken);
            entry3.Payload.Span.SequenceEqual(MakePayload(3, 16)).Should().BeTrue();
        }
    }

    public class RangeReadAsync {
        [Fact]
        public async Task Reads_a_range_in_one_batch() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 3));

            var buffer = new byte[3 * RecordFormat.GetLength(16)];
            var entries = new Entry[3];
            var count = await segment.ReadAsync(1, 3, buffer, entries, TestContext.Current.CancellationToken);

            count.Should().Be(3);
            for (var i = 0; i < count; i++) {
                entries[i].Index.Should().Be(1UL + (ulong)i);
                entries[i].Term.Should().Be(1UL);
                entries[i].Payload.Span.SequenceEqual(MakePayload(1 + (ulong)i, 16)).Should().BeTrue();
            }

            // The payloads point into the buffer: filling it overwrites them.
            Array.Fill(buffer, (byte)0xA5);
            foreach (var entry in entries)
                entry.Payload.ToArray().Should().OnlyContain(b => b == 0xA5);
        }

        [Fact]
        public async Task The_batch_stops_when_the_buffer_or_the_entry_array_is_full() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 5));

            var recordLength = RecordFormat.GetLength(16);

            // The buffer holds two records, the entry array five.
            var buffer = new byte[2 * recordLength];
            var entries = new Entry[5];
            var count = await segment.ReadAsync(1, 5, buffer, entries, TestContext.Current.CancellationToken);
            count.Should().Be(2);

            // The entry array holds one, the buffer five.
            buffer = new byte[5 * recordLength];
            entries = new Entry[1];
            count = await segment.ReadAsync(3, 5, buffer, entries, TestContext.Current.CancellationToken);
            count.Should().Be(1);
            entries[0].Index.Should().Be(3UL);
        }

        [Fact]
        public async Task Refuses_a_buffer_that_cannot_hold_the_first_record() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 2));

            var buffer = new byte[RecordFormat.GetLength(16) - 1];
            var act = async () => await segment.ReadAsync(1, 2, buffer, new Entry[2], TestContext.Current.CancellationToken);

            await act.Should().ThrowExactlyAsync<ArgumentException>();
        }

        [Fact]
        public async Task Throws_on_an_index_outside_the_segment() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");
            using var segment = await Segment.OpenAsync(DiskFileSystem.Instance, path, 3, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(3, 2));

            var buffer = new byte[2 * RecordFormat.GetLength(16)];
            var entries = new Entry[2];

            var below = async () => await segment.ReadAsync(2, 3, buffer, entries, TestContext.Current.CancellationToken);
            await below.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();

            var above = async () => await segment.ReadAsync(5, 5, buffer, entries, TestContext.Current.CancellationToken);
            await above.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();

            var lastBelowFirst = async () => await segment.ReadAsync(4, 3, buffer, entries, TestContext.Current.CancellationToken);
            await lastBelowFirst.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task Reports_damage_after_recovery() {
            using var directory = new TempDirectory();
            var path = directory.GetFilePath("segment.seg");

            // Opened with a share for writing, so the other handle below can damage
            // the file behind the segment's back.
            using var file = new DiskFile(File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, FileOptions.Asynchronous), path);
            using var segment = await Segment.OpenAsync(file, 1, false, TestContext.Current.CancellationToken);
            await segment.AppendAsync(MakeEntries(1, 3));

            var recordLength = RecordFormat.GetLength(16);
            using (var other = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
                other.Position = recordLength + RecordFormat.HeaderLength;
                var original = other.ReadByte();
                other.Position = recordLength + RecordFormat.HeaderLength;
                other.WriteByte((byte)(original ^ 0xFF));
                other.Flush(flushToDisk: true);
            }

            // Every record of the batch is checked: the damaged second one refuses.
            var buffer = new byte[3 * recordLength];
            var entries = new Entry[3];
            var act = async () => await segment.ReadAsync(1, 3, buffer, entries, TestContext.Current.CancellationToken);
            await act.Should().ThrowExactlyAsync<InvalidDataException>();

            // The records around the damaged one still read.
            var first = await segment.ReadAsync(1, 1, new byte[recordLength], new Entry[1], TestContext.Current.CancellationToken);
            first.Should().Be(1);
            var third = await segment.ReadAsync(3, 3, new byte[recordLength], new Entry[1], TestContext.Current.CancellationToken);
            third.Should().Be(1);
        }
    }
}
