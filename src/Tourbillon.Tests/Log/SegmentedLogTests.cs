using AwesomeAssertions;

namespace Tourbillon.Log;

public class SegmentedLogTests {
    // A record of 180 payload bytes takes 207 bytes: 20 entries fill a 4 KB segment
    // (4140 bytes) and 19 do not (3933), so batches of 5 rotate every 20 entries.
    const long SegmentSize = 4096;
    const int PayloadLength = 180;

    static byte[] MakePayload(ulong index, int length, int seed = 0) {
        var payload = new byte[length];
        for (var i = 0; i < length; i++)
            payload[i] = (byte)(index * 31 + (ulong)seed * 17 + (ulong)i * 7 + 3);

        return payload;
    }

    static Entry MakeEntry(ulong index, EntryFlags flags = EntryFlags.None, int payloadLength = PayloadLength, int seed = 0) =>
        new(index, 1, EntryKind.Command, flags, MakePayload(index, payloadLength, seed));

    static Entry[] MakeEntries(ulong firstIndex, int count, EntryFlags flags = EntryFlags.None, int payloadLength = PayloadLength, int seed = 0) {
        var entries = new Entry[count];
        for (var i = 0; i < count; i++)
            entries[i] = MakeEntry(firstIndex + (ulong)i, flags, payloadLength, seed);

        return entries;
    }

    static StorageLimit UnlimitedStorage() => new(() => long.MaxValue, maximumSize: null, reserveSize: 0);

    static ValueTask<SegmentedLog> OpenLogAsync(TempDirectory directory, long segmentSize = SegmentSize, StorageLimit? storage = null) =>
        SegmentedLog.OpenAsync(directory.Path, segmentSize, storage ?? UnlimitedStorage(), TestContext.Current.CancellationToken);

    static string SegmentName(ulong firstIndex) => $"{firstIndex:D20}.seg";

    // The names are 20 digits, so sorting them puts the segments in index order; the
    // file system itself returns them in no particular order.
    static string[] SegmentFiles(TempDirectory directory) {
        var files = Directory.GetFiles(directory.Path, "*.seg");
        Array.Sort(files, StringComparer.Ordinal);
        return files;
    }

    // Reads a segment file while the log still holds it open: the log shares it only
    // for reading, so this read must allow the log's writing (Windows refuses
    // otherwise).
    static async Task<byte[]> ReadWhileOpenAsync(string path) {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var bytes = new byte[file.Length];
        await file.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);
        return bytes;
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

    static async Task AssertEntryReads(SegmentedLog log, Entry expected) {
        log.GetTerm(expected.Index).Should().Be(expected.Term);
        log.GetRecordLength(expected.Index).Should().Be(RecordFormat.GetLength(expected.Payload.Length));

        var buffer = new byte[log.GetRecordLength(expected.Index)];
        var entry = await log.ReadAsync(expected.Index, buffer, TestContext.Current.CancellationToken);
        entry.Index.Should().Be(expected.Index);
        entry.Term.Should().Be(expected.Term);
        entry.Kind.Should().Be(expected.Kind);
        entry.Flags.Should().Be(expected.Flags);
        entry.Payload.Span.SequenceEqual(expected.Payload.Span).Should().BeTrue();
    }

    // Writes 60 entries in batches of 5: segments 1-20, 21-40 and 41-60.
    static async Task WriteThreeSegmentsAsync(TempDirectory directory) {
        using var log = await OpenLogAsync(directory);
        for (var first = 1; first <= 60; first += 5)
            await log.AppendAsync(MakeEntries((ulong)first, 5));
    }

    // Writes 100 entries of 200 bytes in batches of 3: segments 1-21, 22-42, 43-63,
    // 64-84 and 85-100.
    static async Task<List<Entry>> WriteHundredEntriesAsync(TempDirectory directory) {
        var expected = new List<Entry>();
        for (var first = 1; first <= 100; first += 3)
            expected.AddRange(MakeEntries((ulong)first, Math.Min(3, 101 - first), payloadLength: 200));

        using var log = await OpenLogAsync(directory, segmentSize: 4096);
        foreach (var batch in expected.Chunk(3))
            await log.AppendAsync(batch);

        return expected;
    }

    public class AppendAsync {
        [Fact]
        public async Task A_failed_rotation_faults_the_log() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory, segmentSize: 1024);

            await log.AppendAsync(MakeEntries(1, 5));

            // A directory with the name of the next segment: the new file cannot be
            // created, on every platform.
            Directory.CreateDirectory(Path.Combine(directory.Path, SegmentName(6)));

            var append = async () => await log.AppendAsync(MakeEntries(6, 1));
            var exception = (await append.Should().ThrowAsync<Exception>()).Which;

            // Every later call throws the fault; the old segment is untouched.
            var again = async () => await log.AppendAsync(MakeEntries(6, 1));
            (await again.Should().ThrowExactlyAsync<IOException>()).Which.InnerException.Should().BeSameAs(exception);
            var truncate = () => log.TruncateFrom(3);
            truncate.Should().ThrowExactly<IOException>();
            log.LastIndex.Should().Be(5);
            new FileInfo(Path.Combine(directory.Path, SegmentName(1))).Length.Should().Be(5 * RecordFormat.GetLength(PayloadLength));
        }

        [Fact]
        public async Task Rotates_at_the_segment_size() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory, segmentSize: 4096);

            for (var first = 1; first <= 100; first += 3)
                await log.AppendAsync(MakeEntries((ulong)first, Math.Min(3, 101 - first), payloadLength: 200));

            log.FirstIndex.Should().Be(1);
            log.LastIndex.Should().Be(100);

            var files = SegmentFiles(directory);
            files.Should().HaveCount(5);
            files.Select(Path.GetFileName).Should().Equal(
                SegmentName(1), SegmentName(22), SegmentName(43), SegmentName(64), SegmentName(85));

            // Every sealed segment is at or above the size and below size + one batch
            // (a batch of 3 entries of 200 bytes takes 681 bytes).
            foreach (var file in files.Take(4)) {
                var length = new FileInfo(file).Length;
                length.Should().BeInRange(4096, 4096 + 681 - 1);
            }
        }

        [Fact]
        public async Task Reads_every_index_across_segments() {
            using var directory = new TempDirectory();
            var expected = await WriteHundredEntriesAsync(directory);

            using (var log = await OpenLogAsync(directory, segmentSize: 4096))
                foreach (var entry in expected)
                    await AssertEntryReads(log, entry);

            using (var log = await OpenLogAsync(directory, segmentSize: 4096))
                foreach (var entry in expected)
                    await AssertEntryReads(log, entry);
        }

        [Fact]
        public async Task A_batch_is_never_split() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory, segmentSize: 1024);

            // A batch of 10 entries of 200 bytes (2270 bytes) is larger than the
            // segment size: it lands in one segment.
            await log.AppendAsync(MakeEntries(1, 10, payloadLength: 200));

            log.LastIndex.Should().Be(10);
            var files = SegmentFiles(directory);
            files.Should().HaveCount(1);

            var records = DecodeRecords(await ReadWhileOpenAsync(files[0]));
            records.Should().HaveCount(10);
            records.Select(record => record.Index).Should().Equal(Enumerable.Range(1, 10).Select(i => (ulong)i));
        }

        [Fact]
        public async Task A_new_log_starts_at_one() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory);

            log.FirstIndex.Should().Be(1);
            log.LastIndex.Should().Be(0);
            log.Length.Should().Be(0);

            var files = SegmentFiles(directory);
            files.Should().HaveCount(1);
            Path.GetFileName(files[0]).Should().Be(SegmentName(1));
            new FileInfo(files[0]).Length.Should().Be(0);

            await log.AppendAsync(MakeEntries(1, 2));
            log.LastIndex.Should().Be(2);
        }
    }

    public class OpenAsync {
        [Fact]
        public async Task A_flipped_byte_in_a_sealed_segment_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            var sealedFiles = SegmentFiles(directory).Take(2).ToArray();
            var before = SegmentFiles(directory).ToDictionary(path => path, path => File.ReadAllBytes(path));
            var recordLength = RecordFormat.GetLength(PayloadLength);

            // A byte flipped in the first, a middle and the last record of a sealed
            // segment: damage, refused, and no file changed.
            foreach (var file in sealedFiles) {
                var fileBytes = File.ReadAllBytes(file);
                var positions = RecordPositions(fileBytes);
                foreach (var record in new[] { 0, positions.Length / 2, positions.Length - 1 }) {
                    for (var offset = 0; offset < recordLength; offset++) {
                        var flipped = (byte[])fileBytes.Clone();
                        flipped[positions[record] + offset] ^= 0xFF;
                        await File.WriteAllBytesAsync(file, flipped, TestContext.Current.CancellationToken);

                        var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
                        var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
                        exception.Message.Should().Contain(Path.GetFileName(file));

                        foreach (var path in SegmentFiles(directory))
                            File.ReadAllBytes(path).Should().Equal(path == file ? flipped : before[path]);
                    }
                }

                // Restore the original, so the next file's opens see it intact.
                await File.WriteAllBytesAsync(file, fileBytes, TestContext.Current.CancellationToken);
            }
        }

        [Fact]
        public async Task A_missing_segment_in_the_middle_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            File.Delete(Path.Combine(directory.Path, SegmentName(21)));

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
            exception.Message.Should().Contain("gap");
        }

        [Fact]
        public async Task A_gap_between_segments_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            // Rename the last segment to a first index one higher.
            File.Move(
                Path.Combine(directory.Path, SegmentName(41)),
                Path.Combine(directory.Path, SegmentName(42)));

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            await open.Should().ThrowExactlyAsync<InvalidDataException>();
        }

        [Fact]
        public async Task An_overlap_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            // Rename the last segment to a first index one lower: it overlaps the
            // segment before it.
            File.Move(
                Path.Combine(directory.Path, SegmentName(41)),
                Path.Combine(directory.Path, SegmentName(40)));

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            await open.Should().ThrowExactlyAsync<InvalidDataException>();
        }

        [Fact]
        public async Task A_sealed_segment_cut_short_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            // Drop the last record of the first, sealed segment: the file ends at a
            // record boundary, so only the gap check can catch it.
            var first = Path.Combine(directory.Path, SegmentName(1));
            var fileBytes = await File.ReadAllBytesAsync(first, TestContext.Current.CancellationToken);
            var positions = RecordPositions(fileBytes);
            await File.WriteAllBytesAsync(first, fileBytes.AsMemory(0, (int)positions[^1]), TestContext.Current.CancellationToken);

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
            exception.Message.Should().Contain("gap");
        }

        [Fact]
        public async Task A_segment_with_a_bad_name_refuses_to_open() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            await File.WriteAllBytesAsync(Path.Combine(directory.Path, "abc.seg"), new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
            exception.Message.Should().Contain("abc.seg");
        }

        [Fact]
        public async Task A_segment_named_index_zero_refuses_to_open() {
            using var directory = new TempDirectory();
            await File.WriteAllBytesAsync(Path.Combine(directory.Path, SegmentName(0)), [], TestContext.Current.CancellationToken);

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
            exception.Message.Should().Contain(SegmentName(0));
        }

        [Fact]
        public async Task A_torn_tail_in_the_last_segment_is_cut() {
            using var directory = new TempDirectory();
            await WriteThreeSegmentsAsync(directory);

            // Tear the last segment in the middle of its 16th record (index 56).
            var last = Path.Combine(directory.Path, SegmentName(41));
            var fileBytes = await File.ReadAllBytesAsync(last, TestContext.Current.CancellationToken);
            var positions = RecordPositions(fileBytes);
            await File.WriteAllBytesAsync(last, fileBytes.AsMemory(0, (int)positions[15] + 10), TestContext.Current.CancellationToken);

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(55);
                new FileInfo(last).Length.Should().Be(positions[15]);

                for (var index = 1ul; index <= 55; index++)
                    await AssertEntryReads(log, MakeEntry(index));
            }
        }

        [Fact]
        public async Task Other_files_are_left_alone() {
            using var directory = new TempDirectory();
            var stateFile = Path.Combine(directory.Path, "state.tmp");
            var termFile = Path.Combine(directory.Path, "term");
            await File.WriteAllBytesAsync(stateFile, new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(termFile, new byte[] { 4, 5 }, TestContext.Current.CancellationToken);

            using (var log = await OpenLogAsync(directory)) {
                await log.AppendAsync(MakeEntries(1, 5));
                log.LastIndex.Should().Be(5);
            }

            File.ReadAllBytes(stateFile).Should().Equal(new byte[] { 1, 2, 3 });
            File.ReadAllBytes(termFile).Should().Equal(new byte[] { 4, 5 });
            Directory.GetFiles(directory.Path).Should().HaveCount(3);
            SegmentFiles(directory).Should().HaveCount(1);

            using (var log = await OpenLogAsync(directory)) {
                log.LastIndex.Should().Be(5);
                await AssertEntryReads(log, MakeEntry(5));
            }
        }

        [Fact]
        public async Task An_empty_last_segment_recovers() {
            using var directory = new TempDirectory();
            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 25; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                log.LastIndex.Should().Be(25);
            }

            // A crash between the rotation and the first write leaves an empty segment.
            var empty = Path.Combine(directory.Path, SegmentName(26));
            await File.WriteAllBytesAsync(empty, Array.Empty<byte>(), TestContext.Current.CancellationToken);

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(25);

                await log.AppendAsync(MakeEntries(26, 5));
                log.LastIndex.Should().Be(30);
            }

            // The append went into the empty file.
            new FileInfo(empty).Length.Should().Be(RecordFormat.GetLength(PayloadLength) * 5);

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(30);
                for (var index = 1ul; index <= 30; index++)
                    await AssertEntryReads(log, MakeEntry(index));
            }
        }

        [Fact]
        public async Task An_empty_segment_with_a_wrong_name_refuses_to_open() {
            using var directory = new TempDirectory();
            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 25; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                log.LastIndex.Should().Be(25);
            }

            // An empty segment named after an index one too high: a gap.
            var wrong = Path.Combine(directory.Path, SegmentName(27));
            await File.WriteAllBytesAsync(wrong, Array.Empty<byte>(), TestContext.Current.CancellationToken);

            var open = async () => await SegmentedLog.OpenAsync(directory.Path, SegmentSize, UnlimitedStorage(), TestContext.Current.CancellationToken);
            var exception = (await open.Should().ThrowExactlyAsync<InvalidDataException>()).Which;
            exception.Message.Should().Contain("gap");
        }
    }
    public class TruncateFrom {
        [Fact]
        public async Task Truncate_inside_the_active_segment() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 30; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                log.TruncateFrom(25);

                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(24);
                SegmentFiles(directory).Should().HaveCount(2);

                await log.AppendAsync(MakeEntries(25, 6, seed: 1));
                log.LastIndex.Should().Be(30);
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(30);
                for (var index = 1ul; index <= 30; index++)
                    await AssertEntryReads(log, MakeEntry(index, seed: index < 25 ? 0 : 1));
            }
        }

        [Fact]
        public async Task Truncate_across_a_sealed_segment() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 60; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                // Segments 1-20, 21-40 and 41-60; truncate from an index in the first.
                log.TruncateFrom(10);

                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(9);
                var files = SegmentFiles(directory);
                files.Should().HaveCount(1);
                Path.GetFileName(files[0]).Should().Be(SegmentName(1));
                new FileInfo(files[0]).Length.Should().Be(RecordFormat.GetLength(PayloadLength) * 9);

                // Append until a rotation happens again.
                for (var first = 10; first <= 29; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5, seed: 1));

                log.LastIndex.Should().Be(29);
                SegmentFiles(directory).Should().HaveCount(2);
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(29);
                for (var index = 1ul; index <= 29; index++)
                    await AssertEntryReads(log, MakeEntry(index, seed: index < 10 ? 0 : 1));
            }
        }

        [Fact]
        public async Task Truncate_at_the_first_index_of_a_sealed_segment() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 60; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                // Segments 1-20, 21-40 and 41-60; truncate from the first index of
                // the second segment: it stays, empty, as the active one.
                log.TruncateFrom(21);

                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(20);
                var files = SegmentFiles(directory);
                files.Should().HaveCount(2);
                Path.GetFileName(files[0]).Should().Be(SegmentName(1));
                Path.GetFileName(files[1]).Should().Be(SegmentName(21));
                new FileInfo(files[1]).Length.Should().Be(0);

                await log.AppendAsync(MakeEntries(21, 5, seed: 1));
                log.LastIndex.Should().Be(25);
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(1);
                log.LastIndex.Should().Be(25);
                for (var index = 1ul; index <= 25; index++)
                    await AssertEntryReads(log, MakeEntry(index, seed: index < 21 ? 0 : 1));
            }
        }

        [Fact]
        public async Task Truncate_refuses_an_index_outside_the_log() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory);

            for (var first = 1; first <= 30; first += 5)
                await log.AppendAsync(MakeEntries((ulong)first, 5));

            var tooLow = () => log.TruncateFrom(0);
            tooLow.Should().ThrowExactly<ArgumentOutOfRangeException>();

            var tooHigh = () => log.TruncateFrom(32);
            tooHigh.Should().ThrowExactly<ArgumentOutOfRangeException>();

            // The log is still usable.
            log.LastIndex.Should().Be(30);
            await log.AppendAsync(MakeEntries(31, 1));
            log.LastIndex.Should().Be(31);
        }
    }
    public class RemoveUpToAsync {
        [Fact]
        public async Task RemoveUpTo_deletes_exactly_the_whole_segments() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 100; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                // Segments 1-20, 21-40, 41-60, 61-80 and 81-100.
                await log.RemoveUpToAsync(45);

                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(100);
                SegmentFiles(directory).Select(Path.GetFileName).Should().Equal(
                    SegmentName(41), SegmentName(61), SegmentName(81));

                var readRemoved = async () => await log.ReadAsync(40, new byte[RecordFormat.GetLength(PayloadLength)], TestContext.Current.CancellationToken);
                await readRemoved.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();
                await AssertEntryReads(log, MakeEntry(41));
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(100);
                await AssertEntryReads(log, MakeEntry(41));
                await AssertEntryReads(log, MakeEntry(100));
            }
        }

        [Fact]
        public async Task RemoveUpTo_at_a_segment_end() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 60; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                // Segments 1-20, 21-40 and 41-60; 40 is the end of the second.
                await log.RemoveUpToAsync(40);

                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(60);
                SegmentFiles(directory).Select(Path.GetFileName).Should().Equal(SegmentName(41));
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(60);
            }
        }

        [Fact]
        public async Task RemoveUpTo_the_last_index_starts_an_empty_segment() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 60; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                // Let go of every entry: an empty segment starts after it.
                await log.RemoveUpToAsync(60);

                log.FirstIndex.Should().Be(61);
                log.LastIndex.Should().Be(60);
                var files = SegmentFiles(directory);
                files.Should().HaveCount(1);
                Path.GetFileName(files[0]).Should().Be(SegmentName(61));
                new FileInfo(files[0]).Length.Should().Be(0);

                // The append continues the indexes.
                await log.AppendAsync(MakeEntries(61, 5, seed: 1));
                log.LastIndex.Should().Be(65);
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(61);
                log.LastIndex.Should().Be(65);
                await AssertEntryReads(log, MakeEntry(61, seed: 1));
                await AssertEntryReads(log, MakeEntry(65, seed: 1));
            }
        }

        [Fact]
        public async Task RemoveUpTo_below_the_first_index_does_nothing() {
            using var directory = new TempDirectory();

            using (var log = await OpenLogAsync(directory)) {
                for (var first = 1; first <= 60; first += 5)
                    await log.AppendAsync(MakeEntries((ulong)first, 5));

                await log.RemoveUpToAsync(40);
                log.FirstIndex.Should().Be(41);
                var filesBefore = SegmentFiles(directory);

                await log.RemoveUpToAsync(20);

                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(60);
                SegmentFiles(directory).Should().Equal(filesBefore);
            }

            using (var log = await OpenLogAsync(directory)) {
                log.FirstIndex.Should().Be(41);
                log.LastIndex.Should().Be(60);
            }
        }

        [Fact]
        public async Task RemoveUpTo_above_the_last_index_throws() {
            using var directory = new TempDirectory();
            using var log = await OpenLogAsync(directory);

            for (var first = 1; first <= 30; first += 5)
                await log.AppendAsync(MakeEntries((ulong)first, 5));

            var tooHigh = async () => await log.RemoveUpToAsync(31);
            await tooHigh.Should().ThrowExactlyAsync<ArgumentOutOfRangeException>();

            // The log is still usable.
            log.LastIndex.Should().Be(30);
            await log.AppendAsync(MakeEntries(31, 1));
            log.LastIndex.Should().Be(31);
        }
    }
    public class Storage {
        [Fact]
        public async Task Unflagged_entries_are_refused_at_the_reserve() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: storage);

            var appended = 0;
            while (true) {
                var batch = MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048);
                var written = await log.AppendAsync(batch);
                appended += written;
                if (written < batch.Length)
                    break;
            }

            // The bytes used are at most the maximum minus the reserve.
            storage.Used.Should().BeLessThanOrEqualTo(30 * 1024 - 2 * 1024);
            log.LastIndex.Should().Be((ulong)appended);

            // The file holds nothing of the refused entries.
            var files = SegmentFiles(directory);
            files.Should().HaveCount(1);
            var records = DecodeRecords(await ReadWhileOpenAsync(files[0]));
            records.Should().HaveCount(appended);
            records.Select(record => record.Index).Should().Equal(Enumerable.Range(1, appended).Select(i => (ulong)i));
        }

        [Fact]
        public async Task Flagged_entries_use_the_reserve() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: storage);

            var appended = 0;
            while (true) {
                var batch = MakeEntries((ulong)(appended + 1), 5, EntryFlags.MayUseReserve, payloadLength: 2048);
                var written = await log.AppendAsync(batch);
                appended += written;
                if (written < batch.Length)
                    break;
            }

            // Flagged entries are accepted up to the maximum.
            storage.Used.Should().BeLessThanOrEqualTo(30 * 1024);
            storage.Used.Should().BeGreaterThan(30 * 1024 - 5 * RecordFormat.GetLength(2048));
            log.LastIndex.Should().Be((ulong)appended);

            // None beyond it: no write, nothing changes.
            var lengthBefore = log.Length;
            var lastIndexBefore = log.LastIndex;
            var refused = await log.AppendAsync(MakeEntries((ulong)(appended + 1), 5, EntryFlags.MayUseReserve, payloadLength: 2048));
            refused.Should().Be(0);
            log.LastIndex.Should().Be(lastIndexBefore);
            log.Length.Should().Be(lengthBefore);
            new FileInfo(SegmentFiles(directory)[0]).Length.Should().Be(lengthBefore);
        }

        [Fact]
        public async Task A_mixed_batch_stops_at_the_first_that_does_not_fit() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 100 * 1024, reserveSize: 20 * 1024);
            storage.Add(79 * 1024);
            using var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: storage);

            // The example of the design: at used 79 KB the room is 21 KB, so the
            // flagged first entry fits, the unflagged second does not, and the third
            // is never looked at.
            var batch = new[] {
                MakeEntry(1, EntryFlags.MayUseReserve, payloadLength: 2048),
                MakeEntry(2, EntryFlags.None, payloadLength: 2048),
                MakeEntry(3, EntryFlags.MayUseReserve, payloadLength: 2048),
            };

            var written = await log.AppendAsync(batch);

            written.Should().Be(1);
            log.LastIndex.Should().Be(1);
            var records = DecodeRecords(await ReadWhileOpenAsync(SegmentFiles(directory)[0]));
            records.Should().HaveCount(1);
            records[0].Index.Should().Be(1);
        }

        [Fact]
        public async Task Prefix_removal_frees_space() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, storage: storage);

            var appended = 0;
            while (true) {
                var batch = MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048);
                var written = await log.AppendAsync(batch);
                appended += written;
                if (written < batch.Length)
                    break;
            }

            var refused = await log.AppendAsync(MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048));
            refused.Should().Be(0);

            // Remove the sealed segments: their space is free again.
            await log.RemoveUpToAsync((ulong)(appended - 1));

            var afterRemoval = await log.AppendAsync(MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048));
            afterRemoval.Should().Be(5);
            log.LastIndex.Should().Be((ulong)(appended + 5));
        }

        [Fact]
        public async Task Truncate_frees_space() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, storage: storage);

            var appended = 0;
            while (true) {
                var batch = MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048);
                var written = await log.AppendAsync(batch);
                appended += written;
                if (written < batch.Length)
                    break;
            }

            var refused = await log.AppendAsync(MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048));
            refused.Should().Be(0);

            // Truncate inside the active segment: the freed bytes are free again.
            log.TruncateFrom((ulong)(appended - 1));
            var firstNew = log.LastIndex + 1;

            var afterTruncate = await log.AppendAsync(MakeEntries(firstNew, 5, payloadLength: 2048));
            afterTruncate.Should().BeGreaterThan(0);
            log.LastIndex.Should().Be(firstNew + (ulong)afterTruncate - 1);
        }

        [Fact]
        public async Task The_limit_is_shared_by_logs() {
            using var firstDirectory = new TempDirectory();
            using var secondDirectory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var first = await OpenLogAsync(firstDirectory, segmentSize: 1024 * 1024, storage: storage);
            using var second = await OpenLogAsync(secondDirectory, segmentSize: 1024 * 1024, storage: storage);

            // Append to both, back to back: the second is refused once both together
            // reach the maximum minus the reserve.
            var firstIndex = 1;
            var secondIndex = 1;
            while (true) {
                var writtenFirst = await first.AppendAsync(MakeEntries((ulong)firstIndex, 5, payloadLength: 2048));
                firstIndex += writtenFirst;
                var writtenSecond = await second.AppendAsync(MakeEntries((ulong)secondIndex, 5, payloadLength: 2048));
                secondIndex += writtenSecond;
                if (writtenSecond == 0)
                    break;
            }

            storage.Used.Should().BeLessThanOrEqualTo(30 * 1024 - 2 * 1024);
            storage.Used.Should().BeGreaterThan(30 * 1024 - 2 * 1024 - 5 * RecordFormat.GetLength(2048));
            second.LastIndex.Should().Be((ulong)(secondIndex - 1));
        }

        [Fact]
        public async Task Free_disk_space_is_a_limit() {
            using var directory = new TempDirectory();
            // No maximum: the free space of the disk is the limit.
            var storage = new StorageLimit(() => 10 * 1024, maximumSize: null, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: storage);

            // Unflagged entries are refused at the free space minus the reserve.
            var written = await log.AppendAsync(MakeEntries(1, 5, payloadLength: 2048));
            written.Should().Be(3);

            // Flagged entries use the reserve, up to the free space.
            written = await log.AppendAsync(MakeEntries(4, 5, EntryFlags.MayUseReserve, payloadLength: 2048));
            written.Should().Be(4);
        }

        [Fact]
        public async Task Reading_and_removal_go_on_when_full() {
            using var directory = new TempDirectory();
            var storage = new StorageLimit(() => long.MaxValue, maximumSize: 30 * 1024, reserveSize: 2 * 1024);
            using var log = await OpenLogAsync(directory, storage: storage);

            var appended = 0;
            while (true) {
                var batch = MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048);
                var written = await log.AppendAsync(batch);
                appended += written;
                if (written < batch.Length)
                    break;
            }

            var refused = await log.AppendAsync(MakeEntries((ulong)(appended + 1), 5, payloadLength: 2048));
            refused.Should().Be(0);

            // Reading goes on.
            await AssertEntryReads(log, MakeEntry(1, payloadLength: 2048));

            // Removal goes on: the sealed segments go, the active one stays.
            await log.RemoveUpToAsync(10);
            log.FirstIndex.Should().Be(11);
            await AssertEntryReads(log, MakeEntry(11, payloadLength: 2048));
        }

        [Fact]
        public async Task Used_is_restored_on_open() {
            using var directory = new TempDirectory();
            var first = new StorageLimit(() => long.MaxValue, maximumSize: null, reserveSize: 0);
            long length;
            using (var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: first)) {
                await log.AppendAsync(MakeEntries(1, 10, payloadLength: 2048));
                length = log.Length;
            }

            // Closing the log gives its bytes back.
            first.Used.Should().Be(0);

            // Reopening on a fresh limit counts them again.
            var second = new StorageLimit(() => long.MaxValue, maximumSize: null, reserveSize: 0);
            using (var log = await OpenLogAsync(directory, segmentSize: 1024 * 1024, storage: second))
                second.Used.Should().Be(length);
        }
    }
}
