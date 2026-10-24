using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using System.Text;
using AwesomeAssertions;

namespace Tourbillon.Log;

public class Crc32CTests {
    // The standard CRC-32C check value for ASCII "123456789", cited throughout CRC
    // literature (e.g. the CRC RevEng catalogue) as the canonical way to validate a
    // Castagnoli implementation.
    const uint KnownCheckValue = 0xE3069283;
    static readonly byte[] KnownCheckInput = Encoding.ASCII.GetBytes("123456789");

    public class ComputeHash {
        [Fact]
        public void Matches_known_castagnoli_check_value() =>
            Crc32C.ComputeHash(KnownCheckInput).Should().Be(KnownCheckValue);

        [Fact]
        public void Empty_input_is_zero() =>
            Crc32C.ComputeHash(ReadOnlySpan<byte>.Empty).Should().Be(0u);

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(64)]
        [InlineData(1000)]
        public void All_paths_agree_on_random_input(int length) {
            var data = new byte[length];
            new Random(42).NextBytes(data);

            var software = Crc32C.ComputeSoftware(data);

            if (Sse42.IsSupported) {
                var hardware32 = Crc32C.ComputeHardware32(data);
                hardware32.Should().Be(software);
            }

            if (Sse42.X64.IsSupported) {
                var hardwareX64 = Crc32C.ComputeHardwareX64(data);
                hardwareX64.Should().Be(software);
            }

            if (Crc32.Arm64.IsSupported) {
                var hardwareArm64 = Crc32C.ComputeHardwareArm64(data);
                hardwareArm64.Should().Be(software);
            }
        }

        [Fact]
        public void All_paths_agree_on_known_check_value() {
            Crc32C.ComputeSoftware(KnownCheckInput).Should().Be(KnownCheckValue);

            if (Sse42.IsSupported)
                Crc32C.ComputeHardware32(KnownCheckInput).Should().Be(KnownCheckValue);

            if (Sse42.X64.IsSupported)
                Crc32C.ComputeHardwareX64(KnownCheckInput).Should().Be(KnownCheckValue);

            if (Crc32.Arm64.IsSupported)
                Crc32C.ComputeHardwareArm64(KnownCheckInput).Should().Be(KnownCheckValue);
        }
    }
}
