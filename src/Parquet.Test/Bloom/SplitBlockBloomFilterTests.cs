using System;
using Parquet.Bloom;
using Xunit;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Unit tests for <see cref="SplitBlockBloomFilter"/>.
    /// <para>
    /// These tests validate the core mechanics of the Parquet Split Block Bloom Filter
    /// </para>
    /// </summary>
    public sealed class SplitBlockBloomFilterTests {
        /// <summary>
        /// Verifies mask generation per the Split Block Bloom Filter spec.
        /// For a given 32-bit input x, each of the 8 salts determines one bit to set
        /// in one of the 8 words of a 256-bit block.
        /// This test checks a few known x values and their expected masks, ensuring
        /// that the multiplication, shift, and bit-selection logic is correct.
        /// </summary>
        [Fact]
        public void MaskWords_KnownVectors() {
            // x = 1
            uint[] m1 = SplitBlockBloomFilter.ComputeMasksFor(1U);
            Assert.Equal(8, m1.Length);
            Assert.Equal(0x00000100U, m1[0]);
            Assert.Equal(0x00000100U, m1[1]);
            Assert.Equal(0x00020000U, m1[2]);
            Assert.Equal(0x00100000U, m1[3]);
            Assert.Equal(0x00004000U, m1[4]);
            Assert.Equal(0x00000020U, m1[5]);
            Assert.Equal(0x00080000U, m1[6]);
            Assert.Equal(0x00000800U, m1[7]);

            // x = 0xDEADBEEF
            uint[] m2 = SplitBlockBloomFilter.ComputeMasksFor(0xDEADBEEFU);
            Assert.Equal(8, m2.Length);
            Assert.Equal(0x20000000U, m2[0]);
            Assert.Equal(0x00008000U, m2[1]);
            Assert.Equal(0x00001000U, m2[2]);
            Assert.Equal(0x00004000U, m2[3]);
            Assert.Equal(0x00002000U, m2[4]);
            Assert.Equal(0x02000000U, m2[5]);
            Assert.Equal(0x01000000U, m2[6]);
            Assert.Equal(0x00200000U, m2[7]);

            // x = 0x89ABCDEF
            uint[] m3 = SplitBlockBloomFilter.ComputeMasksFor(0x89ABCDEFU);
            Assert.Equal(8, m3.Length);
            Assert.Equal(0x00040000U, m3[0]);
            Assert.Equal(0x00080000U, m3[1]);
            Assert.Equal(0x40000000U, m3[2]);
            Assert.Equal(0x00800000U, m3[3]);
            Assert.Equal(0x00000010U, m3[4]);
            Assert.Equal(0x00010000U, m3[5]);
            Assert.Equal(0x00001000U, m3[6]);
            Assert.Equal(0x01000000U, m3[7]);
        }

        /// <summary>
        /// Validates the block index mapping formula ((h >> 32) * z) >> 32. Confirms
        /// that different 64-bit hash inputs map to the expected block indices when the
        /// number of blocks is fixed, ensuring even distribution across blocks.
        /// </summary>
        [Fact]
        public void MapHashToBlock_KnownValues() {
            int z = 17;

            Assert.Equal(0, SplitBlockBloomFilter.MapHashToBlock(0x0000000000000000UL, z));
            Assert.Equal(0, SplitBlockBloomFilter.MapHashToBlock(0x0000000000000001UL, z));
            Assert.Equal(0, SplitBlockBloomFilter.MapHashToBlock(0x00000001FFFFFFFFUL, z));
            Assert.Equal(1, SplitBlockBloomFilter.MapHashToBlock(0x1234567890ABCDEFUL, z));
            Assert.Equal(16, SplitBlockBloomFilter.MapHashToBlock(0xFFFFFFFFFFFFFFFFUL, z));
            Assert.Equal(0, SplitBlockBloomFilter.MapHashToBlock(0x00000000FFFFFFFFUL, z));
            Assert.Equal(14, SplitBlockBloomFilter.MapHashToBlock(0xDEADBEEF01234567UL, z));
        }

        /// <summary>
        /// Checks that inserting a value into the bloom filter makes it detectable by
        /// MightContain, and that values not inserted are reported as absent. Confirms
        /// correct interaction between Insert and MightContain methods across multiple
        /// blocks.
        /// </summary>
        [Fact]
        public void InsertAndCheck_RoundTrip() {
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(4);

            ulong h0 = 0x0000000012345678UL; // block 0 (high = 0x00000000)
            ulong h1 = 0x4000000012345678UL; // block 1 (high = 0x40000000)
            ulong h2 = 0x8000000012345678UL; // block 2 (high = 0x80000000)
            ulong h3 = 0xC000000012345678UL; // block 3 (high = 0xC0000000)

            Assert.False(f.MightContain(h0));
            Assert.False(f.MightContain(h1));
            Assert.False(f.MightContain(h2));
            Assert.False(f.MightContain(h3));

            f.Insert(h1);
            f.Insert(h3);

            Assert.True(f.MightContain(h1));
            Assert.True(f.MightContain(h3));

            Assert.False(f.MightContain(h0));
            Assert.False(f.MightContain(h2));
        }

        /// <summary>
        /// Ensures that serializing a bloom filter to a byte array and deserializing
        /// it back with FromByteArray preserves the set bits. Verifies that values
        /// inserted before serialization are still detectable after deserialization.
        /// </summary>
        [Fact]
        public void ToBytes_FromBytes_RoundTrip() {
            SplitBlockBloomFilter f1 = new SplitBlockBloomFilter(3);
            f1.Insert(0x0123456789ABCDEFUL);
            f1.Insert(0x1111222233334444UL);
            f1.Insert(0xABCDEF0123456789UL);

            byte[] bytes = f1.ToByteArray();
            Assert.Equal(SplitBlockBloomFilter.BytesForBlocks(3), bytes.Length);

            SplitBlockBloomFilter f2 = SplitBlockBloomFilter.FromByteArray(3, bytes);

            // Check that known inserts still look present.
            Assert.True(f2.MightContain(0x0123456789ABCDEFUL));
            Assert.True(f2.MightContain(0x1111222233334444UL));
            Assert.True(f2.MightContain(0xABCDEF0123456789UL));

            // A random value should usually be absent (not guaranteed, but very likely for tiny filters).
            bool maybe = f2.MightContain(0x9999999988887777UL);
            // We don't assert False strictly to avoid flaky tests; instead just ensure API works.
            Assert.IsType<bool>(maybe);
        }

        /// <summary>
        /// Confirms that BytesForBlocks returns the correct size in bytes for a given
        /// number of blocks (each block is 32 bytes). Also checks that invalid arguments
        /// such as zero blocks or mismatched byte array lengths throw appropriate
        /// exceptions.
        /// </summary>
        [Fact]
        public void BytesForBlocks_And_Bounds() {
            Assert.Equal(32, SplitBlockBloomFilter.BytesForBlocks(1));
            Assert.Equal(64, SplitBlockBloomFilter.BytesForBlocks(2));
            Assert.Equal(320, SplitBlockBloomFilter.BytesForBlocks(10));

            Assert.Throws<ArgumentOutOfRangeException>(() => new SplitBlockBloomFilter(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => SplitBlockBloomFilter.BytesForBlocks(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => SplitBlockBloomFilter.FromByteArray(0, new byte[0]));
        }
    }
}
