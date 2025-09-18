using System;
using System.Text;
using Parquet.Bloom;
using Parquet.Bloom.Hash;
using Xunit;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Tests that wire XXH64(seed=0) hashing into the Split-Block Bloom Filter's
    /// byte-based Insert/MightContain APIs.
    /// </summary>
    public sealed class SplitBlockBloomFilter_HashingTests {
        /// <summary>
        /// Confirms that inserting byte payloads makes them detectable via
        /// MightContain(byte[]), and that unrelated payloads are typically absent.
        /// Uses a relatively large filter to keep false positives vanishingly low.
        /// </summary>
        [Fact]
        public void Bytes_Insert_And_Query_RoundTrip() {
            // Large filter: 1024 blocks = 32 KB bitset (keeps FP rate negligible for a few inserts)
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(1024);

            byte[] a = Encoding.ASCII.GetBytes("alpha");
            byte[] b = Encoding.ASCII.GetBytes("bravo");
            byte[] c = Encoding.ASCII.GetBytes("charlie");

            Assert.False(f.MightContain(a));
            Assert.False(f.MightContain(b));
            Assert.False(f.MightContain(c));

            f.Insert(a);
            f.Insert(c);

            Assert.True(f.MightContain(a));
            Assert.True(f.MightContain(c));
            Assert.False(f.MightContain(b)); // Extremely unlikely to be true with this sizing
        }

        /// <summary>
        /// Verifies that Insert(byte[],offset,len) and MightContain(byte[],offset,len)
        /// operate on the specified slice and match the behavior of hashing that slice
        /// directly with XXH64(seed=0).
        /// </summary>
        [Fact]
        public void Slice_Insert_And_Query_Match_Hash() {
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(64);

            byte[] buf = Encoding.ASCII.GetBytes("....HelloWorld....");
            int off = 4;
            int len = 10; // "HelloWorld"

            // Manual path: compute hash and use Insert(ulong)
            ulong h = XxHash64.Compute(buf, off, len, 0UL);
            f.Insert(h);

            // Slice-based query should match
            Assert.True(f.MightContain(buf, off, len));

            // Adjacent slice should not match (different bytes)
            Assert.False(f.MightContain(buf, off - 1, len)); // ".HelloWorld"
            Assert.False(f.MightContain(buf, off, len - 1)); // "HelloWorl"
        }

        /// <summary>
        /// Ensures that the byte-based APIs throw for null arrays and invalid slices.
        /// </summary>
        [Fact]
        public void ByteApis_ArgumentChecks() {
            SplitBlockBloomFilter f = new SplitBlockBloomFilter(8);

#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type. This is intentional to test argument checking.
            Assert.Throws<ArgumentNullException>(() => f.Insert(null));
            Assert.Throws<ArgumentNullException>(() => f.MightContain(null));
#pragma warning restore CS8625

            byte[] data = new byte[4] { 1, 2, 3, 4 };

            Assert.Throws<ArgumentOutOfRangeException>(() => f.Insert(data, -1, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => f.Insert(data, 0, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => f.Insert(data, 3, 2));

            Assert.Throws<ArgumentOutOfRangeException>(() => f.MightContain(data, -1, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => f.MightContain(data, 0, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => f.MightContain(data, 4, 1));
        }

        /// <summary>
        /// Confirms that the byte-based Insert path is equivalent to computing
        /// XXH64(seed=0) externally and calling Insert(ulong).
        /// </summary>
        [Fact]
        public void InsertBytes_Equals_InsertHash() {
            SplitBlockBloomFilter f1 = new SplitBlockBloomFilter(32);
            SplitBlockBloomFilter f2 = new SplitBlockBloomFilter(32);

            byte[] msg = Encoding.ASCII.GetBytes("Parity check between Insert(bytes) and Insert(hash)");

            // Path A: Insert using bytes
            f1.Insert(msg);

            // Path B: Compute hash and insert via ulong API
            ulong h = XxHash64.Compute(msg, 0, msg.Length, 0UL);
            f2.Insert(h);

            // The same query should succeed against both
            Assert.True(f1.MightContain(msg));
            Assert.True(f2.MightContain(msg));
        }
    }
}
