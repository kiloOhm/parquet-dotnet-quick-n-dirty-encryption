using System;
using System.Linq;
using Parquet.Bloom;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Verifies that <see cref="BloomCollector"/> inserts PLAIN-encoded bytes
    /// (without variable-length prefixes) into a <see cref="SplitBlockBloomFilter"/>
    /// for various Parquet physical types.
    /// </summary>
    public sealed class BloomCollectorTests {
        private static byte[] LeBytes(int value) {
            // PLAIN: little-endian 4 bytes
            byte[] buf = BitConverter.GetBytes(value);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(buf);
            return buf;
        }

        private static byte[] LeBytes(long value) {
            // PLAIN: little-endian 8 bytes
            byte[] buf = BitConverter.GetBytes(value);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(buf);
            return buf;
        }

        private static byte[] LeBytes(float value) {
            // PLAIN: IEEE754 single, little-endian 4 bytes
            byte[] buf = new byte[4];
            Buffer.BlockCopy(new[] { value }, 0, buf, 0, 4);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(buf);
            return buf;
        }

        private static byte[] LeBytes(double value) {
            // PLAIN: IEEE754 double, little-endian 8 bytes
            byte[] buf = new byte[8];
            Buffer.BlockCopy(new[] { value }, 0, buf, 0, 8);
            if(!BitConverter.IsLittleEndian)
                Array.Reverse(buf);
            return buf;
        }

        /// <summary>
        /// Confirms that <see cref="BloomCollector.AddInt32"/> inserts the same bytes
        /// as manually PLAIN-encoding a 32-bit integer (little-endian) and calling
        /// <see cref="SplitBlockBloomFilter.Insert(byte[])"/>.
        /// </summary>
        [Fact]
        public void AddInt32_Matches_PlainEncoding() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            int v = 123456789;
            viaCollector.AddInt32(v);
            manual.Insert(LeBytes(v));

            // The same probe should succeed in both filters
            Assert.True(viaCollector.Filter.MightContain(LeBytes(v)));
            Assert.True(manual.MightContain(LeBytes(v)));
        }

        /// <summary>
        /// Confirms that <see cref="BloomCollector.AddInt64"/> uses PLAIN little-endian encoding
        /// (8 bytes) equivalent to manual insertion.
        /// </summary>
        [Fact]
        public void AddInt64_Matches_PlainEncoding() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            long v = 0x1122334455667788L;
            viaCollector.AddInt64(v);
            manual.Insert(LeBytes(v));

            Assert.True(viaCollector.Filter.MightContain(LeBytes(v)));
            Assert.True(manual.MightContain(LeBytes(v)));
        }

        /// <summary>
        /// Confirms that <see cref="BloomCollector.AddFloat"/> inserts IEEE754 single-precision bytes
        /// in little-endian order, matching manual PLAIN encoding.
        /// </summary>
        [Fact]
        public void AddFloat_Matches_PlainEncoding() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            float v = 12345.75f;
            viaCollector.AddFloat(v);
            manual.Insert(LeBytes(v));

            Assert.True(viaCollector.Filter.MightContain(LeBytes(v)));
            Assert.True(manual.MightContain(LeBytes(v)));
        }

        /// <summary>
        /// Confirms that <see cref="BloomCollector.AddDouble"/> inserts IEEE754 double-precision bytes
        /// in little-endian order, matching manual PLAIN encoding.
        /// </summary>
        [Fact]
        public void AddDouble_Matches_PlainEncoding() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            double v = Math.PI * Math.E;
            viaCollector.AddDouble(v);
            manual.Insert(LeBytes(v));

            Assert.True(viaCollector.Filter.MightContain(LeBytes(v)));
            Assert.True(manual.MightContain(LeBytes(v)));
        }

        /// <summary>
        /// Verifies that <see cref="BloomCollector.AddBoolean"/> inserts a single byte (0 or 1),
        /// matching the PLAIN encoding of BOOLEAN.
        /// </summary>
        [Fact]
        public void AddBoolean_Matches_PlainEncoding() {
            int blocks = 32;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            byte[] t = new byte[] { 1 };
            byte[] f = new byte[] { 0 };

            viaCollector.AddBoolean(true);
            viaCollector.AddBoolean(false);

            manual.Insert(t);
            manual.Insert(f);

            Assert.True(viaCollector.Filter.MightContain(t));
            Assert.True(viaCollector.Filter.MightContain(f));
            Assert.True(manual.MightContain(t));
            Assert.True(manual.MightContain(f));
        }

        /// <summary>
        /// Ensures that <see cref="BloomCollector.AddString"/> uses UTF-8 bytes of the string
        /// (without any length prefix), per the Parquet bloom filter spec.
        /// </summary>
        [Fact]
        public void AddString_Uses_Utf8_Bytes_NoLength() {
            int blocks = 64;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            string s = "héłło world 👋";
            byte[] utf8 = Encoding.UTF8.GetBytes(s);

            viaCollector.AddString(s);
            manual.Insert(utf8);

            Assert.True(viaCollector.Filter.MightContain(utf8));
            Assert.True(manual.MightContain(utf8));
        }

        /// <summary>
        /// Ensures that <see cref="BloomCollector.AddByteArray"/> inserts the byte content as-is
        /// (no length prefix), matching manual insertion.
        /// </summary>
        [Fact]
        public void AddByteArray_AsIs_NoLength() {
            int blocks = 32;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            byte[] payload = Enumerable.Range(0, 16).Select(i => (byte)((i * 7) + 3)).ToArray();

            viaCollector.AddByteArray(payload);
            manual.Insert(payload);

            Assert.True(viaCollector.Filter.MightContain(payload));
            Assert.True(manual.MightContain(payload));
        }

        /// <summary>
        /// Verifies that <see cref="BloomCollector.AddFixed"/> inserts bytes as-is, appropriate for
        /// FIXED_LEN_BYTE_ARRAY columns.
        /// </summary>
        [Fact]
        public void AddFixed_AsIs() {
            int blocks = 32;
            var viaCollector = new BloomCollector(blocks);
            var manual = new SplitBlockBloomFilter(blocks);

            byte[] fixedBytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0xAA, 0x55 };

            viaCollector.AddFixed(fixedBytes);
            manual.Insert(fixedBytes);

            Assert.True(viaCollector.Filter.MightContain(fixedBytes));
            Assert.True(manual.MightContain(fixedBytes));
        }

        /// <summary>
        /// Confirms that null inputs are ignored by the collector and do not modify the filter.
        /// Compares the bitset before and after a series of null insertions.
        /// </summary>
        [Fact]
        public void Nulls_Are_Ignored() {
            int blocks = 16;
            var viaCollector = new BloomCollector(blocks);

            byte[] before = viaCollector.Filter.ToByteArray();

            viaCollector.AddBoolean(null);
            viaCollector.AddInt32(null);
            viaCollector.AddInt64(null);
            viaCollector.AddFloat(null);
            viaCollector.AddDouble(null);
            viaCollector.AddString(null);
            viaCollector.AddByteArray(null);
            viaCollector.AddFixed(null);

            byte[] after = viaCollector.Filter.ToByteArray();

            Assert.Equal(before, after);
        }
    }
}
