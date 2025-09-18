using System;
using Parquet.Bloom;
using Parquet.Meta;
using Xunit;
using Type = Parquet.Meta.Type;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Tests for reader-side pruning using Bloom filters with equality predicates.
    /// Validates that values present in the writer-built filter return true,
    /// and non-present values can be ruled out.
    /// </summary>
    public sealed class BloomPruningTests {
        private static SchemaElement Physical(Type t) => new SchemaElement { Type = t };

        /// <summary>
        /// INT32: values present -> MightMatchEquals returns true; absent value -> returns false.
        /// </summary>
        [Fact]
        public void Int32_Equals_Prunes_Absent() {
            var bloom = new BloomCollector(32);
            bloom.AddInt32(7);
            bloom.AddInt32(42);

            SchemaElement se = Physical(Parquet.Meta.Type.INT32);

            Assert.True(BloomPruning.MightMatchEquals(7, se, bloom.Filter));
            Assert.True(BloomPruning.MightMatchEquals(42, se, bloom.Filter));

            // Very unlikely false positive with 32 blocks; acceptable for unit test purpose
            Assert.False(BloomPruning.MightMatchEquals(123456, se, bloom.Filter));
        }

        /// <summary>
        /// BYTE_ARRAY (UTF-8 string): ensures PLAIN (no length) UTF-8 encoding is used for probes.
        /// </summary>
        [Fact]
        public void ByteArray_String_Equals_Prunes_Absent() {
            var bloom = new BloomCollector(32);
            bloom.AddString("parquet");
            bloom.AddString("bloom");

            SchemaElement se = Physical(Parquet.Meta.Type.BYTE_ARRAY);

            Assert.True(BloomPruning.MightMatchEquals("parquet", se, bloom.Filter));
            Assert.True(BloomPruning.MightMatchEquals("bloom", se, bloom.Filter));
            Assert.False(BloomPruning.MightMatchEquals("not-here", se, bloom.Filter));
        }

        /// <summary>
        /// DOUBLE: confirms IEEE-754 little-endian encoding matches writer-side collector.
        /// </summary>
        [Fact]
        public void Double_Equals_Prunes_Absent() {
            var bloom = new BloomCollector(32);
            bloom.AddDouble(Math.PI);
            bloom.AddDouble(123.5);

            SchemaElement se = Physical(Parquet.Meta.Type.DOUBLE);

            Assert.True(BloomPruning.MightMatchEquals(Math.PI, se, bloom.Filter));
            Assert.True(BloomPruning.MightMatchEquals(123.5, se, bloom.Filter));
            Assert.False(BloomPruning.MightMatchEquals(999.0, se, bloom.Filter));
        }

        /// <summary>
        /// FIXED_LEN_BYTE_ARRAY: probes raw bytes as-is (no length prefix).
        /// </summary>
        [Fact]
        public void FixedLenByteArray_Equals_Prunes_Absent() {
            var bloom = new BloomCollector(32);
            byte[] present = new byte[] { 0, 1, 2, 3, 4, 5 };
            byte[] absent = new byte[] { 5, 4, 3, 2, 1, 0 };
            bloom.AddFixed(present);

            SchemaElement se = Physical(Parquet.Meta.Type.FIXED_LEN_BYTE_ARRAY);

            Assert.True(BloomPruning.MightMatchEquals(present, se, bloom.Filter));
            Assert.False(BloomPruning.MightMatchEquals(absent, se, bloom.Filter));
        }

        /// <summary>
        /// Nulls are not indexed by blooms; helper conservatively returns true (no pruning).
        /// </summary>
        [Fact]
        public void Null_Literal_Does_Not_Prune() {
            var bloom = new BloomCollector(16);
            bloom.AddInt64(10);

            SchemaElement se = Physical(Parquet.Meta.Type.INT64);

            Assert.True(BloomPruning.MightMatchEquals(null, se, bloom.Filter));
        }

        /// <summary>
        /// If bloom is missing or algorithm unsupported, helper returns true (no pruning).
        /// </summary>
        [Fact]
        public void Missing_Bloom_Does_Not_Prune() {
            SchemaElement se = Physical(Parquet.Meta.Type.INT32);
            Assert.True(BloomPruning.MightMatchEquals(1, se, null));
        }
    }
}
