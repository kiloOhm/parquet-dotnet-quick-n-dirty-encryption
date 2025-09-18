using System;
using System.Text;
using Parquet.Bloom;
using Parquet.Meta;
using Encoding = System.Text.Encoding;

namespace Parquet.Bloom {
    /// <summary>
    /// Helper to probe SplitBlock Bloom Filters for equality predicates.
    /// Encodes values per Parquet's PLAIN rules (no length prefix for variable-size types).
    /// </summary>
    internal static class BloomPruning {
        /// <summary>
        /// Returns <c>false</c> if the value is definitely not present in the column chunk,
        /// <c>true</c> if it might be present (or filter is unavailable/unsupported).
        /// </summary>
        public static bool MightMatchEquals(object? value, SchemaElement physical, SplitBlockBloomFilter? bloom) {
            if(value is null)
                return true;                 // blooms don't index nulls; can't rule-in/out nulls
            if(bloom is null)
                return true;                 // no bloom -> can't prune

            switch(physical.Type!.Value) {
                case Parquet.Meta.Type.BOOLEAN:
                    return bloom.MightContain(new[] { ((bool)value) ? (byte)1 : (byte)0 });

                case Parquet.Meta.Type.INT32:
                    return bloom.MightContain(PlainLE.Int32((int)Convert.ChangeType(value, typeof(int))));

                case Parquet.Meta.Type.INT64:
                    return bloom.MightContain(PlainLE.Int64((long)Convert.ChangeType(value, typeof(long))));

                case Parquet.Meta.Type.FLOAT:
                    return bloom.MightContain(PlainLE.Single((float)Convert.ChangeType(value, typeof(float))));

                case Parquet.Meta.Type.DOUBLE:
                    return bloom.MightContain(PlainLE.Double((double)Convert.ChangeType(value, typeof(double))));

                case Parquet.Meta.Type.BYTE_ARRAY: {
                        if(value is byte[] bytes)
                            return bloom.MightContain(bytes);
                        if(value is string s)
                            return bloom.MightContain(Encoding.UTF8.GetBytes(s));
                        throw new NotSupportedException("BYTE_ARRAY equality requires byte[] or string literal.");
                    }

                case Parquet.Meta.Type.FIXED_LEN_BYTE_ARRAY: {
                        if(value is byte[] bytes)
                            return bloom.MightContain(bytes);
                        throw new NotSupportedException("FIXED_LEN_BYTE_ARRAY equality requires byte[] literal.");
                    }

                default:
                    // INT96 / others not wired — don't prune.
                    return true;
            }
        }

        private static class PlainLE {
            public static byte[] Int32(int v) {
                byte[] b = BitConverter.GetBytes(v);
                if(!BitConverter.IsLittleEndian)
                    Array.Reverse(b);
                return b;
            }

            public static byte[] Int64(long v) {
                byte[] b = BitConverter.GetBytes(v);
                if(!BitConverter.IsLittleEndian)
                    Array.Reverse(b);
                return b;
            }

            public static byte[] Single(float v) {
                byte[] b = new byte[4];
                Buffer.BlockCopy(new[] { v }, 0, b, 0, 4);
                if(!BitConverter.IsLittleEndian)
                    Array.Reverse(b);
                return b;
            }

            public static byte[] Double(double v) {
                byte[] b = new byte[8];
                Buffer.BlockCopy(new[] { v }, 0, b, 0, 8);
                if(!BitConverter.IsLittleEndian)
                    Array.Reverse(b);
                return b;
            }
        }
    }
}
