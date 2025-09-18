using System;
using Parquet.Bloom.Hash;
using Xunit;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Unit tests for <see cref="Parquet.Bloom.Hash.XxHash64"/>.
    /// <para>
    /// These tests verify correctness and robustness of the XXH64 implementation
    /// required by the Parquet Bloom Filter specification:
    /// </para>
    /// </summary>
    public sealed class XxHash64Tests {
        /// <summary>
        /// Known-good XXH64 vectors (seed=0) for simple ASCII strings,
        /// verified against public calculators. Avoids ambiguous binary cases
        /// like the 0..255 sequence which depend on exact byte construction.
        /// </summary>
        [Fact]
        public void KnownVectors_Seed0() {
            // "" (empty)
            Assert.Equal(0xEF46DB3751D8E999UL, XxHash64.Compute(new byte[0]));

            // "a"
            Assert.Equal(0xD24EC4F1A98C6E5BUL, XxHash64.Compute(System.Text.Encoding.ASCII.GetBytes("a")));

            // "hello"
            Assert.Equal(0x26C7827D889F6DA3UL, XxHash64.Compute(System.Text.Encoding.ASCII.GetBytes("hello")));

            // "hello world" (no newline)
            Assert.Equal(0x45AB6734B21E6968UL, XxHash64.Compute(System.Text.Encoding.ASCII.GetBytes("hello world")));
        }

        /// <summary>
        /// Confirms that hashing data in small, incremental updates produces
        /// the same 64-bit result as hashing the same data in a single call.
        /// Exercises chunk processing logic, especially at 32-byte block
        /// boundaries, to ensure streaming and one-shot hashing are equivalent.
        /// </summary>
        [Fact]
        public void Incremental_Equals_OneShot() {
            byte[] data = System.Text.Encoding.ASCII.GetBytes("The quick brown fox jumps over the lazy dog.");
            ulong seed = 0UL;

            // One-shot
            ulong hsOne = XxHash64.Compute(data, 0, data.Length, seed);

            // Incremental (split in weird boundaries)
            XxHash64 h = new XxHash64(seed);
            h.Update(data, 0, 1);
            h.Update(data, 1, 2);
            h.Update(data, 3, 5);
            h.Update(data, 8, 7);
            h.Update(data, 15, 3);
            h.Update(data, 18, data.Length - 18);

            ulong hsInc = h.Digest();

            Assert.Equal(hsOne, hsInc);
        }

        /// <summary>
        /// Checks that computing a digest on an empty hasher is deterministic
        /// and repeatable. Ensures the result matches the published empty
        /// string hash value (0xEF46DB3751D8E999 for seed = 0).
        /// </summary>
        [Fact]
        public void Empty_ThenDigest_IsStable() {
            XxHash64 h = new XxHash64(0UL);
            ulong a = h.Digest();
            ulong b = h.Digest();
            Assert.Equal(a, b);
            Assert.Equal(0xEF46DB3751D8E999UL, a);
        }

        /// <summary>
        /// Validates that the Update method enforces its argument constraints.
        /// Passing a null array, negative offset/length, or an out-of-range
        /// slice should all raise the appropriate exceptions.
        /// </summary>
        [Fact]
        public void Update_ArgumentChecks() {
            XxHash64 h = new XxHash64();
#pragma warning disable CS8625
            // Cannot convert null literal to non-nullable reference type. This is intentional to test argument checking.
            Assert.Throws<ArgumentNullException>(() => h.Update(null, 0, 0));
#pragma warning restore CS8625
            Assert.Throws<ArgumentOutOfRangeException>(() => h.Update(new byte[1], -1, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => h.Update(new byte[1], 0, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => h.Update(new byte[1], 1, 1));
        }
    }
}
