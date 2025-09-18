using System;
using Parquet.Bloom;
using Xunit;

namespace Parquet.Test.Bloom {
    /// <summary>
    /// Tests for sizing Split-Block Bloom Filters from either target FPP or explicit BPV.
    /// <para>
    /// FPP = false-positive probability
    /// </para>
    /// <para>
    /// BPV = bits per value
    /// </para>
    /// </summary>
    public sealed class BloomSizingTests {
        /// <summary>
        /// Verifies that defaulting (no FPP, no BPV) yields ~1% configuration
        /// with the correct blocks/bytes math.
        /// </summary>
        [Fact]
        public void Default_Is_Approx_1pct() {
            long n = 1000L; // 1000 distinct values
            BloomSizing.BloomPlan plan = BloomSizing.Plan(n, null, null);

            // BPV ~ 10.5 => total bits = ceil(10.5 * 1000) = 10,500
            // blocks = ceil(10,500 / 256) = 42  (since 41 * 256 = 10,496 < 10,500)
            Assert.Equal(42, plan.Blocks);
            Assert.Equal(42 * 32, plan.NumBytes); // 1,344 bytes
            Assert.True(plan.BitsPerValue >= 10.5 - 1e-9 && plan.BitsPerValue <= 10.5 + 1e-9);
        }

        /// <summary>
        /// Ensures target FPP maps to conservative bits-per-value thresholds.
        /// </summary>
        [Fact]
        public void Fpp_To_Bpv_Thresholds() {
            Assert.Equal(6.0, BloomSizing.BitsPerValueForFpp(0.10), 6);
            Assert.Equal(10.5, BloomSizing.BitsPerValueForFpp(0.01), 6);
            Assert.Equal(16.9, BloomSizing.BitsPerValueForFpp(0.001), 6);
            Assert.Equal(26.4, BloomSizing.BitsPerValueForFpp(0.0001), 6);
            Assert.Equal(41.0, BloomSizing.BitsPerValueForFpp(0.00001), 6);
        }

        /// <summary>
        /// Confirms that explicit bits-per-value override is respected and block math is correct.
        /// </summary>
        [Fact]
        public void BitsPerValue_Override_Wins() {
            long n = 10_000L;
            double bpv = 8.0; // explicit override

            BloomSizing.BloomPlan plan = BloomSizing.Plan(n, 0.001, bpv);

            // total bits = ceil(80000) => 80000
            // blocks = ceil(80000/256) = 313
            Assert.Equal(313, plan.Blocks);
            Assert.Equal(313 * 32, plan.NumBytes);
            Assert.Equal(bpv, plan.BitsPerValue, 10);
            Assert.Equal(n, plan.EstimatedDistinctValues);
        }

        /// <summary>
        /// Verifies that empty inputs still produce a well-formed filter (z >= 1).
        /// </summary>
        [Fact]
        public void Zero_Values_Still_Allocates_One_Block() {
            BloomSizing.BloomPlan plan = BloomSizing.Plan(0, 0.01, null);
            Assert.Equal(1, plan.Blocks);
            Assert.Equal(32, plan.NumBytes);
        }

        /// <summary>
        /// Ensures input validation throws for out-of-range parameters.
        /// </summary>
        [Fact]
        public void Argument_Validation() {
            Assert.Throws<ArgumentOutOfRangeException>(() => BloomSizing.Plan(-1, null, null));
            Assert.Throws<ArgumentOutOfRangeException>(() => BloomSizing.Plan(1, -0.5, null));
            Assert.Throws<ArgumentOutOfRangeException>(() => BloomSizing.Plan(1, 1.0, null));
            Assert.Throws<ArgumentOutOfRangeException>(() => BloomSizing.Plan(1, null, 0.0));
        }
    }
}
