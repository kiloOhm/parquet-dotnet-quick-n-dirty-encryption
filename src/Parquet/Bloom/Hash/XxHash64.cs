using System;

namespace Parquet.Bloom.Hash
{
    /// <summary>
    /// XXH64 hashing (seeded), compatible with xxHash v0.6+ reference.
    /// Used by Parquet Bloom Filters to hash Plain-encoded values with seed = 0.
    /// </summary>
    public sealed class XxHash64
    {
        // Primes from xxhash reference
        private const ulong PRIME64_1 = 11400714785074694791UL; // 0x9E3779B185EBCA87
        private const ulong PRIME64_2 = 14029467366897019727UL; // 0xC2B2AE3D27D4EB4F
        private const ulong PRIME64_3 = 1609587929392839161UL;  // 0x165667B19E3779F9
        private const ulong PRIME64_4 = 9650029242287828579UL;  // 0x85EBCA77C2B2AE63
        private const ulong PRIME64_5 = 2870177450012600261UL;  // 0x27D4EB2F165667C5

        private ulong _seed;
        private ulong _totalLen;

        // Accumulators
        private ulong _v1;
        private ulong _v2;
        private ulong _v3;
        private ulong _v4;

        // Tail buffer for <32B chunks
        private readonly byte[] _mem;
        private int _memSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="XxHash64"/> class with a seed value of 0.
        /// </summary>
        public XxHash64()
            : this(0UL)
        {
        }

        
        /// <summary>
        /// Initializes a new instance of the <see cref="XxHash64"/> class with the specified seed value.
        /// </summary>
        /// <param name="seed">The seed value to initialize the hash function.</param>
        public XxHash64(ulong seed) {
            this._mem = new byte[32];
            this.Reset(seed);
        }

        /// <summary>
        /// Resets the hasher with a new seed (default 0).
        /// </summary>
        public void Reset(ulong seed)
        {
            this._seed = seed;
            this._totalLen = 0UL;
            this._memSize = 0;

            this._v1 = seed + PRIME64_1 + PRIME64_2;
            this._v2 = seed + PRIME64_2;
            this._v3 = seed + 0UL;
            this._v4 = seed - PRIME64_1;
        }

        /// <summary>
        /// Update the hash with a byte array segment.
        /// </summary>
        public void Update(byte[] data, int offset, int length)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }
            if (offset < 0 || length < 0 || offset + length > data.Length)
            {
                throw new ArgumentOutOfRangeException("offset/length");
            }

            this._totalLen += (ulong)length;
            int idx = offset;
            int end = offset + length;

            // If we have buffered bytes + input enough to reach 32
            if (this._memSize + length < 32)
            {
                while (idx < end)
                {
                    this._mem[this._memSize++] = data[idx++];
                }
                return;
            }

            // If there is some left-over from previous update, fill to 32 and consume
            if (this._memSize > 0)
            {
                int fill = 32 - this._memSize;
                Buffer.BlockCopy(data, idx, this._mem, this._memSize, fill);
                idx += fill;

                this.ProcessChunk(this._mem, 0);
                this._memSize = 0;
            }

            // Consume 32-byte chunks directly from input
            while (idx + 32 <= end)
            {
                this.ProcessChunk(data, idx);
                idx += 32;
            }

            // Buffer remaining tail
            while (idx < end)
            {
                this._mem[this._memSize++] = data[idx++];
            }
        }

        /// <summary>
        /// Compute the 64-bit hash for already provided data.
        /// </summary>
        public ulong Digest()
        {
            ulong acc;
            if (this._totalLen >= 32UL)
            {
                acc =
                    RotateLeft(this._v1, 1) +
                    RotateLeft(this._v2, 7) +
                    RotateLeft(this._v3, 12) +
                    RotateLeft(this._v4, 18);

                acc = MergeRound(acc, this._v1);
                acc = MergeRound(acc, this._v2);
                acc = MergeRound(acc, this._v3);
                acc = MergeRound(acc, this._v4);
            }
            else
            {
                acc = this._seed + PRIME64_5;
            }

            acc += this._totalLen;

            int idx = 0;

            // Process 8-byte lanes from tail
            while (idx + 8 <= this._memSize)
            {
                ulong k1 = ReadUInt64LE(this._mem, idx);
                k1 *= PRIME64_2;
                k1 = RotateLeft(k1, 31);
                k1 *= PRIME64_1;
                acc ^= k1;
                acc = (RotateLeft(acc, 27) * PRIME64_1) + PRIME64_4;
                idx += 8;
            }

            // Process 4-byte lane from tail
            if (idx + 4 <= this._memSize)
            {
                acc ^= (ulong)ReadUInt32LE(this._mem, idx) * PRIME64_1;
                acc = (RotateLeft(acc, 23) * PRIME64_2) + PRIME64_3;
                idx += 4;
            }

            // Process remaining bytes
            while (idx < this._memSize)
            {
                acc ^= (ulong)this._mem[idx] * PRIME64_5;
                acc = RotateLeft(acc, 11) * PRIME64_1;
                idx++;
            }

            // Avalanche
            acc ^= acc >> 33;
            acc *= PRIME64_2;
            acc ^= acc >> 29;
            acc *= PRIME64_3;
            acc ^= acc >> 32;

            return acc;
        }

        /// <summary>
        /// Convenience one-shot API: compute XXH64(data, seed).
        /// </summary>
        public static ulong Compute(byte[] data, int offset, int length, ulong seed)
        {
            XxHash64 h = new XxHash64(seed);
            h.Update(data, offset, length);
            return h.Digest();
        }

        /// <summary>
        /// Convenience one-shot API: compute XXH64(data, seed=0).
        /// </summary>
        public static ulong Compute(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }
            return XxHash64.Compute(data, 0, data.Length, 0UL);
        }

        // --- Internals ---

        private void ProcessChunk(byte[] data, int offset)
        {
            ulong d1 = ReadUInt64LE(data, offset + 0);
            ulong d2 = ReadUInt64LE(data, offset + 8);
            ulong d3 = ReadUInt64LE(data, offset + 16);
            ulong d4 = ReadUInt64LE(data, offset + 24);

            this._v1 = Round(this._v1, d1);
            this._v2 = Round(this._v2, d2);
            this._v3 = Round(this._v3, d3);
            this._v4 = Round(this._v4, d4);
        }

        private static ulong Round(ulong acc, ulong input)
        {
            acc += input * PRIME64_2;
            acc = RotateLeft(acc, 31);
            acc *= PRIME64_1;
            return acc;
        }

        private static ulong MergeRound(ulong acc, ulong val)
        {
            val = Round(0UL, val);
            acc ^= val;
            acc = (acc * PRIME64_1) + PRIME64_4;
            return acc;
        }

        private static ulong RotateLeft(ulong x, int r)
        {
            return (x << r) | (x >> (64 - r));
        }

        private static ulong ReadUInt64LE(byte[] data, int offset)
        {
            // Manual LE read to avoid BitConverter.Endian branches
            uint lo = (uint)(data[offset + 0]
                            | (data[offset + 1] << 8)
                            | (data[offset + 2] << 16)
                            | (data[offset + 3] << 24));
            uint hi = (uint)(data[offset + 4]
                            | (data[offset + 5] << 8)
                            | (data[offset + 6] << 16)
                            | (data[offset + 7] << 24));
            return ((ulong)hi << 32) | (ulong)lo;
        }

        private static uint ReadUInt32LE(byte[] data, int offset)
        {
            return (uint)(data[offset + 0]
                        | (data[offset + 1] << 8)
                        | (data[offset + 2] << 16)
                        | (data[offset + 3] << 24));
        }
    }
}
