using System;
using System.Buffers;

namespace Parquet.File {
    internal readonly struct RentedBytes : IDisposable {
        private readonly ArrayPool<byte>? _pool;

        public RentedBytes(byte[] buffer, int length, ArrayPool<byte>? pool) {
            Buffer = buffer;
            Length = length;
            _pool = pool;
        }

        public byte[] Buffer { get; }
        public int Length { get; }

        public Span<byte> AsSpan() {
            return new Span<byte>(Buffer, 0, Length);
        }

        public void Dispose() {
            if(_pool != null && Buffer != null) {
                _pool.Return(Buffer);
            }
        }
    }
}
