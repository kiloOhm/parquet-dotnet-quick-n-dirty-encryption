using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;

namespace Parquet.File {
    // Keep using your existing CompressionMethod enum.
    // Only Gzip is supported now; others will throw.

    static class Compressor {
        // Compress to GZip. Returns the compressed bytes.
        public static byte[] Compress(CompressionMethod method, ReadOnlySpan<byte> input, CompressionLevel compressionLevel) {
            if(method != CompressionMethod.Gzip) {
                throw new NotSupportedException("Only GZip compression is supported without IronCompress.");
            }

            // NOTE: .NET 4.7.2 does not have GZipStream.Write(Span<byte>) so we copy to byte[]
            byte[] inputArray = input.ToArray();

            using(MemoryStream ms = new MemoryStream()) {
                using(GZipStream gzip = new GZipStream(ms, compressionLevel, leaveOpen: true)) {
                    gzip.Write(inputArray, 0, inputArray.Length);
                }
                return ms.ToArray();
            }
        }

        // Decompress from GZip. outLength is optional; if <= 0, we stream to a growing buffer.
        public static byte[] Decompress(CompressionMethod method, ReadOnlySpan<byte> input, int outLength) {
            if(method != CompressionMethod.Gzip) {
                throw new NotSupportedException("Only GZip is supported (IronCompress removed).");
            }

            // NOTE: .NET 4.7.2 lacks Span-based Stream.Write/Read, so copy to byte[].
            byte[] inputArray = input.ToArray();

            using(MemoryStream source = new MemoryStream(inputArray, writable: false))
            using(GZipStream gzip = new GZipStream(source, CompressionMode.Decompress, leaveOpen: false)) {
                if(outLength > 0) {
                    byte[] exact = new byte[outLength];
                    int total = 0;
                    while(total < outLength) {
                        int read = gzip.Read(exact, total, outLength - total);
                        if(read == 0) {
                            break;
                        }
                        total += read;
                    }

                    if(total != outLength) {
                        Array.Resize(ref exact, total);
                    }
                    return exact;
                } else {
                    using(MemoryStream sink = new MemoryStream()) {
                        gzip.CopyTo(sink);
                        return sink.ToArray();
                    }
                }
            }
        }
    }
}
