using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;

namespace Parquet.File {

    /// <summary>
    /// Reader for Parquet data column
    /// </summary>
    class DataColumnReader {
        private readonly DataField _dataField;
        private readonly Stream _inputStream;
        private readonly ColumnChunk _thriftColumnChunk;
        private readonly SchemaElement? _schemaElement;
        private readonly ThriftFooter _footer;
        private readonly ParquetOptions _options;
        private readonly DataColumnStatistics? _stats;

        internal DataColumnReader(
           DataField dataField,
           Stream inputStream,
           ColumnChunk thriftColumnChunk,
           DataColumnStatistics? stats,
           ThriftFooter footer,
           ParquetOptions? parquetOptions) {
            _dataField = dataField ?? throw new ArgumentNullException(nameof(dataField));
            _inputStream = inputStream ?? throw new ArgumentNullException(nameof(inputStream));
            _thriftColumnChunk = thriftColumnChunk ?? throw new ArgumentNullException(nameof(thriftColumnChunk));
            _stats = stats;
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            dataField.EnsureAttachedToSchema(nameof(dataField));

            _schemaElement = _footer.GetSchemaElement(_thriftColumnChunk);
        }

        /// <summary>
        /// Return data column statistics
        /// </summary>
        /// <returns>Data column statistics or null</returns>
        public DataColumnStatistics? GetColumnStatistics() => _stats;

        /// <summary>
        /// Read entire column data
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>DataColumn object filled in with data</returns>
        /// <exception cref="NotSupportedException">Unsupported page type</exception>
        public async Task<DataColumn> ReadAsync(CancellationToken cancellationToken = default) {

            // how many values are in column chunk, as there may be multiple data pages
            int totalValuesInChunk = (int)_thriftColumnChunk.MetaData!.NumValues;
            int definedValuesCount = totalValuesInChunk;
            if(_stats?.NullCount != null)
                definedValuesCount -= (int)_stats.NullCount.Value;
            using var pc = new PackedColumn(_dataField, totalValuesInChunk, definedValuesCount);
            long fileOffset = GetFileOffset();
            _inputStream.Seek(fileOffset, SeekOrigin.Begin);

            while(pc.ValuesRead < totalValuesInChunk) {
                PageHeader ph = PageHeader.Read(new ThriftCompactProtocolReader(_inputStream));

                switch(ph.Type) {
                    case PageType.DICTIONARY_PAGE:
                        await ReadDictionaryPage(ph, pc);
                        break;
                    case PageType.DATA_PAGE:
                        await ReadDataPageV1Async(ph, pc);
                        break;
                    case PageType.DATA_PAGE_V2:
                        await ReadDataPageV2Async(ph, pc, totalValuesInChunk);
                        break;
                    default:
                        throw new NotSupportedException($"can't read page type {ph.Type}");
                }
            }

            // all the data is available here!
            DataColumn column = pc.Unpack();
            if(_stats != null)
                column.Statistics = _stats;
            return column;
        }

        private async Task<RentedBytes> ReadPageDataAsync(PageHeader ph) {
            byte[] data = ArrayPool<byte>.Shared.Rent(ph.CompressedPageSize);

            int totalBytesRead = 0;
            int remainingBytes = ph.CompressedPageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            byte[]? iv = _options.AES_IV_BYTES;
            byte[]? key = _options.ENC_KEY_BYTES;

            if(iv != null && key != null) {
                // decrypt
                Encryptor.AES_CTR_inPlace(data.AsSpan(0, ph.CompressedPageSize), key, iv);
            }

            if(_thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED) {
                // Just pass through the rented buffer; caller will Dispose() to return it.
                return new RentedBytes(data, ph.CompressedPageSize, ArrayPool<byte>.Shared);
            }

            // GZip-only decompression
            CompressionMethod method = (CompressionMethod)(int)_thriftColumnChunk.MetaData.Codec;
            if(method != CompressionMethod.Gzip) {
                // Return rented first, then throw to avoid leaking the buffer.
                ArrayPool<byte>.Shared.Return(data);
                throw new NotSupportedException($"Compression codec '{_thriftColumnChunk.MetaData.Codec}' is not supported. Only GZip is supported.");
            }

            byte[] decompressed = Compressor.Decompress(method, data.AsSpan(0, ph.CompressedPageSize), ph.UncompressedPageSize);

            // Return the rented compressed buffer now that we’re done with it.
            ArrayPool<byte>.Shared.Return(data);

            // Hand back decompressed bytes (not rented).
            return new RentedBytes(decompressed, decompressed.Length, null);
        }

        private async Task<RentedBytes> ReadPageDataV2Async(PageHeader ph) {
            int pageSize = ph.CompressedPageSize;

            byte[] data = ArrayPool<byte>.Shared.Rent(pageSize);

            int totalBytesRead = 0;
            int remainingBytes = pageSize;
            do {
                int bytesRead = await _inputStream.ReadAsync(data, totalBytesRead, remainingBytes);
                totalBytesRead += bytesRead;
                remainingBytes -= bytesRead;
            }
            while(remainingBytes != 0);

            byte[]? iv = _options.AES_IV_BYTES;
            byte[]? key = _options.ENC_KEY_BYTES;

            if(iv != null && key != null) {
                // decrypt
                Encryptor.AES_CTR_inPlace(data.AsSpan(0, pageSize), key, iv);
            }

            // V2 helper just returns the raw (possibly still-compressed) bytes;
            // callers decide whether to decompress based on the header flags.
            return new RentedBytes(data, pageSize, ArrayPool<byte>.Shared);
        }

        private async ValueTask ReadDictionaryPage(PageHeader ph, PackedColumn pc) {

            if(pc.HasDictionary)
                throw new InvalidOperationException("dictionary already read");

            //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.
            using(RentedBytes bytes = await ReadPageDataAsync(ph)) {

                Array dictionary = _dataField.CreateArray(ph.DictionaryPageHeader!.NumValues);

                ParquetPlainEncoder.Decode(
                    dictionary, 0, ph.DictionaryPageHeader.NumValues,
                    _schemaElement!, bytes.AsSpan(), out int dictionaryOffset);

                pc.AssignDictionary(dictionary);
            }
        }

        private long GetFileOffset() =>
            // get the minimum offset, we'll just read pages in sequence as DictionaryPageOffset/Data_page_offset are not reliable
            new[]
                {
                    _thriftColumnChunk.MetaData?.DictionaryPageOffset ?? 0,
                    _thriftColumnChunk.MetaData!.DataPageOffset
                }
                .Where(e => e != 0)
                .Min();

        private async Task ReadDataPageV1Async(PageHeader ph, PackedColumn pc) {
            using(RentedBytes bytes = await ReadPageDataAsync(ph)) {
                if(ph.DataPageHeader == null) {
                    throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
                }

                int dataUsed = 0;
                int allValueCount = (int)_thriftColumnChunk.MetaData!.NumValues;
                int pageValueCount = ph.DataPageHeader.NumValues;

                if(_dataField.MaxRepetitionLevel > 0) {
                    int levelsRead = ReadLevels(
                        bytes.AsSpan(), _dataField.MaxRepetitionLevel,
                        pc.GetWriteableRepetitionLevelSpan(),
                        pageValueCount, null, out int usedLength);
                    pc.MarkRepetitionLevels(levelsRead);
                    dataUsed += usedLength;
                }

                int defNulls = 0;
                if(_dataField.MaxDefinitionLevel > 0) {
                    int levelsRead = ReadLevels(
                        bytes.AsSpan().Slice(dataUsed), _dataField.MaxDefinitionLevel,
                        pc.GetWriteableDefinitionLevelSpan(),
                        pageValueCount, null, out int usedLength);
                    dataUsed += usedLength;
                    defNulls = pc.MarkDefinitionLevels(levelsRead, _dataField.MaxDefinitionLevel);
                }

                int dataElementCount = pageValueCount - defNulls;

                ReadColumn(
                    bytes.AsSpan().Slice(dataUsed),
                    ph.DataPageHeader.Encoding,
                    allValueCount, dataElementCount,
                    pc);
            }
        }

        private async Task ReadDataPageV2Async(PageHeader ph, PackedColumn pc, long maxValues) {
            if(ph.DataPageHeaderV2 == null) {
                throw new ParquetException($"column '{_dataField.Path}' is missing data page header, file is corrupt");
            }

            using(RentedBytes bytes = await ReadPageDataV2Async(ph)) {
                int dataUsed = 0;

                if(_dataField.MaxRepetitionLevel > 0) {
                    int levelsRead = ReadLevels(
                        bytes.AsSpan(),
                        _dataField.MaxRepetitionLevel,
                        pc.GetWriteableRepetitionLevelSpan(),
                        ph.DataPageHeaderV2.NumValues,
                        ph.DataPageHeaderV2.RepetitionLevelsByteLength,
                        out int usedLength);
                    dataUsed += usedLength;
                    pc.MarkRepetitionLevels(levelsRead);
                }

                if(_dataField.MaxDefinitionLevel > 0) {
                    int levelsRead = ReadLevels(
                        bytes.AsSpan().Slice(dataUsed),
                        _dataField.MaxDefinitionLevel,
                        pc.GetWriteableDefinitionLevelSpan(),
                        ph.DataPageHeaderV2.NumValues,
                        ph.DataPageHeaderV2.DefinitionLevelsByteLength,
                        out int usedLength);
                    dataUsed += usedLength;
                    pc.MarkDefinitionLevels(levelsRead);
                }

                int maxReadCount = ph.DataPageHeaderV2.NumValues - ph.DataPageHeaderV2.NumNulls;

                bool isCompressedFlag = ph.DataPageHeaderV2.IsCompressed ?? false;
                bool metaSaysUncompressed = _thriftColumnChunk.MetaData!.Codec == CompressionCodec.UNCOMPRESSED;

                if(!isCompressedFlag || metaSaysUncompressed) {
                    ReadColumn(bytes.AsSpan().Slice(dataUsed), ph.DataPageHeaderV2.Encoding, maxValues, maxReadCount, pc);
                    return;
                }

                int dataSize = ph.CompressedPageSize
                               - ph.DataPageHeaderV2.RepetitionLevelsByteLength
                               - ph.DataPageHeaderV2.DefinitionLevelsByteLength;

                int decompressedSize = ph.UncompressedPageSize
                                       - ph.DataPageHeaderV2.RepetitionLevelsByteLength
                                       - ph.DataPageHeaderV2.DefinitionLevelsByteLength;

                CompressionMethod method = (CompressionMethod)(int)_thriftColumnChunk.MetaData.Codec;
                if(method != CompressionMethod.Gzip) {
                    throw new NotSupportedException($"Compression codec '{_thriftColumnChunk.MetaData.Codec}' is not supported. Only GZip is supported.");
                }

                byte[] decompressed = Compressor.Decompress(
                    method,
                    bytes.AsSpan().Slice(dataUsed, dataSize),
                    decompressedSize);

                ReadColumn(
                    new Span<byte>(decompressed, 0, decompressed.Length),
                    ph.DataPageHeaderV2.Encoding,
                    maxValues, maxReadCount,
                    pc);
            }
        }

        private int ReadLevels(Span<byte> s, int maxLevel,
            Span<int> dest,
            int pageSize,
            int? length, out int usedLength) {

            int bitWidth = maxLevel.GetBitWidth();

            return RleBitpackedHybridEncoder.Decode(s, bitWidth, length, out usedLength, dest, pageSize);
        }

        private void ReadColumn(Span<byte> src,
            Encoding encoding, long totalValuesInChunk, int totalValuesInPage,
            PackedColumn pc) {

            //dictionary encoding uses RLE to encode data

            switch(encoding) {
                case Encoding.PLAIN: { // 0
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        ParquetPlainEncoder.Decode(plainData,
                            offset,
                            totalValuesInPage,
                            _schemaElement!, src, out int read);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Encoding.PLAIN_DICTIONARY: // 2  // values are still encoded in RLE
                case Encoding.RLE_DICTIONARY: { // 8
                        Span<int> span = pc.AllocateOrGetDictionaryIndexes(totalValuesInPage);
                        int indexCount = ReadRleDictionary(src, totalValuesInPage, span);
                        pc.MarkUsefulDictionaryIndexes(indexCount);
                        pc.Checkpoint();
                    }
                    break;

                case Encoding.RLE: { // 3
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        if(_dataField.ClrType == typeof(bool)) {
                            // for boolean values, we need to read into temporary int buffer and convert to booleans.
                            // todo: we can optimise this by implementing boolean RLE decoder

                            int[] tmp = new int[plainData.Length];
                            int read = RleBitpackedHybridEncoder.Decode(src,
                                _schemaElement!.TypeLength ?? 0,
                                src.Length, out int usedLength, tmp.AsSpan(offset), totalValuesInPage);

                            // copy back to bool array
                            bool[] tgt = (bool[])plainData;
                            for(int i = 0; i < read; i++) {
                                tgt[i + offset] = tmp[i] == 1;
                            }

                            pc.MarkUsefulPlainData(read);
                            pc.Checkpoint();

                        } else {
                            Span<int> span = ((int[])plainData).AsSpan(offset);
                            int read = RleBitpackedHybridEncoder.Decode(src,
                                _schemaElement!.TypeLength ?? 0,
                                src.Length, out int usedLength, span, totalValuesInPage);
                            pc.MarkUsefulPlainData(read);
                            pc.Checkpoint();
                        }
                    }
                    break;

                case Encoding.DELTA_BINARY_PACKED: {// 5
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaBinaryPackedEncoder.Decode(src, plainData, offset, totalValuesInPage, out _);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Encoding.DELTA_LENGTH_BYTE_ARRAY: {  // 6
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaLengthByteArrayEncoder.Decode(src, plainData, offset, totalValuesInPage);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Encoding.DELTA_BYTE_ARRAY: {         // 7
                        Array plainData = pc.GetPlainDataToReadInto(out int offset);
                        int read = DeltaByteArrayEncoder.Decode(src, plainData, offset, totalValuesInPage);
                        pc.MarkUsefulPlainData(read);
                    }
                    break;

                case Encoding.BIT_PACKED:                // 4 (deprecated)
                case Encoding.BYTE_STREAM_SPLIT:         // 9
                default:
                    throw new ParquetException($"encoding {encoding} is not supported.");
            }
        }

        private static int ReadRleDictionary(Span<byte> s, int maxReadCount, Span<int> dest) {
            int offset = 0;
            int destOffset = 0;
            int start = destOffset;
            int bitWidth = s[offset++];

            int length = s.Length - 1;

            //when bit width is zero reader must stop and just repeat zero maxValue number of times
            if(bitWidth == 0 || length == 0) {
                for(int i = 0; i < maxReadCount; i++) {
                    dest[destOffset++] = 0;
                }
            } else {
                if(length != 0) {
                    destOffset += RleBitpackedHybridEncoder.Decode(s.Slice(1), bitWidth, length, out int usedLength, dest, maxReadCount);
                }
            }

            return destOffset - start;
        }
    }
}
