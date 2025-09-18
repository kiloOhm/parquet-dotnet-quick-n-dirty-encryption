using System;
using System.IO;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class EncryptionTest : TestBase {

        [Fact]
        public async Task EncryptAndDecrypt() {
            string key = "Hallo Welt"; // 11 bytes only, will be derived to 32 bytes inside

            //write a single file having 3 row groups
            var id = new DataField<int>("id");
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms, new ParquetOptions() {
                EncryptionKey = key
            })) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 2 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3 }));
                }
            }

            //read the file back and validate
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions() {
                EncryptionKey = key
            })) {
                Assert.Equal(3, reader.RowGroupCount);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 1 }, dc.Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(1)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 2 }, dc.Data);
                }

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(2)) {
                    Assert.Equal(1, rg.RowCount);
                    DataColumn dc = await rg.ReadColumnAsync(id);
                    Assert.Equal(new int[] { 3 }, dc.Data);
                }
            }
        }

        [Fact]
        public async Task EncryptAndFailToDecrypt() {
            string key = "Hallo Welt"; // 11 bytes only, will be derived to 32 bytes inside

            //write a single file having 3 row groups
            var id = new DataField<int>("id");
            var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(id), ms, new ParquetOptions() {
                EncryptionKey = key
            })) {
                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 1 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 2 }));
                }

                using(ParquetRowGroupWriter rg = writer.CreateRowGroup()) {
                    await rg.WriteColumnAsync(new DataColumn(id, new int[] { 3 }));
                }

                writer.CustomMetadata = new System.Collections.Generic.Dictionary<string, string> {
                    { "mykey", "myvalue" }
                };
            }

            //read the file back and validate
            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions() {
                EncryptionKey = "wrong key"
            })) {
                reader.CustomMetadata.TryGetValue("mykey", out string? val);
                Assert.NotEqual("myvalue", val);

                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    Assert.Equal(1, rg.RowCount);
                    await Assert.ThrowsAnyAsync<Exception>(async () => {
                        DataColumn dc = await rg.ReadColumnAsync(id);
                    });
                }
            }
        }
    }
}