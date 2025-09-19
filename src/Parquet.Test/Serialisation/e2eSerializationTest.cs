using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Serialization;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class e2eSerializationTest : TestBase {
        class ClassWithListOfStringsProp {
            public List<string>? Strings { get; set; }
        }

        [Fact]
        public async Task E2EListOfStrings() {
            using var ms = new MemoryStream();

            var testData = new ClassWithListOfStringsProp {
                Strings = new List<string> { "a", "b", "c" }
            };

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(typeof(ClassWithListOfStringsProp).GetParquetSchema(true), ms)) {
                await ParquetSerializer.SerializeRowGroupAsync(writer, new[] { testData }, default);
            }
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ParquetSchema schema = reader.Schema;
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
            var buffer = new List<ClassWithListOfStringsProp>();
            await ParquetSerializer.DeserializeRowGroupAsync(rg, schema, buffer);
            Assert.Equal(testData.Strings.Count, buffer[0]?.Strings?.Count);
            Assert.Equal(testData.Strings[0], buffer[0]?.Strings?[0]);
        }

        class Item {
            public string? Name { get; set; }
            public int Value { get; set; }
        }

        class ClassWithListOfInstancesProp {
            public List<Item>? Items { get; set; }
        }

        [Fact]
        public async Task E2EListOfInstances() {
            using var ms = new MemoryStream();

            var testData = new ClassWithListOfInstancesProp {
                Items = new List<Item> {
                    new Item { Name = "a", Value = 1 },
                    new Item { Name = "b", Value = 2 },
                    new Item { Name = "c", Value = 3 }
                }
            };

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(typeof(ClassWithListOfInstancesProp).GetParquetSchema(true), ms)) {
                await ParquetSerializer.SerializeRowGroupAsync(writer, new[] { testData }, default);
            }
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ParquetSchema schema = reader.Schema;
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
            var buffer = new List<ClassWithListOfInstancesProp>();
            await ParquetSerializer.DeserializeRowGroupAsync(rg, schema, buffer);
            Assert.Equal(testData.Items.Count, buffer[0]?.Items?.Count);
            Assert.Equal(testData.Items[0]?.Name, buffer[0]?.Items?[0]?.Name);
            Assert.Equal(testData.Items[0]?.Value, buffer[0]?.Items?[0]?.Value);
        }

        class ClassWithDictOfDateTimeDoubleProp {
            public Dictionary<DateTime, double> Values { get; set; } = new Dictionary<DateTime, double>();
        }

        [Fact]
        public async Task E2EDictOfDateTimeDouble() {
            using var ms = new MemoryStream();

            var testData = new ClassWithDictOfDateTimeDoubleProp {
                Values = new Dictionary<DateTime, double> {
                    { new DateTime(2021, 1, 1), 1.1 },
                    { new DateTime(2021, 1, 2), 2.2 },
                    { new DateTime(2021, 1, 3), 3.3 }
                }
            };

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(typeof(ClassWithDictOfDateTimeDoubleProp).GetParquetSchema(true), ms)) {
                await ParquetSerializer.SerializeRowGroupAsync(writer, new[] { testData }, default);
            }
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ParquetSchema schema = reader.Schema;
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
            var buffer = new List<ClassWithDictOfDateTimeDoubleProp>();
            await ParquetSerializer.DeserializeRowGroupAsync(rg, schema, buffer);
            Assert.Equal(testData.Values.Count, buffer[0]?.Values?.Count);
            Assert.Equal(testData.Values[new DateTime(2021, 1, 1)], buffer[0]?.Values?[new DateTime(2021, 1, 1)]);
        }
    }
}
