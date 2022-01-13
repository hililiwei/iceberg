/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkParquetWriter extends DataTest {
  private static final int NUM_RECORDS = 100;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private void writeAndValidate(Iterable<RowData> iterable, Schema schema) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
        .build()) {
      Iterator<RowData> expected = iterable.iterator();
      Iterator<Record> actual = reader.iterator();
      LogicalType rowType = FlinkSchemaUtil.convert(schema);
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of rows", actual.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), rowType, actual.next(), expected.next());
      }
      Assert.assertFalse("Should not have extra rows", actual.hasNext());
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(
        RandomRowData.generate(schema, NUM_RECORDS, 19981), schema);

    writeAndValidate(RandomRowData.convert(schema,
        RandomGenericData.generateDictionaryEncodableRecords(schema, NUM_RECORDS, 21124)),
        schema);

    writeAndValidate(RandomRowData.convert(schema,
        RandomGenericData.generateFallbackRecords(schema, NUM_RECORDS, 21124, NUM_RECORDS / 20)),
        schema);
  }

  @Test
  public void testParquetProperties() throws Exception {
    final MessageType schemaSimple =
        MessageTypeParser.parseMessageType(
            "message m {" +
                "  optional int32 id = 1;" +
                "  optional binary data (STRING) = 2;" +
                "}");

    final ColumnDescriptor colADesc = schemaSimple.getColumns().get(0);
    final ColumnDescriptor colBDesc = schemaSimple.getColumns().get(1);

    List<ColumnDescriptor> columnDescriptors = Arrays.asList(colADesc, colBDesc);

    int expectedRowCount = 100000;
    List<RowData> rows = Lists.newArrayListWithCapacity(expectedRowCount);
    for (int i = 0; i < expectedRowCount; i++) {
      rows.add(SimpleDataUtil.createRowData(1, UUID.randomUUID().toString().substring(0, 10)));
    }

    String location = temp.getRoot().getAbsolutePath();

    ImmutableMap<String, String> properties = ImmutableMap.of(
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(expectedRowCount * 10),
        TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(expectedRowCount),
        TableProperties.PARQUET_DICT_SIZE_BYTES, String.valueOf(expectedRowCount),
        TableProperties.PARQUET_COMPRESSION, "uncompressed");

    Table table = SimpleDataUtil.createTable(location, properties, false);

    writeAndCommit(table, ImmutableList.of(), false, rows);
    table.refresh();

    CloseableIterator<DataFile> iterator =
        FindFiles.in(table).collect().iterator();

    Assert.assertTrue(iterator.hasNext());

    DataFile dataFile = iterator.next();
    Path path = new Path((String) dataFile.path());

    Configuration configuration = new Configuration();
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, path);
    ParquetFileReader parquetFileReader = new ParquetFileReader(
        configuration, footer.getFileMetaData(), path, footer.getBlocks(), columnDescriptors);
    Assert.assertEquals(2, footer.getBlocks().size());

    int colAPageANum = 0;
    int colBPageNum = 0;
    long rowCount = 0;
    PageReadStore pageReadStore;
    while ((pageReadStore = parquetFileReader.readNextRowGroup()) != null) {

      rowCount += pageReadStore.getRowCount();

      DictionaryPage colADictionaryPage = readDictionaryPage(colADesc, pageReadStore);
      String colAEncodingName = colADictionaryPage.getEncoding().name();
      Assert.assertEquals("PLAIN_DICTIONARY", colAEncodingName);
      Assert.assertTrue(
          "The size of dictionary page should be smaller than " + expectedRowCount,
          colADictionaryPage.getUncompressedSize() <= expectedRowCount);

      DictionaryPage colBDictionaryPage = readDictionaryPage(colADesc, pageReadStore);
      String colBEncodingName = colBDictionaryPage.getEncoding().name();
      Assert.assertEquals("PLAIN_DICTIONARY", colBEncodingName);
      Assert.assertTrue(
          "The size of dictionary page should be smaller than " + expectedRowCount,
          colBDictionaryPage.getUncompressedSize() <= expectedRowCount);

      DataPageV1 colADataPage;
      while ((colADataPage = readNextPage(colADesc, pageReadStore)) != null) {
        Assert.assertTrue(
            "The size of each page should be smaller than " + expectedRowCount,
            colADataPage.getUncompressedSize() <= expectedRowCount);
        colAPageANum++;
      }
      DataPageV1 colBDataPage;
      while ((colBDataPage = readNextPage(colBDesc, pageReadStore)) != null) {
        Assert.assertTrue(
            "The size of each page should be smaller than " + expectedRowCount,
            colBDataPage.getUncompressedSize() <= expectedRowCount);
        colBPageNum++;
      }
    }

    Assert.assertEquals("should have 6 pages.", 6, colAPageANum);
    Assert.assertEquals("should have 16 pages.", 16, colBPageNum);
    Assert.assertEquals(expectedRowCount, rowCount);
  }

  private void writeAndCommit(Table table, List<Integer> eqFieldIds, boolean upsert, List<RowData> rows)
      throws IOException {
    TaskWriter<RowData> writer = createTaskWriter(table);
    for (RowData row : rows) {
      writer.write(row);
    }
    RowDelta delta = table.newRowDelta();
    WriteResult result = writer.complete();

    for (DataFile dataFile : result.dataFiles()) {
      delta.addRows(dataFile);
    }

    for (DeleteFile deleteFile : result.deleteFiles()) {
      delta.addDeletes(deleteFile);
    }

    delta.commit();
  }

  /**
   * Read the next page for a column
   */
  private DataPageV1 readNextPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return (DataPageV1) pageReadStore.getPageReader(colDesc).readPage();
  }

  /**
   * Read the next page for a column
   */
  private DictionaryPage readDictionaryPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return pageReadStore.getPageReader(colDesc).readDictionaryPage();
  }

  private TaskWriter<RowData> createTaskWriter(Table table) {
    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table),
        SimpleDataUtil.ROW_TYPE,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
        FileFormat.PARQUET,
        null,
        false);

    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }
}
