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

import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkParquetProperties extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String TABLE_NAME = "test_table";
  private final FileFormat format;
  private final boolean isStreamingJob;
  private TableEnvironment tEnv;
  private Table icebergTable;

  public TestFlinkParquetProperties(
      String catalogName,
      Namespace baseNamespace,
      FileFormat format,
      Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
        }
      }
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env = StreamExecutionEnvironment
              .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
          env.enableCheckpointing(400);
          env.setMaxParallelism(1);
          env.setParallelism(1);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id int, data varchar) with " +
            "('write.format.default'='%s'," +
            "'" + TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES + "'='300'," +
            "'" + TableProperties.PARQUET_PAGE_SIZE_BYTES + "'='100'," +
            "'" + TableProperties.PARQUET_DICT_SIZE_BYTES + "'='100'," +
            "'" + TableProperties.PARQUET_COMPRESSION + "'='uncompressed')",
        TABLE_NAME, format.name());
  }

  @Override
  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
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

    List<ApiExpression> lists = Lists.newArrayListWithCapacity(500);
    for (int i = 0; i < 500; i++) {
      lists.add(Expressions.row(2, "world"));
    }
    // Register the rows into a temporary table.
    getTableEnv().createTemporaryView(
        "sourceTable",
        getTableEnv().fromValues(
            SimpleDataUtil.FLINK_SCHEMA.toRowDataType(),
            lists
        )
    );

    sql("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);

    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    CloseableIterator<DataFile> iterator = FindFiles.in(icebergTable).collect().iterator();
    Assert.assertTrue(iterator.hasNext());

    DataFile dataFile = iterator.next();
    Path path = new Path((String) dataFile.path());

    Configuration configuration = new Configuration();
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, path);
    ParquetFileReader parquetFileReader = new ParquetFileReader(
        configuration, footer.getFileMetaData(), path, footer.getBlocks(), columnDescriptors);

    int colAPageANum = 0;
    int colBPageNum = 0;
    PageReadStore pageReadStore;
    while ((pageReadStore = parquetFileReader.readNextRowGroup()) != null) {
      DictionaryPage colADictionaryPage = readNextDictionaryPage(colADesc, pageReadStore);
      String colAEncodingName = colADictionaryPage.getEncoding().name();
      Assert.assertEquals("PLAIN_DICTIONARY", colAEncodingName);

      DictionaryPage colBDictionaryPage = readNextDictionaryPage(colBDesc, pageReadStore);
      String colBEncodingName = colBDictionaryPage.getEncoding().name();
      Assert.assertEquals("PLAIN_DICTIONARY", colBEncodingName);

      while (readNextPage(colADesc, pageReadStore) != null) {
        colAPageANum++;
      }
      while (readNextPage(colBDesc, pageReadStore) != null) {
        colBPageNum++;
      }
    }

    Assert.assertEquals("should have 6 pages.", 6, colAPageANum);
    Assert.assertEquals("should have 6 pages.", 6, colBPageNum);

    Assert.assertEquals(3, footer.getBlocks().size());
    Assert.assertEquals(277, footer.getBlocks().get(0).getRowCount());
    Assert.assertEquals(138, footer.getBlocks().get(1).getRowCount());
    Assert.assertEquals(85, footer.getBlocks().get(2).getRowCount());
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
  private DictionaryPage readNextDictionaryPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return pageReadStore.getPageReader(colDesc).readDictionaryPage();
  }
}
