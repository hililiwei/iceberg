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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Test Flink SELECT SQLs.
 */
public class TestFlinkScanSql extends TestFlinkSource {

  private volatile TableEnvironment tEnv;

  public TestFlinkScanSql(String fileFormat) {
    super(fileFormat);
  }

  @Override
  public void before() throws IOException {
    super.before();
    sql("create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse);
    sql("use catalog iceberg_catalog");
    getTableEnv().getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  private TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv = TableEnvironment.create(EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  @Override
  protected List<Row> run(FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter,
                          String... sqlSelectedFields) {
    String select = String.join(",", sqlSelectedFields);

    StringBuilder builder = new StringBuilder();
    sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

    String optionStr = builder.toString();

    if (optionStr.endsWith(",")) {
      optionStr = optionStr.substring(0, optionStr.length() - 1);
    }

    if (!optionStr.isEmpty()) {
      optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
    }

    return sql("select %s from t %s %s", select, optionStr, sqlFilter);
  }

  @Test
  public void testResiduals() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), TestFixtures.SCHEMA, TestFixtures.SPEC);

    List<Record> writeRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    Expression filter = Expressions.and(Expressions.equal("dt", "2020-03-20"), Expressions.equal("id", 123));
    org.apache.iceberg.flink.TestHelpers.assertRecords(runWithFilter(
        filter, "where dt='2020-03-20' and id=123"), expectedRecords, TestFixtures.SCHEMA);
  }

  @Test
  public void testNestProjection() throws Exception {
    Types.MapType map = Types.MapType.ofOptional(
        11, 12,
        Types.IntegerType.get(),
        Types.StringType.get()
    );

    Types.StructType struct = Types.StructType.of(
        Types.NestedField.required(21, "a", Types.LongType.get()),
        Types.NestedField.optional(22, "b", Types.LongType.get())
    );

    Types.StructType struct2 = Types.StructType.of(
        Types.NestedField.optional(31, "a", Types.StringType.get())
    );

    Schema schema = new Schema(
        required(1, "data", Types.StringType.get()),
        required(2, "id", Types.LongType.get()),
        required(3, "mp", map),
        required(4, "st", struct),
        required(5, "dt", Types.StringType.get()),
        required(6, "st2", struct2)
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("dt")
        .bucket("id", 1)
        .build();

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), schema, spec);

    List<Record> writeRecords = RandomGenericData.generate(schema, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(4, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(4, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));
    expectedRecords.add(writeRecords.get(1));

    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    helper.appendToTable(dataFile);

    List<Row> result = sql("SELECT id,st.a,st.b,st2.a,mp FROM t ");

    for (int i = 0; i < result.size(); i++) {
      Row actorResult = result.get(i);
      Record expectedResult = expectedRecords.get(i);
      Assert.assertEquals("id must be equal!", expectedResult.get(1), actorResult.getField(0));
      Assert.assertEquals("st.a must be equal!", ((Record) expectedResult.get(3)).get(0), actorResult.getField(1));
      Assert.assertEquals("st.b must be equal!", ((Record) expectedResult.get(3)).get(1), actorResult.getField(2));
      Assert.assertEquals("st2.a must be equal!", ((Record) expectedResult.get(5)).get(0), actorResult.getField(3));
      Assert.assertEquals("mp must be equal!", expectedResult.get(2), actorResult.getField(4));
    }
  }

  @Test
  public void testInferedParallelism() throws IOException {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), TestFixtures.SCHEMA, TestFixtures.SPEC);

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
    FlinkInputFormat flinkInputFormat = FlinkSource.forRowData().tableLoader(tableLoader).table(table).buildFormat();
    ScanContext scanContext = ScanContext.builder().build();

    // Empty table, infer parallelism should be at least 1
    int parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, scanContext);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0),
        RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    // Make sure to generate 2 CombinedScanTasks
    long maxFileLen = Math.max(dataFile1.fileSizeInBytes(), dataFile2.fileSizeInBytes());
    sql("ALTER TABLE t SET ('read.split.open-file-cost'='1', 'read.split.target-size'='%s')", maxFileLen);

    // 2 splits (max infer is the default value 100 , max > splits num), the parallelism is splits num : 2
    parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, scanContext);
    Assert.assertEquals("Should produce the expected parallelism.", 2, parallelism);

    // 2 splits and limit is 1 , max infer parallelism is default 100，
    // which is greater than splits num and limit, the parallelism is the limit value : 1
    parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, ScanContext.builder().limit(1).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits and max infer parallelism is 1 (max < splits num), the parallelism is  1
    Configuration configuration = new Configuration();
    configuration.setInteger(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX, 1);
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, max infer parallelism is 1, limit is 3, the parallelism is max infer parallelism : 1
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().limit(3).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, infer parallelism is disabled, the parallelism is flink default parallelism 1
    configuration.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().limit(3).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);
  }

  private List<Row> sql(String query, Object... args) {
    TableResult tableResult = getTableEnv().executeSql(String.format(query, args));
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      List<Row> results = Lists.newArrayList(iter);
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  private String optionToKv(String key, Object value) {
    return "'" + key + "'='" + value + "'";
  }
}
