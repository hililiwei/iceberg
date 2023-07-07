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
package org.apache.iceberg.flink.source.enumerator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.file.src.impl.DynamicFileSplitEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.flink.FlinkTestBase;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.DefaultSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for the {@link DynamicFileSplitEnumerator}. */
public class TestDynamicStaticIcebergEnumerator extends FlinkTestBase {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private static String warehouse;
  private TableLoader tableLoader;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Before
  public void before() {
    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d VARCHAR) PARTITIONED BY (d) WITH ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg','a'),(2,'b','b'),(3,CAST(NULL AS VARCHAR),'a')",
        TABLE_NAME);
    sql("INSERT INTO %s VALUES (4,'d','b')", TABLE_NAME);

    this.tableLoader =
        TableLoader.fromHadoopTable(warehouse + "/" + DATABASE_NAME + "/" + TABLE_NAME);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    dropCatalog(CATALOG_NAME, true);
  }

  @Test
  public void testDiscoverSplitWhenDynamicEnumerator() throws Exception {
    DefaultSplitAssigner splitPlanner = new DefaultSplitAssigner(null, Collections.emptyList());

    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    DynamicStaticIcebergEnumerator enumerator =
        createDynamicIcebergEnumerator(splitPlanner, enumeratorContext);

    DynamicFilteringEvent event = mockDynamicFilteringEvent();
    List<IcebergSourceSplit> icebergSourceSplits = enumerator.planSplits(event.getData());
    Assert.assertEquals(1, icebergSourceSplits.size());
    Assert.assertEquals(1, icebergSourceSplits.get(0).task().files().size());

    enumerator.handleSourceEvent(2, event);

    Assert.assertEquals(1, splitPlanner.pendingSplitCount());

    // register one reader, and let it request a split
    enumeratorContext.registerReader(2, "localhost");
    enumerator.handleSourceEvent(2, new SplitRequestEvent());
    Assert.assertEquals(0, splitPlanner.pendingSplitCount());
  }

  @Test
  public void testReceiveDynamicFilteringDataAfterStarted() throws Exception {
    DefaultSplitAssigner splitPlanner = new DefaultSplitAssigner(null, Collections.emptyList());

    MockSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new MockSplitEnumeratorContext<>(4);
    DynamicStaticIcebergEnumerator enumerator =
        createDynamicIcebergEnumerator(splitPlanner, enumeratorContext);

    List<IcebergSourceSplit> icebergSourceSplits = enumerator.planSplits(null);
    Assert.assertEquals(2, icebergSourceSplits.size());
    List<String> expectedPartitions = Lists.newArrayList("a", "b");
    List<String> partitions =
        icebergSourceSplits.stream()
            .map(IcebergSourceSplit::asScanTaskGroup)
            .map(ScanTaskGroup::groupingKey)
            .map(one -> one.get(0, String.class))
            .collect(Collectors.toList());
    Assertions.assertThatList(expectedPartitions).containsExactlyInAnyOrderElementsOf(partitions);
    for (IcebergSourceSplit icebergSourceSplit : icebergSourceSplits) {
      if (icebergSourceSplit.asScanTaskGroup().groupingKey().get(0, String.class).equals("a")) {
        Assertions.assertThat(1).isEqualTo(icebergSourceSplit.asScanTaskGroup().tasks().size());
      } else {
        Assertions.assertThat(2).isEqualTo(icebergSourceSplit.asScanTaskGroup().tasks().size());
      }
    }

    // register one reader, and let it request a split
    enumeratorContext.registerReader(new ReaderInfo(2, "localhost"));
    enumerator.handleSourceEvent(2, new SplitRequestEvent());

    Assert.assertEquals(1, getAssignedSplits(enumeratorContext).size());
    Assert.assertEquals(1, splitPlanner.pendingSplitCount());

    DynamicFilteringEvent event = mockDynamicFilteringEvent();
    enumerator.handleSourceEvent(2, event);

    Assert.assertEquals(1, getAssignedSplits(enumeratorContext).size());
    Assert.assertEquals(2, splitPlanner.pendingSplitCount());

    // request more than 3-1=2 times to check whether a split will be assigned twice
    for (int i = 0; i < 4; i++) {
      enumerator.handleSourceEvent(2, new SplitRequestEvent());
    }

    Assert.assertEquals(2, getAssignedSplits(enumeratorContext).size());
    Assert.assertEquals(0, splitPlanner.pendingSplitCount());
  }

  @Test
  public void testCreateRowSupportedTypes() throws Exception {
    List<Tuple3<LogicalType, Object, Object>> testTypeValues = Lists.newArrayList();
    testTypeValues.add(new Tuple3<>(new BooleanType(), true, true));
    testTypeValues.add(new Tuple3<>(new IntType(), 42, 42));
    testTypeValues.add(new Tuple3<>(new BigIntType(), 9876543210L, 9876543210L));
    testTypeValues.add(new Tuple3<>(new SmallIntType(), (short) 41, 41));
    testTypeValues.add(new Tuple3<>(new TinyIntType(), (byte) 40, 40));
    testTypeValues.add(new Tuple3<>(new VarCharType(), StringData.fromString("1234"), "1234"));
    testTypeValues.add(new Tuple3<>(new CharType(), StringData.fromString("7"), "7"));

    testTypeValues.add(
        new Tuple3<>(
            new DateType(),
            DataFormatConverters.LocalDateConverter.INSTANCE.toInternal(LocalDate.of(2022, 2, 22)),
            19045));
    testTypeValues.add(
        new Tuple3<>(
            new TimestampType(9),
            new DataFormatConverters.LocalDateTimeConverter(9)
                .toInternal(LocalDateTime.of(2022, 2, 22, 22, 2, 20, 20222022)),
            1645567340020222L));

    DynamicStaticIcebergEnumerator enumerator =
        createDynamicIcebergEnumerator(
            new DefaultSplitAssigner(null, Collections.emptyList()),
            new TestingSplitEnumeratorContext<IcebergSourceSplit>(4));

    for (int i = 0; i < testTypeValues.size(); i++) {
      GenericRowData genericRowData = new GenericRowData(testTypeValues.size());
      genericRowData.setField(0, testTypeValues.get(i).f1);

      RowType rowType = RowType.of(testTypeValues.get(i).f0);
      List<Expression> filters = enumerator.createFilters(rowType, genericRowData);

      Assert.assertEquals(
          ((UnboundPredicate) filters.get(0)).literal().value(), testTypeValues.get(i).f2);
    }
  }

  private static List<IcebergSourceSplit> getAssignedSplits(
      MockSplitEnumeratorContext<IcebergSourceSplit> context) {
    return context.getSplitsAssignmentSequence().stream()
        .flatMap(s -> s.assignment().get(2).stream())
        .collect(Collectors.toList());
  }

  private DynamicStaticIcebergEnumerator createDynamicIcebergEnumerator(
      DefaultSplitAssigner splitPlanner,
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext) {
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    DynamicStaticIcebergEnumerator enumerator =
        new DynamicStaticIcebergEnumerator(
            enumeratorContext,
            splitPlanner,
            scanContext,
            Lists.newArrayList("d"),
            tableLoader.clone());
    enumerator.start();
    return enumerator;
  }

  private static DynamicFilteringEvent mockDynamicFilteringEvent() {
    // Mock a DynamicFilteringData, typeInfo and rowType of which are not used.
    RowType rowType = RowType.of(new VarCharType());
    List<RowData> buildRows = Lists.newArrayList();
    buildRows.add(rowData("a"));
    DynamicFilteringData data =
        new DynamicFilteringData(
            InternalTypeInfo.of(rowType),
            rowType,
            buildRows.stream()
                .map(r -> serialize(InternalTypeInfo.of(rowType), r))
                .collect(Collectors.toList()),
            true);
    return new DynamicFilteringEvent(data);
  }

  private static RowData rowData(Object... values) {
    GenericRowData rowData = new GenericRowData(values.length);
    for (int i = 0; i < values.length; ++i) {
      Object value = values[i];
      value = value instanceof String ? new BinaryStringData((String) value) : value;
      rowData.setField(i, value);
    }
    return rowData;
  }

  private static byte[] serialize(TypeInformation<RowData> typeInfo, RowData row) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      typeInfo
          .createSerializer(new ExecutionConfig())
          .serialize(row, new DataOutputViewStreamWrapper(baos));
    } catch (IOException e) {
      // throw as RuntimeException so the function can use in lambda
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }
}
