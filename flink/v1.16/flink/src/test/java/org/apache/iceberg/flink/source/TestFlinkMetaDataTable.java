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
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkMetaDataTable extends FlinkCatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private static TemporaryFolder temp = new TemporaryFolder();

  public TestFlinkMetaDataTable(String catalogName, Namespace baseNamespace) {
    super(catalogName, baseNamespace);
  }

  @Override
  protected TableEnvironment getTableEnv() {
    Configuration configuration = super.getTableEnv().getConfig().getConfiguration();
    configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
    configuration.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, true);
    return super.getTableEnv();
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @Override
  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testSnapshots() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
    String sql = String.format("SELECT * FROM %s$snapshots ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      Assert.assertEquals(
          "Should have expected timestamp",
          ((Instant) row.getField(0)).toEpochMilli(),
          next.timestampMillis());
      Assert.assertEquals("Should have expected snapshot id", row.getField(1), next.snapshotId());
      Assert.assertEquals("Should have expected parent id", row.getField(2), next.parentId());
      Assert.assertEquals("Should have expected operation", row.getField(3), next.operation());
      Assert.assertEquals(
          "Should have expected manifest list location",
          row.getField(4),
          next.manifestListLocation());
      Assert.assertEquals("Should have expected summary", row.getField(5), next.summary());
    }
  }

  @Test
  public void testHistory() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
    String sql = String.format("SELECT * FROM %s$history ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      Assert.assertEquals(
          "Should have expected made_current_at",
          ((Instant) row.getField(0)).toEpochMilli(),
          next.timestampMillis());
      Assert.assertEquals("Should have expected snapshot id", row.getField(1), next.snapshotId());
      Assert.assertEquals("Should have expected parent id", row.getField(2), next.parentId());
      Assert.assertEquals(
          "Should have expected is current ancestor",
          row.getField(3),
          SnapshotUtil.isAncestorOf(
              table, next.snapshotId(), table.currentSnapshot().snapshotId()));
    }
  }

  @Test
  public void testManifests() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
    String sql = String.format("SELECT * FROM %s$manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    List<ManifestFile> expectedDataManifests = dataManifests(table);

    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      Assert.assertEquals(
          "Should have expected content", row.getField(0), manifestFile.content().id());
      Assert.assertEquals("Should have expected path", row.getField(1), manifestFile.path());
      Assert.assertEquals("Should have expected length", row.getField(2), manifestFile.length());
      Assert.assertEquals(
          "Should have expected partition_spec_id",
          row.getField(3),
          manifestFile.partitionSpecId());
      Assert.assertEquals(
          "Should have expected added_snapshot_id", row.getField(4), manifestFile.snapshotId());
      Assert.assertEquals(
          "Should have expected added_data_files_count",
          row.getField(5),
          manifestFile.addedFilesCount());
      Assert.assertEquals(
          "Should have expected existing_data_files_count",
          row.getField(6),
          manifestFile.existingFilesCount());
      Assert.assertEquals(
          "Should have expected deleted_data_files_count",
          row.getField(7),
          manifestFile.deletedFilesCount());
    }
  }

  @Test
  public void testAllManifests() {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
    String sql = String.format("SELECT * FROM %s$all_manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    List<ManifestFile> expectedDataManifests = dataManifests(table);

    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      Assert.assertEquals(
          "Should have expected content", row.getField(0), manifestFile.content().id());
      Assert.assertEquals("Should have expected path", row.getField(1), manifestFile.path());
      Assert.assertEquals("Should have expected length", row.getField(2), manifestFile.length());
      Assert.assertEquals(
          "Should have expected partition_spec_id",
          row.getField(3),
          manifestFile.partitionSpecId());
      Assert.assertEquals(
          "Should have expected added_snapshot_id", row.getField(4), manifestFile.snapshotId());
      Assert.assertEquals(
          "Should have expected added_data_files_count",
          row.getField(5),
          manifestFile.addedFilesCount());
      Assert.assertEquals(
          "Should have expected existing_data_files_count",
          row.getField(6),
          manifestFile.existingFilesCount());
      Assert.assertEquals(
          "Should have expected deleted_data_files_count",
          row.getField(7),
          manifestFile.deletedFilesCount());
    }
  }

  @Test
  public void testUnPartitionedTable() throws IOException {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(dataDelete.copy("id", 1));

    temp.create();
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp.newFile()), dataDeletes, deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    Assert.assertEquals("Should have 1 data manifest", 1, expectedDataManifests.size());
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("files"))
            .schema();

    // check delete files table
    Table deleteFilesTable =
        MetadataTableUtils.createMetadataTableInstance(
            table, MetadataTableType.from("delete_files"));
    Schema deleteFilesTableSchema = deleteFilesTable.schema();

    List<Row> actualDeleteFiles = sql("SELECT * FROM %s$delete_files", TABLE_NAME);
    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());

    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be one delete file manifest entry", 1, expectedDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        deleteFilesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check data files table
    List<Row> actualDataFiles = sql("SELECT * FROM %s$data_files", TABLE_NAME);
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());

    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // check all files table
    List<Row> actualFiles = sql("SELECT * FROM %s$files ORDER BY content", TABLE_NAME);
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());

    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have two files manifest entries", 2, expectedFiles.size());
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testPartitionedTable() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) PARTITIONED BY (data) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());

    sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20)", TABLE_NAME);

    sql("INSERT INTO %s VALUES (1,'b',10),(2,'b',20)", TABLE_NAME);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    temp.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    deleteRow.put("data", "a");
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    deleteRow.put("data", "b");
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes2).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    Assert.assertEquals("Should have 2 delete manifests", 2, expectedDeleteManifests.size());

    Table deleteFilesTable =
        MetadataTableUtils.createMetadataTableInstance(
            table, MetadataTableType.from("delete_files"));
    Schema filesTableSchema = deleteFilesTable.schema();

    // Check delete files table
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals(
        "Should have one delete file manifest entry", 1, expectedDeleteFiles.size());

    List<Row> actualDeleteF = sql("SELECT * FROM %s$delete_files", TABLE_NAME);

    List<Row> actualDeleteFiles =
        sql("SELECT * FROM %s$delete_files WHERE `partition`.`data`='a'", TABLE_NAME);

    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check data files table
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should have one data file manifest entry", 1, expectedDataFiles.size());

    List<Row> actualDataFiles =
        sql("SELECT * FROM %s$data_files  WHERE `partition`.`data`='a'", TABLE_NAME);
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    List<Row> actualPartitionsWithProjection =
        sql("SELECT file_count FROM %s$partitions ", TABLE_NAME);
    Assert.assertEquals(
        "Metadata table should return two partitions record",
        2,
        actualPartitionsWithProjection.size());
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(1, actualPartitionsWithProjection.get(i).getField(0));
    }

    // Check files table
    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have two file manifest entries", 2, expectedFiles.size());

    List<Row> actualFiles =
        sql("SELECT * FROM %s$files  WHERE `partition`.`data`='a' ORDER BY content", TABLE_NAME);
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testAllFilesUnpartitioned() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());

    sql("INSERT INTO %s VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)", TABLE_NAME);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    temp.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    Assert.assertEquals("Should have 1 data manifest", 1, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(
                table, MetadataTableType.from("all_data_files"))
            .schema();

    // Check all data files table
    List<Row> actualDataFiles = sql("SELECT * FROM %s$all_data_files", TABLE_NAME);

    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // Check all delete files table
    List<Row> actualDeleteFiles = sql("SELECT * FROM %s$all_delete_files", TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be one delete file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check all files table
    List<Row> actualFiles = sql("SELECT * FROM %s$all_files ORDER BY content", TABLE_NAME);
    List<GenericData.Record> expectedFiles =
        ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles, actualFiles);
  }

  @Test
  public void testAllFilesPartitioned() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) PARTITIONED BY (data) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());

    sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20)", TABLE_NAME);

    sql("INSERT INTO %s VALUES (1,'b',10),(2,'b',20)", TABLE_NAME);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    // Create delete file
    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    temp.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).addDeletes(eqDeletes2).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(
                table, MetadataTableType.from("all_data_files"))
            .schema();

    // Check all data files table
    List<Row> actualDataFiles =
        sql("SELECT * FROM %s$all_data_files WHERE `partition`.`data`='a'", TABLE_NAME);
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // Check all delete files table
    List<Row> actualDeleteFiles =
        sql("SELECT * FROM %s$all_delete_files WHERE `partition`.`data`='a'", TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDeleteFiles.size());

    TestHelpers.assertEqualsSafe(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check all files table
    List<Row> actualFiles =
        sql("SELECT * FROM %s$all_files WHERE `partition`.`data`='a' ORDER BY content", TABLE_NAME);
    List<GenericData.Record> expectedFiles =
        ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(filesTableSchema, expectedFiles, actualFiles);
  }

  @Test
  public void testSnapshotReferencesMetatable() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) PARTITIONED BY (data) WITH ('format-version'='2', 'write.format.default'='%s')",
        TABLE_NAME, format.name());

    sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20)", TABLE_NAME);

    sql("INSERT INTO %s VALUES (1,'b',10),(2,'b',20)", TABLE_NAME);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Long currentSnapshotId = table.currentSnapshot().snapshotId();

    // Create branch
    table
        .manageSnapshots()
        .createBranch("testBranch", currentSnapshotId)
        .setMaxRefAgeMs("testBranch", 10)
        .setMinSnapshotsToKeep("testBranch", 20)
        .setMaxSnapshotAgeMs("testBranch", 30)
        .commit();
    // Create Tag
    table
        .manageSnapshots()
        .createTag("testTag", currentSnapshotId)
        .setMaxRefAgeMs("testTag", 50)
        .commit();
    // Check refs table
    List<Row> references = sql("SELECT * FROM %s$refs", TABLE_NAME);
    Assert.assertEquals("Refs table should return 3 rows", 3, references.size());
    List<Row> branches = sql("SELECT * FROM %s$refs WHERE type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("Refs table should return 2 branches", 2, branches.size());
    List<Row> tags = sql("SELECT * FROM %s$refs WHERE type='TAG'", TABLE_NAME);
    Assert.assertEquals("Refs table should return 1 tag", 1, tags.size());

    // Check branch entries in refs table
    List<Row> mainBranch =
        sql("SELECT * FROM %s$refs WHERE name='main' AND type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("main", mainBranch.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", mainBranch.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, mainBranch.get(0).getFieldAs("snapshot_id"));

    List<Row> testBranch =
        sql("SELECT * FROM  %s$refs WHERE name='testBranch' AND type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("testBranch", testBranch.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", testBranch.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranch.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(10), testBranch.get(0).getFieldAs("max_reference_age_in_ms"));
    Assert.assertEquals(Integer.valueOf(20), testBranch.get(0).getFieldAs("min_snapshots_to_keep"));
    Assert.assertEquals(Long.valueOf(30), testBranch.get(0).getFieldAs("max_snapshot_age_in_ms"));

    // Check tag entries in refs table
    List<Row> testTag =
        sql("SELECT * FROM %s$refs WHERE name='testTag' AND type='TAG'", TABLE_NAME);
    Assert.assertEquals("testTag", testTag.get(0).getFieldAs("name"));
    Assert.assertEquals("TAG", testTag.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testTag.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(50), testTag.get(0).getFieldAs("max_reference_age_in_ms"));

    // Check projection in refs table
    List<Row> testTagProjection =
        sql(
            "SELECT name,type,snapshot_id,max_reference_age_in_ms,min_snapshots_to_keep FROM %s$refs where type='TAG'",
            TABLE_NAME);
    Assert.assertEquals("testTag", testTagProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("TAG", testTagProjection.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testTagProjection.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(50), testTagProjection.get(0).getFieldAs("max_reference_age_in_ms"));
    Assert.assertNull(testTagProjection.get(0).getFieldAs("min_snapshots_to_keep"));

    List<Row> mainBranchProjection =
        sql("SELECT name, type FROM %s$refs WHERE name='main' AND type = 'BRANCH'", TABLE_NAME);
    Assert.assertEquals("main", mainBranchProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", mainBranchProjection.get(0).getFieldAs("type"));

    List<Row> testBranchProjection =
        sql(
            "SELECT type, name, max_reference_age_in_ms, snapshot_id FROM %s$refs WHERE name='testBranch' AND type = 'BRANCH'",
            TABLE_NAME);
    Assert.assertEquals("testBranch", testBranchProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", testBranchProjection.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranchProjection.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(10), testBranchProjection.get(0).getFieldAs("max_reference_age_in_ms"));
  }

  /**
   * Find matching manifest entries of an Iceberg table
   *
   * @param table iceberg table
   * @param expectedContent file content to populate on entries
   * @param entriesTableSchema schema of Manifest entries
   * @param manifestsToExplore manifests to explore of the table
   * @param partValue partition value that manifest entries must match, or null to skip filtering
   */
  private List<GenericData.Record> expectedEntries(
      Table table,
      FileContent expectedContent,
      Schema entriesTableSchema,
      List<ManifestFile> manifestsToExplore,
      String partValue)
      throws IOException {
    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows =
          Avro.read(in).project(entriesTableSchema).build()) {
        for (GenericData.Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              asMetadataRecord(file, expectedContent);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  // Populate certain fields derived in the metadata tables
  private void asMetadataRecord(GenericData.Record file, FileContent content) {
    file.put(0, content.id());
    file.put(3, 0); // specId
  }

  private boolean partitionMatch(GenericData.Record file, String partValue) {
    if (partValue == null) {
      return true;
    }
    GenericData.Record partition = (GenericData.Record) file.get(4);
    return partValue.equals(partition.get(0).toString());
  }

  private List<ManifestFile> dataManifests(Table table) {
    return table.currentSnapshot().dataManifests(table.io());
  }

  private List<ManifestFile> deleteManifests(Table table) {
    return table.currentSnapshot().deleteManifests(table.io());
  }
}
