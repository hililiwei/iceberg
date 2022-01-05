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

package org.apache.iceberg.flink;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestOrcTableProperties extends TestFlinkTableSink {

  public TestOrcTableProperties(
      String catalogName,
      Namespace baseNamespace,
      FileFormat format,
      Boolean isStreamingJob) {
    super(catalogName, baseNamespace, format, isStreamingJob);
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC}) {
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

  @Test
  public void testOrcStripSize() throws Exception {

    String stripeSize = "16";
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);

    sql("CREATE TABLE %s (id int, data varchar) " +
            "with ('write.format.default'='%s'," +
            "'%s'='%s'," +
            "'orc.rows.between.memory.checks'='1'," +
            "'iceberg.orc.vectorbatch.size'='1')",
        TABLE_NAME, format.name(), TableProperties.ORC_STRIPE_SIZE_BYTES, stripeSize);

    sql("INSERT INTO %s values (1,'a'),(2,'b'),(3,'c')  ", TABLE_NAME);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    CloseableIterator<DataFile> iterator = FindFiles.in(table).collect().iterator();
    Assert.assertTrue(iterator.hasNext());

    Assert.assertEquals(table.properties().get(TableProperties.ORC_STRIPE_SIZE_BYTES), stripeSize);

    DataFile dataFile = iterator.next();
    Reader reader =
        OrcFile.createReader(
            new Path((String) dataFile.path()), OrcFile.readerOptions(new Configuration()));

    Assert.assertEquals(3, reader.getStripes().size());
  }

  @Test
  public void testOrcBlockSize() throws Exception {

    long blockSize = 18L;
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);

    sql("CREATE TABLE %s (id int, data varchar) " +
            "with ('write.format.default'='%s'," +
            "'%s'='%s')",
        TABLE_NAME, format.name(), TableProperties.ORC_BLOCK_SIZE_BYTES, blockSize);

    sql("INSERT INTO %s values (1,'a'),(2,'b'),(3,'c') ", TABLE_NAME);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    CloseableIterator<DataFile> iterator = FindFiles.in(table).collect().iterator();
    Assert.assertTrue(iterator.hasNext());

    Assert.assertEquals(blockSize, Long.parseLong(table.properties().get(TableProperties.ORC_BLOCK_SIZE_BYTES)));

    DataFile dataFile = iterator.next();
    Reader reader =
        OrcFile.createReader(
            new Path((String) dataFile.path()), OrcFile.readerOptions(new Configuration()));

    Assert.assertEquals(blockSize, reader.getStripes().get(0).getOffset());
  }
}
