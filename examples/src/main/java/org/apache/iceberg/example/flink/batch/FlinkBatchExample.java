/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.example.flink.batch;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

public class FlinkBatchExample {
  public static void main(String[] args) throws Exception {

    Schema schema = new Schema(
        Types.NestedField.required(
            1,
            "level",
            Types.StringType.get()),
        Types.NestedField.required(
            2,
            "event_time",
            Types.TimestampType.withZone()),
        Types.NestedField.required(
            3,
            "message",
            Types.StringType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .hour("event_time")
        .identity("level")
        .build();

    File warehouse = File.createTempFile("warehouse", null);
    warehouse.delete();

    Configuration conf = new Configuration();
    String warehousePath = "file://" + warehouse.getPath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier name = TableIdentifier.of("logging", "logs");
    Table table = catalog.createTable(name, schema, spec);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    TableLoader tableLoader = TableLoader.fromHadoopTable(warehousePath + "/logging/logs");

    List<Row> rows = Arrays.asList(
        Row.of("ERROR", Instant.ofEpochMilli(1645783200000L), "A"),
        Row.of("ERROR", Instant.ofEpochMilli(1645783200000L), "A"),
        Row.of("ERROR", Instant.ofEpochMilli(1645786800000L), "A"),
        Row.of("ERROR", Instant.ofEpochMilli(1645786800000L), "A"));
    DataStreamSource<Row> source = env.fromCollection(rows);

    TableSchema tableSchema =
        TableSchema.builder()
            .field("level", DataTypes.STRING().notNull())
            .field("event_time", DataTypes.TIMESTAMP_LTZ().notNull())
            .field("message", DataTypes.STRING().notNull())
            .build();

    FlinkSink.forRow(source, tableSchema)
        .tableLoader(tableLoader)
        .append();

    env.execute("Test Iceberg DataStream");

    table.newScan().planFiles().forEach(one -> {
      System.out.println(one.file().path());
    });

    StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment();
    DataStream<RowData> batch = FlinkSource.forRowData()
        .env(env2)
        .tableLoader(tableLoader)
        .streaming(false)
        .build();

    // Print all records to stdout.
    batch.map(row -> {
      return new StringBuffer()
          .append(row.getString(0).toString())
          .append(",")
          .append(row.getTimestamp(1, 0).toString())
          .append(",")
          .append(row.getString(1).toString()).toString();
    }).print();

    env2.execute("Test Iceberg DataStream");
  }
}
