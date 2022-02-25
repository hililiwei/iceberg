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

package org.apache.iceberg.example.flink.streaming;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.example.ExampleUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

public class FlinkSteamingExample {
  public static void main(String[] args) throws Exception {
    // the host and the port to connect to
    final String hostname = "localhost";
    final int port=9999;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      // hostname = params.has("hostname") ? params.get("hostname") : "localhost";
      // port = params.getInt("port");
    } catch (Exception e) {
      System.err.println(
          "No port specified. Please run 'SocketWindowWordCount "
              + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
              + "and port is the address of the text server");
      System.err.println(
          "To start a simple text server, run 'netcat -l <port>' and "
              + "type the input text into the command line");
      return;
    }

    File warehouse = File.createTempFile("warehouse", null);
    warehouse.delete();

    Configuration conf = new Configuration();
    String warehousePath = "file://" + warehouse.getPath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    Table table = catalog.createTable(ExampleUtil.TABLE_IDENTIFIER, ExampleUtil.SCHEMA, ExampleUtil.PARTITION_SPEC);

    TableLoader tableLoader = TableLoader.fromHadoopTable(warehousePath + "/logging/logs");

    // get the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // get input data by connecting to the socket
    SingleOutputStreamOperator<Row> source = env
        .socketTextStream(hostname, port, "\n")
        .filter(one -> !Strings.isNullOrEmpty(one))
        .map(one -> {
          String[] split = one.split(",");
          return Row.of(split[0], Instant.ofEpochMilli(Long.parseLong(split[1])), split[2]);
        });

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
    // CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
    //   try {
    //     env.execute("Test Iceberg DataStream");
    //   } catch (Exception e) {
    //     e.printStackTrace();
    //   }
    // });

    // final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
    // DataStream<RowData> stream = FlinkSource.forRowData()
    //     .env(env2)
    //     .tableLoader(tableLoader)
    //     .streaming(true)
    //     .build();
    // stream.print();
    //
    // CompletableFuture cf = CompletableFuture.runAsync(() -> {
    //   try {
    //     env2.execute("start");
    //   } catch (Exception e) {
    //     e.printStackTrace();
    //   }
    // });
    //
    // cf.get();
    // cf2.get();
  }
}
