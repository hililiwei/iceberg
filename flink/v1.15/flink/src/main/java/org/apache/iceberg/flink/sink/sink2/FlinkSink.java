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

package org.apache.iceberg.flink.sink.sink2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSinkCommon;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.sink1.TaskWriterFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.flink.sink.FlinkSinkCommon.distributeDataStream;
import static org.apache.iceberg.flink.sink.FlinkSinkCommon.toFlinkRowType;

public class FlinkSink implements Sink<RowData>,
    WithPreWriteTopology<RowData>,
    WithPreCommitTopology<RowData, WriteResult>,
    WithPostCommitTopology<RowData, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSink.class);

  private final TableLoader tableLoader;
  private Table table;
  private final TableSchema tableSchema;
  private boolean overwrite = false;
  private DistributionMode distributionMode = null;
  private Integer writeParallelism = null;
  private boolean upsert = false;
  private List<String> equalityFieldColumns = null;
  private String uidPrefix = null;
  private final Map<String, String> snapshotProperties = Maps.newHashMap();
  private ReadableConfig readableConfig;

  public FlinkSink(
      TableLoader tableLoader,
      Table table, TableSchema tableSchema, boolean overwrite, DistributionMode distributionMode,
      Integer writeParallelism, boolean upsert, List<String> equalityFieldColumns, String uidPrefix,
      ReadableConfig readableConfig) {
    this.tableLoader = tableLoader;
    this.table = table;
    this.tableSchema = tableSchema;
    this.overwrite = overwrite;
    this.distributionMode = distributionMode;
    this.writeParallelism = writeParallelism;
    this.upsert = upsert;
    this.equalityFieldColumns = equalityFieldColumns;
    this.uidPrefix = uidPrefix;
    this.readableConfig = readableConfig;

    Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

    if (table == null) {
      tableLoader.open();
      try (TableLoader loader = tableLoader) {
        this.table = loader.loadTable();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to load iceberg table from table loader: " + tableLoader, e);
      }
    }
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);
    List<Integer> equalityFieldIds = FlinkSinkCommon.checkAndGetEqualityFieldIds(table, equalityFieldColumns);
    return distributeDataStream(
        inputDataStream, table.properties(), equalityFieldIds, table.spec(), table.schema(), flinkRowType,
        distributionMode, equalityFieldColumns);
  }

  @Override
  public PrecommittingSinkWriter<RowData, WriteResult> createWriter(InitContext context) throws IOException {

    RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);
    List<Integer> equalityFieldIds = FlinkSinkCommon.checkAndGetEqualityFieldIds(table, equalityFieldColumns);

    // Fallback to use upsert mode parsed from table properties if don't specify in job level.
    boolean upsertMode = upsert || PropertyUtil.propertyAsBoolean(table.properties(),
        UPSERT_ENABLED, UPSERT_ENABLED_DEFAULT);

    // Validate the equality fields and partition fields if we enable the upsert mode.
    if (upsertMode) {
      Preconditions.checkState(
          !overwrite,
          "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
      Preconditions.checkState(
          !equalityFieldIds.isEmpty(),
          "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
      if (!table.spec().isUnpartitioned()) {
        for (PartitionField partitionField : table.spec().fields()) {
          Preconditions.checkState(equalityFieldIds.contains(partitionField.sourceId()),
              "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
              partitionField, equalityFieldColumns);
        }
      }
    }

    Map<String, String> props = table.properties();
    long targetFileSize = PropertyUtil.propertyAsLong(
        props,
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    FileFormat fileFormat = getFileFormat(props);

    Table serializableTable = SerializableTable.copyOf(table);
    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(
        serializableTable, flinkRowType, targetFileSize,
        fileFormat, equalityFieldIds, upsert);

    return new IcebergStreamWriter(table.name(), taskWriterFactory, context.getSubtaskId(), 1);
  }

  @Override
  public DataStream<CommittableMessage<WriteResult>> addPreCommitTopology(DataStream<CommittableMessage<WriteResult>> writeResults) {
    return writeResults.map(values -> values).setParallelism(1).setMaxParallelism(1);
  }

  @Override
  public Committer<WriteResult> createCommitter() {
    return new IcebergFilesCommitter(tableLoader, overwrite, snapshotProperties,
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE));
  }

  @Override
  public SimpleVersionedSerializer<WriteResult> getCommittableSerializer() {
    return new IcebergWriteResultSerializer();
  }

  @Override
  public void addPostCommitTopology(DataStream<CommittableMessage<WriteResult>> committables) {
    // to commit后面的事务处理

  }

  private FileFormat getFileFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }
}
