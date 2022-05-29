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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSinkBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSinkBuilder.class);

  private Function<String, DataStream<RowData>> inputCreator = null;
  private TableLoader tableLoader;
  private Table table;
  private TableSchema tableSchema;
  private boolean overwrite = false;
  private DistributionMode distributionMode = null;
  private Integer writeParallelism = null;
  private boolean upsert = false;
  private List<String> equalityFieldColumns = null;
  private String uidPrefix = null;
  private final Map<String, String> snapshotProperties = Maps.newHashMap();
  private ReadableConfig readableConfig = new Configuration();

  private FlinkSinkBuilder() {
  }

  /**
   * This iceberg {@link Table} instance is used for initializing {@link IcebergStreamWriter} which will write all
   * the records into {@link DataFile}s and emit them to downstream operator. Providing a table would avoid so many
   * table loading from each separate task.
   *
   * @param newTable the loaded iceberg table instance.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder table(Table newTable) {
    this.table = newTable;
    return this;
  }

  /**
   * The table loader is used for loading tables in {@link IcebergFilesCommitter} lazily, we need this loader because
   * {@link Table} is not serializable and could not just use the loaded table from Builder#table in the remote task
   * manager.
   *
   * @param newTableLoader to load iceberg table inside tasks.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder tableLoader(TableLoader newTableLoader) {
    this.tableLoader = newTableLoader;
    return this;
  }

  public FlinkSinkBuilder tableSchema(TableSchema newTableSchema) {
    this.tableSchema = newTableSchema;
    return this;
  }

  public FlinkSinkBuilder overwrite(boolean newOverwrite) {
    this.overwrite = newOverwrite;
    return this;
  }

  public FlinkSinkBuilder flinkConf(ReadableConfig config) {
    this.readableConfig = config;
    return this;
  }

  /**
   * Configure the write {@link DistributionMode} that the flink sink will use. Currently, flink support
   * {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
   *
   * @param mode to specify the write distribution mode.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder distributionMode(DistributionMode mode) {
    Preconditions.checkArgument(
        !DistributionMode.RANGE.equals(mode),
        "Flink does not support 'range' write distribution mode now.");
    this.distributionMode = mode;
    return this;
  }

  /**
   * Configuring the write parallel number for iceberg stream writer.
   *
   * @param newWriteParallelism the number of parallel iceberg stream writer.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder writeParallelism(int newWriteParallelism) {
    this.writeParallelism = newWriteParallelism;
    return this;
  }

  /**
   * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which means it will
   * DELETE the old records and then INSERT the new records. In partitioned table, the partition fields should be
   * a subset of equality fields, otherwise the old row that located in partition-A could not be deleted by the
   * new row that located in partition-B.
   *
   * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder upsert(boolean enabled) {
    this.upsert = enabled;
    return this;
  }

  /**
   * Configuring the equality field columns for iceberg table that accept CDC or UPSERT events.
   *
   * @param columns defines the iceberg table's key.
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder equalityFieldColumns(List<String> columns) {
    this.equalityFieldColumns = columns;
    return this;
  }

  /**
   * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of multiple operators (like
   * writer, committer, dummy sink etc.) Actually operator uid will be appended with a suffix like "uidPrefix-writer".
   * <br><br>
   * If provided, this prefix is also applied to operator names.
   * <br><br>
   * Flink auto generates operator uid if not set explicitly. It is a recommended
   * <a href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
   * best-practice to set uid for all operators</a> before deploying to production. Flink has an option to {@code
   * pipeline.auto-generate-uid=false} to disable auto-generation and force explicit setting of all operator uid.
   * <br><br>
   * Be careful with setting this for an existing job, because now we are changing the operator uid from an
   * auto-generated one to this new value. When deploying the change with a checkpoint, Flink won't be able to restore
   * the previous Flink sink operator state (more specifically the committer operator state). You need to use {@code
   * --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink state is used to check if
   * last commit was actually successful or not. {@code --allowNonRestoredState} can lead to data loss if the
   * Iceberg commit failed in the last completed checkpoint.
   *
   * @param newPrefix prefix for Flink sink operator uid and name
   * @return {@link FlinkSinkBuilder} to connect the iceberg table.
   */
  public FlinkSinkBuilder uidPrefix(String newPrefix) {
    this.uidPrefix = newPrefix;
    return this;
  }

  public FlinkSinkBuilder setSnapshotProperties(Map<String, String> properties) {
    snapshotProperties.putAll(properties);
    return this;
  }

  public FlinkSinkBuilder setSnapshotProperty(String property, String value) {
    snapshotProperties.put(property, value);
    return this;
  }

  /**
   * Append the iceberg sink operators to write records to iceberg table.
   */
  public void append() {
    Preconditions.checkArgument(inputCreator != null,
        "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
    Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

    DataStream<RowData> rowDataInput = inputCreator.apply(uidPrefix);
    rowDataInput.sinkTo(build());
  }

  public FlinkSink build() {
    return new FlinkSink(
        tableLoader,
        table,
        tableSchema,
        overwrite,
        distributionMode,
        writeParallelism,
        upsert,
        equalityFieldColumns,
        uidPrefix,
        readableConfig);
  }

  private String operatorName(String suffix) {
    return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
  }
}

