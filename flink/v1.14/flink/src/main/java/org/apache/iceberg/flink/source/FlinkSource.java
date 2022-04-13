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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Accessors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSource {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSource.class);

  private FlinkSource() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}. See more options
   * in {@link ScanContext}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and read
   * records incrementally.
   * <ul>
   *   <li>Without startSnapshotId: Bounded</li>
   *   <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  /**
   * Source builder to build {@link DataStream}.
   */
  public static class Builder {
    private static final Set<String> FILE_SYSTEM_SUPPORT_LOCALITY = ImmutableSet.of("hdfs");

    private StreamExecutionEnvironment env;
    private Table table;
    private TableLoader tableLoader;
    private TableSchema projectedSchema;
    private int[][] projectedFields;
    private ReadableConfig readableConfig = new Configuration();
    private final ScanContext.Builder contextBuilder = ScanContext.builder();
    private Boolean exposeLocality;

    public Builder tableLoader(TableLoader newLoader) {
      this.tableLoader = newLoader;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder env(StreamExecutionEnvironment newEnv) {
      this.env = newEnv;
      return this;
    }

    public Builder filters(List<Expression> filters) {
      contextBuilder.filters(filters);
      return this;
    }

    public Builder project(TableSchema schema) {
      this.projectedSchema = schema;
      return this;
    }

    public Builder projectFields(int[][] newProjectFields) {
      this.projectedFields = newProjectFields;
      return this;
    }

    public Builder limit(long newLimit) {
      contextBuilder.limit(newLimit);
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      contextBuilder.fromProperties(properties);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      contextBuilder.caseSensitive(caseSensitive);
      return this;
    }

    public Builder snapshotId(Long snapshotId) {
      contextBuilder.useSnapshotId(snapshotId);
      return this;
    }

    public Builder startSnapshotId(Long startSnapshotId) {
      contextBuilder.startSnapshotId(startSnapshotId);
      return this;
    }

    public Builder endSnapshotId(Long endSnapshotId) {
      contextBuilder.endSnapshotId(endSnapshotId);
      return this;
    }

    public Builder asOfTimestamp(Long asOfTimestamp) {
      contextBuilder.asOfTimestamp(asOfTimestamp);
      return this;
    }

    public Builder splitSize(Long splitSize) {
      contextBuilder.splitSize(splitSize);
      return this;
    }

    public Builder splitLookback(Integer splitLookback) {
      contextBuilder.splitLookback(splitLookback);
      return this;
    }

    public Builder splitOpenFileCost(Long splitOpenFileCost) {
      contextBuilder.splitOpenFileCost(splitOpenFileCost);
      return this;
    }

    public Builder streaming(boolean streaming) {
      contextBuilder.streaming(streaming);
      return this;
    }

    public Builder exposeLocality(boolean newExposeLocality) {
      this.exposeLocality = newExposeLocality;
      return this;
    }

    public Builder nameMapping(String nameMapping) {
      contextBuilder.nameMapping(nameMapping);
      return this;
    }

    public Builder flinkConf(ReadableConfig config) {
      this.readableConfig = config;
      return this;
    }

    public FlinkInputFormat buildFormat() {
      Preconditions.checkNotNull(tableLoader, "TableLoader should not be null");

      Schema icebergSchema;
      FileIO io;
      EncryptionManager encryption;
      if (table == null) {
        // load required fields by table loader.
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          table = loader.loadTable();
          icebergSchema = table.schema();
          io = table.io();
          encryption = table.encryption();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        icebergSchema = table.schema();
        io = table.io();
        encryption = table.encryption();
      }

      if (projectedSchema == null) {
        contextBuilder.project(icebergSchema);
      } else if (projectedFields != null) {
        // Push down the nested projection so that don't need to get extra fields when get data from files.
        Schema icebergProjectionSchema = projectSchema(icebergSchema);
        contextBuilder.project(icebergProjectionSchema);
        projectedFields = mappingNestProjectedFields(icebergProjectionSchema);
      } else {
        contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedSchema));
      }

      contextBuilder.exposeLocality(localityEnabled());
      contextBuilder.planParallelism(readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE));

      return new FlinkInputFormat(tableLoader, icebergSchema, io, encryption, projectedFields,
          contextBuilder.build());
    }

    public DataStream<RowData> build() {
      Preconditions.checkNotNull(env, "StreamExecutionEnvironment should not be null");
      FlinkInputFormat format = buildFormat();

      ScanContext context = contextBuilder.build();

      TypeInformation<RowData> typeInfo =
          FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(FlinkSchemaUtil.convert(projectedSchema)));

      if (!context.isStreaming()) {
        int parallelism = inferParallelism(format, context);
        if (env.getMaxParallelism() > 0) {
          parallelism = Math.min(parallelism, env.getMaxParallelism());
        }
        return env.createInput(format, typeInfo).setParallelism(parallelism);
      } else {
        StreamingMonitorFunction function = new StreamingMonitorFunction(tableLoader, context);

        String monitorFunctionName = String.format("Iceberg table (%s) monitor", table);
        String readerOperatorName = String.format("Iceberg table (%s) reader", table);

        return env.addSource(function, monitorFunctionName)
            .transform(readerOperatorName, typeInfo, StreamingReaderOperator.factory(format));
      }
    }

    int inferParallelism(FlinkInputFormat format, ScanContext context) {
      int parallelism = readableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
      if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM)) {
        int maxInferParallelism = readableConfig.get(FlinkConfigOptions
            .TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX);
        Preconditions.checkState(
            maxInferParallelism >= 1,
            FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX.key() + " cannot be less than 1");
        int splitNum;
        try {
          FlinkInputSplit[] splits = format.createInputSplits(0);
          splitNum = splits.length;
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to create iceberg input splits for table: " + table, e);
        }

        parallelism = Math.min(splitNum, maxInferParallelism);
      }

      if (context.limit() > 0) {
        int limit = context.limit() >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) context.limit();
        parallelism = Math.min(parallelism, limit);
      }

      // parallelism must be positive.
      parallelism = Math.max(1, parallelism);
      return parallelism;
    }

    private boolean localityEnabled() {
      Boolean localityEnabled =
          this.exposeLocality != null ? this.exposeLocality :
              readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO);

      if (localityEnabled != null && !localityEnabled) {
        return false;
      }

      FileIO fileIO = table.io();
      if (fileIO instanceof HadoopFileIO) {
        HadoopFileIO hadoopFileIO = (HadoopFileIO) fileIO;
        try {
          String scheme = new Path(table.location()).getFileSystem(hadoopFileIO.getConf()).getScheme();
          return FILE_SYSTEM_SUPPORT_LOCALITY.contains(scheme);
        } catch (IOException e) {
          LOG.warn("Failed to determine whether the locality information can be exposed for table: {}", table, e);
        }
      }

      return false;
    }

    private Schema projectSchema(Schema tableSchema) {
      List<String> fieldNames = Lists.newArrayListWithCapacity(projectedFields.length);
      for (int[] indexPath : projectedFields) {
        Types.NestedField nestedField = tableSchema.columns().get(indexPath[0]);
        StringBuilder builder = new StringBuilder(nestedField.name());
        for (int index = 1; index < indexPath.length; index++) {
          Types.NestedField nestedFieldTemp = nestedField.type().asStructType().fields().get(indexPath[index]);
          builder.append(".").append(nestedFieldTemp.name());
        }
        fieldNames.add(builder.toString());
      }
      return tableSchema.select(fieldNames);
    }

    public int[][] mappingNestProjectedFields(Schema icebergSchema) {
      int[][] mappingNestProjectedFields = new int[projectedSchema.getFieldCount()][];

      String[] fieldNames = projectedSchema.getFieldNames();
      for (int i = 0; i < fieldNames.length; i++) {
        String fieldName = fieldNames[i];
        Integer[] positions =
            Accessors.toPositions(icebergSchema.accessorForField(icebergSchema.findField(fieldName).fieldId()));
        mappingNestProjectedFields[i] = Arrays.stream(positions).mapToInt(l -> l).toArray();
      }

      return mappingNestProjectedFields;
    }
  }

  public static boolean isBounded(Map<String, String> properties) {
    return !ScanContext.builder().fromProperties(properties).build().isStreaming();
  }
}
