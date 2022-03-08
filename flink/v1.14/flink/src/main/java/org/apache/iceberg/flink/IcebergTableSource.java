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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Flink Iceberg table source.
 */
public class IcebergTableSource
    implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown {

  private int[][] projectedFields;
  private int[] topProjectedFields;
  private long limit;
  private List<Expression> filters;

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.topProjectedFields = toCopy.topProjectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
  }

  public IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                            ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, -1, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(TableLoader loader, TableSchema schema, Map<String, String> properties,
                             int[][] projectedFields, boolean isLimitPushDown,
                             long limit, List<Expression> filters, ReadableConfig readableConfig) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectedFields = projectFields;
    this.topProjectedFields = Arrays.stream(projectFields).mapToInt(array -> array[0]).distinct().toArray();
  }

  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .properties(properties)
        .project(getProjectedSchema())
        .projectedFields(projectedFields)
        .limit(limit)
        .filters(filters)
        .flinkConf(readableConfig)
        .build();
  }

  private TableSchema getProjectedSchema() {
    return projectedFields == null ? schema : projectSchema(schema, topProjectedFields);
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isPresent()) {
        expressions.add(icebergExpression.get());
        acceptedFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;
    return Result.of(acceptedFilters, flinkFilters);
  }

  @Override
  public boolean supportsNestedProjection() {
    return true;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        return createDataStream(execEnv);
      }

      @Override
      public boolean isBounded() {
        return FlinkSource.isBounded(properties);
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new IcebergTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table source";
  }

  private static TableSchema projectSchema(TableSchema tableSchema, int[] projectedFields) {
    Preconditions.checkArgument(
        FlinkCompatibilityUtil.allPhysicalColumns(tableSchema),
        "Projection is only supported for physical columns.");
    TableSchema.Builder builder = TableSchema.builder();

    FieldsDataType fields =
        (FieldsDataType)
            DataTypeUtils.projectRow(tableSchema.toRowDataType(), projectedFields);
    RowType topFields = (RowType) fields.getLogicalType();
    for (int i = 0; i < topFields.getFieldCount(); i++) {
      builder.field(topFields.getFieldNames().get(i), fields.getChildren().get(i));
    }
    return builder.build();
  }
}
