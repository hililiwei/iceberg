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

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.iceberg.util.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplitEnumerator implementation that supports dynamic filtering.
 *
 * <p>This enumerator handles {@link DynamicFilteringEvent} and filter out the desired input splits
 * with the support of the {@link DynamicFileEnumerator}.
 *
 * <p>If the enumerator receives the first split request before any dynamic filtering data is
 * received, it will enumerate all splits. If a DynamicFilterEvent is received during the fully
 * enumerating, the remaining splits will be filtered accordingly.
 */
@Internal
public class DynamicStaticIcebergEnumerator extends AbstractIcebergEnumerator
    implements SupportsHandleExecutionAttemptSourceEvent {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicStaticIcebergEnumerator.class);

  private final SplitAssigner assigner;
  private final ScanContext scanContext;

  /**
   * Stores the id of splits that has been assigned. The split assigner may be rebuilt when a
   * DynamicFilteringEvent is received. After that, the splits that are already assigned can be
   * assigned for the second time. We have to retain the state and filter out the splits that has
   * been assigned with this set.
   */
  private final Set<String> assignedSplits;

  private transient Set<String> allEnumeratingSplits;

  private final List<String> dynamicFilterFields;
  private Table table;
  private final TableLoader tableLoader;

  private Collection<IcebergSourceSplit> sourceSplits;

  public DynamicStaticIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner,
      ScanContext scanContext,
      List<String> dynamicFilterFields,
      TableLoader tableLoader) {
    super(enumeratorContext, assigner);
    this.assigner = assigner;
    this.scanContext = scanContext;
    this.dynamicFilterFields = dynamicFilterFields;
    this.tableLoader = tableLoader;
    this.assignedSplits = Sets.newHashSet();
  }

  @Override
  public void start() {
    super.start();
    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void close() throws IOException {
    super.close();
    tableLoader.close();
  }

  @Override
  public void addReader(int subtaskId) {
    // this source is purely lazy-pull-based, nothing to do upon registration
  }

  @Override
  public void addSplitsBack(List<IcebergSourceSplit> splits, int subtaskId) {
    LOG.info("Dynamic Iceberg Source Enumerator adds splits back: {}", splits);
    if (sourceSplits != null) {
      List<IcebergSourceSplit> fileSplits = Lists.newArrayList(splits);
      // Only add back splits enumerating. A split may be filtered after it is assigned.
      fileSplits.removeIf(s -> !allEnumeratingSplits.contains(s.splitId()));
      // Added splits should be removed from assignedSplits for re-assignment
      fileSplits.forEach(s -> assignedSplits.remove(s.splitId()));
      super.addSplitsBack(splits, subtaskId);
    }
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof DynamicFilteringEvent) {
      LOG.info("Received DynamicFilteringEvent: {}", subtaskId);
      DynamicFilteringData dynamicFilteringData = ((DynamicFilteringEvent) sourceEvent).getData();
      this.sourceSplits = planSplits(dynamicFilteringData);
      assigner.onDiscoveredSplits(sourceSplits);
    } else {
      if (sourceSplits == null) {
        // No DynamicFilteringData is received before the first split request, plan all splits
        this.sourceSplits = planSplits(null);
        assigner.onDiscoveredSplits(sourceSplits);
      }

      super.handleSourceEvent(subtaskId, sourceEvent);
    }
  }

  @Override
  protected boolean shouldAssignSplit(SourceSplit split) {
    return !assignedSplits.contains(split.splitId());
  }

  @Override
  protected void afterAssign(SourceSplit split) {
    assignedSplits.add(split.splitId());
  }

  @Override
  public IcebergEnumeratorState snapshotState(long checkpointId) {
    throw new UnsupportedOperationException(
        "DynamicFileSplitEnumerator only supports batch execution.");
  }

  @Override
  public void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent) {
    // Only recognize events that don't care attemptNumber
    handleSourceEvent(subtaskId, sourceEvent);
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return false;
  }

  @VisibleForTesting
  List<Expression> createFilters(RowType rowType, RowData rowData) {
    List<Expression> newFilters = Lists.newArrayList();
    for (int fieldPos = 0; fieldPos < rowType.getFieldCount(); ++fieldPos) {
      String partitionName = dynamicFilterFields.get(fieldPos);
      LogicalType fieldType = rowType.getTypeAt(fieldPos);
      switch (rowType.getTypeAt(fieldPos).getTypeRoot()) {
        case CHAR:
        case VARCHAR:
          if (Type.TypeID.UUID == table.schema().findType(partitionName).typeId()) {
            newFilters.add(
                Expressions.equal(partitionName, UUIDUtil.convert(rowData.getBinary(fieldPos))));
          } else {
            newFilters.add(
                Expressions.equal(partitionName, rowData.getString(fieldPos).toString()));
          }

          break;
        case BOOLEAN:
          newFilters.add(Expressions.equal(partitionName, rowData.getBoolean(fieldPos)));
          break;
        case DECIMAL:
          final int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
          final int decimalScale = LogicalTypeChecks.getScale(fieldType);
          Expressions.equal(
              partitionName,
              rowData.getDecimal(fieldPos, decimalPrecision, decimalScale).toBigDecimal());
          break;
        case TINYINT:
          newFilters.add(Expressions.equal(partitionName, (int) rowData.getByte(fieldPos)));
          break;
        case SMALLINT:
          newFilters.add(Expressions.equal(partitionName, (int) rowData.getShort(fieldPos)));
          break;
        case INTEGER:
        case DATE:
          newFilters.add(Expressions.equal(partitionName, rowData.getInt(fieldPos)));
          break;
        case BIGINT:
          newFilters.add(Expressions.equal(partitionName, rowData.getLong(fieldPos)));
          break;
        case FLOAT:
          newFilters.add(Expressions.equal(partitionName, rowData.getFloat(fieldPos)));
          break;
        case DOUBLE:
          newFilters.add(Expressions.equal(partitionName, rowData.getDouble(fieldPos)));
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          final int timestampPrecision = LogicalTypeChecks.getPrecision(fieldType);
          LocalDateTime localDateTime =
              rowData.getTimestamp(fieldPos, timestampPrecision).toLocalDateTime();
          newFilters.add(
              Expressions.equal(partitionName, DateTimeUtil.microsFromTimestamp(localDateTime)));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported type for dynamic filtering:" + rowType.getTypeAt(fieldPos));
      }
    }

    return newFilters;
  }

  @VisibleForTesting
  List<IcebergSourceSplit> planSplits(@Nullable DynamicFilteringData data) {
    ScanContext context = scanContext;
    if (data != null && data.isFiltering()) {
      LOG.debug("Filtering partitions of table {} based on the data: {}", table, data);
      RowType rowType = data.getRowType();
      Collection<RowData> rowDataSet = data.getData();

      Preconditions.checkArgument(rowType.getFieldCount() == dynamicFilterFields.size());

      List<Expression> filters = Lists.newArrayList();
      for (RowData rowData : rowDataSet) {
        filters.addAll(createFilters(rowType, rowData));
      }

      context = scanContext.copyAndAddFilters(filters);
    }

    ExecutorService workerPool =
        ThreadPools.newWorkerPool("DynamicFilteringData", scanContext.planParallelism());
    try {
      List<IcebergSourceSplit> pendingSplits =
          FlinkSplitPlanner.planPartitioningIcebergSourceSplits(table, context, workerPool);

      this.allEnumeratingSplits =
          pendingSplits.stream().map(IcebergSourceSplit::splitId).collect(Collectors.toSet());
      LOG.info("Discovered {} splits from table {}", pendingSplits.size(), table.name());
      return pendingSplits;
    } finally {
      workerPool.shutdown();
    }
  }

  /** refine scan with common configs */
  private static <T extends Scan<T, FileScanTask, CombinedScanTask>> T refineScanWithBaseConfigs(
      T scan, ScanContext context, ExecutorService workerPool) {
    T refinedScan =
        scan.caseSensitive(context.caseSensitive()).project(context.project()).planWith(workerPool);

    if (context.includeColumnStats()) {
      refinedScan = refinedScan.includeColumnStats();
    }

    refinedScan = refinedScan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());

    refinedScan =
        refinedScan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());

    refinedScan =
        refinedScan.option(
            TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());

    if (context.filters() != null) {
      for (Expression filter : context.filters()) {
        refinedScan = refinedScan.filter(filter);
      }
    }

    return refinedScan;
  }
}
