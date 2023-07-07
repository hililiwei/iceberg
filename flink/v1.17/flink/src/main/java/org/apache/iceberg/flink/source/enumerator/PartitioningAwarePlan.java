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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PartitioningAwarePlan<T extends PartitionScanTask> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitioningAwarePlan.class);

  private List<T> tasks = null; // lazy cache of uncombined tasks
  private Set<PartitionSpec> specs = null; // lazy cache of scanned specs
  private List<ScanTaskGroup<T>> taskGroups = null; // lazy cache of task groups
  private Types.StructType groupingKeyType = null; // lazy cache of the grouping key type
  private final boolean preserveDataGrouping;

  private final Table table;
  private final Schema expectedSchema;

  public PartitioningAwarePlan(
      Table table,
      Schema expectedSchema,
      boolean preserveDataGrouping,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan) {
    this.table = table;
    this.expectedSchema = expectedSchema;
    this.preserveDataGrouping = preserveDataGrouping;
    this.scan = scan;
  }

  protected abstract Class<T> taskJavaClass();

  private final Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan;

  protected Types.StructType groupingKeyType() {
    if (groupingKeyType == null) {
      if (preserveDataGrouping) {
        this.groupingKeyType = computeGroupingKeyType();
      } else {
        this.groupingKeyType = Types.StructType.of();
      }
    }

    return groupingKeyType;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected Table table() {
    return table;
  }

  private Types.StructType computeGroupingKeyType() {
    return org.apache.iceberg.Partitioning.groupingKeyType(expectedSchema(), specs());
  }

  protected Set<PartitionSpec> specs() {
    if (specs == null) {
      // avoid calling equals/hashCode on specs as those methods are relatively expensive
      IntStream specIds = tasks().stream().mapToInt(task -> task.spec().specId()).distinct();
      this.specs = specIds.mapToObj(id -> table().specs().get(id)).collect(Collectors.toSet());
    }

    return specs;
  }

  protected synchronized List<T> tasks() {
    if (tasks == null) {
      try (CloseableIterable<? extends ScanTask> taskIterable = scan.planFiles()) {
        List<T> plannedTasks = Lists.newArrayList();

        for (ScanTask task : taskIterable) {
          ValidationException.check(
              taskJavaClass().isInstance(task),
              "Unsupported task type, expected a subtype of %s: %s",
              taskJavaClass().getName(),
              task.getClass().getName());

          plannedTasks.add(taskJavaClass().cast(task));
        }

        this.tasks = plannedTasks;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close scan: " + scan, e);
      }
    }

    return tasks;
  }

  public synchronized List<ScanTaskGroup<T>> taskGroups() {
    if (taskGroups == null) {
      if (groupingKeyType().fields().isEmpty()) {
        CloseableIterable<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                CloseableIterable.withNoopClose(tasks()),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost());
        this.taskGroups = Lists.newArrayList(plannedTaskGroups);

        LOG.debug(
            "Planned {} task group(s) without data grouping for table {}",
            taskGroups.size(),
            table().name());

      } else {
        List<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                tasks(),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost(),
                groupingKeyType());
        StructLikeSet plannedGroupingKeys = collectGroupingKeys(plannedTaskGroups);

        LOG.debug(
            "Planned {} task group(s) with {} grouping key type and {} unique grouping key(s) for table {}",
            plannedTaskGroups.size(),
            groupingKeyType(),
            plannedGroupingKeys.size(),
            table().name());

        this.taskGroups = plannedTaskGroups;
      }
    }

    return taskGroups;
  }

  private StructLikeSet collectGroupingKeys(Iterable<ScanTaskGroup<T>> taskGroupIterable) {
    StructLikeSet keys = StructLikeSet.create(groupingKeyType());

    for (ScanTaskGroup<T> taskGroup : taskGroupIterable) {
      keys.add(taskGroup.groupingKey());
    }

    return keys;
  }
}
