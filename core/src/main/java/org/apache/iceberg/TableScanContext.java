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
package org.apache.iceberg;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ThreadPools;

/** Context object with optional arguments for a TableScan. */
final class TableScanContext {
  private final Long snapshotId;
  private final Expression rowFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final boolean colStats;
  private final Schema projectedSchema;
  private final Collection<String> selectedColumns;
  private final ImmutableMap<String, String> options;
  private final Long fromSnapshotId;
  private final Long toSnapshotId;
  private final ExecutorService planExecutor;
  private final boolean fromSnapshotInclusive;
  private final MetricsReporter metricsReporter;
  private final String branch;

  TableScanContext() {
    this.snapshotId = null;
    this.rowFilter = Expressions.alwaysTrue();
    this.ignoreResiduals = false;
    this.caseSensitive = true;
    this.colStats = false;
    this.projectedSchema = null;
    this.selectedColumns = null;
    this.options = ImmutableMap.of();
    this.fromSnapshotId = null;
    this.toSnapshotId = null;
    this.planExecutor = null;
    this.fromSnapshotInclusive = false;
    this.metricsReporter = new LoggingMetricsReporter();
    this.branch = null;
  }

  private TableScanContext(
      Long snapshotId,
      Expression rowFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      boolean colStats,
      Schema projectedSchema,
      Collection<String> selectedColumns,
      ImmutableMap<String, String> options,
      Long fromSnapshotId,
      Long toSnapshotId,
      ExecutorService planExecutor,
      boolean fromSnapshotInclusive,
      MetricsReporter metricsReporter,
      String branch) {
    this.snapshotId = snapshotId;
    this.rowFilter = rowFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.colStats = colStats;
    this.projectedSchema = projectedSchema;
    this.selectedColumns = selectedColumns;
    this.options = options;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
    this.planExecutor = planExecutor;
    this.fromSnapshotInclusive = fromSnapshotInclusive;
    this.metricsReporter = metricsReporter;
    this.branch = branch;
  }

  Long snapshotId() {
    return snapshotId;
  }

  TableScanContext useSnapshotId(Long scanSnapshotId) {
    return new TableScanContext(
        scanSnapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  String branch() {
    return branch;
  }

  TableScanContext useBranch(String ref) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        ref);
  }

  Expression rowFilter() {
    return rowFilter;
  }

  TableScanContext filterRows(Expression filter) {
    return new TableScanContext(
        snapshotId,
        filter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  boolean ignoreResiduals() {
    return ignoreResiduals;
  }

  TableScanContext ignoreResiduals(boolean shouldIgnoreResiduals) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        shouldIgnoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  TableScanContext setCaseSensitive(boolean isCaseSensitive) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        isCaseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  boolean returnColumnStats() {
    return colStats;
  }

  TableScanContext shouldReturnColumnStats(boolean returnColumnStats) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        returnColumnStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  Collection<String> selectedColumns() {
    return selectedColumns;
  }

  TableScanContext selectColumns(Collection<String> columns) {
    Preconditions.checkState(
        projectedSchema == null, "Cannot select columns when projection schema is set");
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        null,
        columns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  TableScanContext project(Schema schema) {
    Preconditions.checkState(
        selectedColumns == null, "Cannot set projection schema when columns are selected");
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        schema,
        null,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  Map<String, String> options() {
    return options;
  }

  TableScanContext withOption(String property, String value) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options);
    builder.put(property, value);
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        builder.build(),
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  Long fromSnapshotId() {
    return fromSnapshotId;
  }

  TableScanContext fromSnapshotIdExclusive(long id) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        id,
        toSnapshotId,
        planExecutor,
        false,
        metricsReporter,
        branch);
  }

  TableScanContext fromSnapshotIdInclusive(long id) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        id,
        toSnapshotId,
        planExecutor,
        true,
        metricsReporter,
        branch);
  }

  boolean fromSnapshotInclusive() {
    return fromSnapshotInclusive;
  }

  Long toSnapshotId() {
    return toSnapshotId;
  }

  TableScanContext toSnapshotId(long id) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        id,
        planExecutor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  ExecutorService planExecutor() {
    return Optional.ofNullable(planExecutor).orElseGet(ThreadPools::getWorkerPool);
  }

  boolean planWithCustomizedExecutor() {
    return planExecutor != null;
  }

  TableScanContext planWith(ExecutorService executor) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        executor,
        fromSnapshotInclusive,
        metricsReporter,
        branch);
  }

  MetricsReporter metricsReporter() {
    return metricsReporter;
  }

  TableScanContext reportWith(MetricsReporter reporter) {
    return new TableScanContext(
        snapshotId,
        rowFilter,
        ignoreResiduals,
        caseSensitive,
        colStats,
        projectedSchema,
        selectedColumns,
        options,
        fromSnapshotId,
        toSnapshotId,
        planExecutor,
        fromSnapshotInclusive,
        reporter,
        branch);
  }
}
