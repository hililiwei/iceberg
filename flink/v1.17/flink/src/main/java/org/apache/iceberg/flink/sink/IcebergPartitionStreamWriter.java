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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.data.PartitionWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class IcebergPartitionStreamWriter extends AbstractStreamOperator<WriteResult>
    implements OneInputStreamOperator<RowData, WriteResult>, BoundedOneInput {
  private static final long serialVersionUID = 1L;

  private final String fullTableName;
  private final TaskWriterFactory<RowData> taskWriterFactory;

  private transient PartitionCommitWriter writer;
  private transient int subTaskId;
  private transient int attemptId;
  private transient IcebergStreamWriterMetrics writerMetrics;
  private transient long currentWatermark;

  IcebergPartitionStreamWriter(String fullTableName, TaskWriterFactory<RowData> taskWriterFactory) {
    this.fullTableName = fullTableName;
    this.taskWriterFactory = taskWriterFactory;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    this.writerMetrics = new IcebergStreamWriterMetrics(super.metrics, fullTableName);

    // Initialize the task writer factory.
    this.taskWriterFactory.initialize(subTaskId, attemptId);

    // Initialize the task writer.
    this.writer = (PartitionCommitWriter) taskWriterFactory.create();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    if (writer == null) {
      this.writer = (PartitionCommitWriter) taskWriterFactory.create();
    }
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    writer.write(element.getValue());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    this.currentWatermark = mark.getTimestamp();
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.

    // Set the watermark to the maximum value to ensure that all bounded data are flushed.
    this.currentWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    flush();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  /** close all open files and emit files to downstream committer operator */
  private void flush() throws IOException {
    if (writer == null) {
      return;
    }

    long startNano = System.nanoTime();
    List<PartitionWriteResult> results = Lists.newArrayList();
    PartitionWriteResult newResult = writer.complete(currentWatermark);
    while (newResult != null) {
      results.add(newResult);
      newResult = writer.complete(currentWatermark);
    }

    for (PartitionWriteResult result : results) {
      writerMetrics.updateFlushResult(result);
      output.collect(new StreamRecord<>(result));
      writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    }
  }
}
