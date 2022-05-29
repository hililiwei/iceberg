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

package org.apache.iceberg.flink.sink.v2;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergStreamWriter<T> implements
    StatefulSinkWriter<T, IcebergStreamWriterState<T>>,
    SinkWriter<T>,
    TwoPhaseCommittingSink.PrecommittingSinkWriter<T, IcebergFlinkCommittable> {

  private static final long serialVersionUID = 1L;

  private final String fullTableName;
  private TaskWriterFactory<T> taskWriterFactory;
  private transient TaskWriter<T> writer;
  private transient int subTaskId;
  private transient long attemptId;
  private List<IcebergFlinkCommittable> writeResultsRestore = Lists.newArrayList();

  public IcebergStreamWriter(String fullTableName, TaskWriter<T> writer, int subTaskId,
      long attemptId) {
    this.fullTableName = fullTableName;
    this.writer = writer;
    this.subTaskId = subTaskId;
    this.attemptId = attemptId;
  }

  public IcebergStreamWriter(String fullTableName, TaskWriterFactory<T> taskWriterFactory, int subTaskId,
                             long attemptId) {
    this.fullTableName = fullTableName;
    this.subTaskId = subTaskId;
    this.attemptId = attemptId;
    this.taskWriterFactory = taskWriterFactory;
    // Initialize the task writer factory.
    taskWriterFactory.initialize(subTaskId, attemptId);
    // Initialize the task writer.
    this.writer = taskWriterFactory.create();
  }

  public void writeResults(List<IcebergFlinkCommittable> newWriteResults) {
    this.writeResultsRestore = newWriteResults;
  }

  @Override
  public void write(T element, Context context) throws IOException, InterruptedException {
    writer.write(element);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  @Override
  public Collection<IcebergFlinkCommittable> prepareCommit() throws IOException {
    List<IcebergFlinkCommittable> writeResults = Lists.newArrayList();
    WriteResult complete = writer.complete();
    IcebergFlinkCommittable icebergFlinkCommittable = new IcebergFlinkCommittable(complete);
    writeResults.add(icebergFlinkCommittable);
    if (!writeResultsRestore.isEmpty()) {
      writeResults.addAll(writeResultsRestore);
      writeResultsRestore = Lists.newArrayList();
    }
    this.writer = taskWriterFactory.create();

    return writeResults;
  }

  @Override
  public List<IcebergStreamWriterState<T>> snapshotState(long checkpointId) {
    List<IcebergStreamWriterState<T>> state = Lists.newArrayList();

    state.add(new IcebergStreamWriterState<T>(checkpointId, writeResultsRestore));
    return state;
  }

  public IcebergStreamWriter<T> restoreWriter(Collection<IcebergStreamWriterState<T>> recoveredState) {

    List<IcebergFlinkCommittable> icebergFlinkCommittables = Lists.newArrayList();
    for (IcebergStreamWriterState<T> icebergStreamWriterState : recoveredState) {
      icebergFlinkCommittables.addAll(icebergStreamWriterState.writeResults());
    }
    this.writeResultsRestore.addAll(icebergFlinkCommittables);
    return this;
  }
}
