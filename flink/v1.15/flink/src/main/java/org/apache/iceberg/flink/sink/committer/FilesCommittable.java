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
package org.apache.iceberg.flink.sink.committer;

import java.io.Serializable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class FilesCommittable implements Serializable {
  private final WriteResult committable;
  private String jobID;
  private long checkpointId;
  private int subtaskId;

  public FilesCommittable(WriteResult committable, String jobID) {
    this.committable = committable;
    this.jobID = jobID;
  }

  public FilesCommittable(WriteResult committable, String jobID, long checkpointId, int subtaskId) {
    this.committable = committable;
    this.jobID = jobID;
    this.checkpointId = checkpointId;
    this.subtaskId = subtaskId;
  }

  public WriteResult committable() {
    return committable;
  }

  public Long checkpointId() {
    return checkpointId;
  }

  public String jobID() {
    return jobID;
  }

  public void jobID(String newJobID) {
    this.jobID = newJobID;
  }

  public void checkpointId(long newCheckpointId) {
    this.checkpointId = newCheckpointId;
  }

  public void subtaskId(int newSubtaskId) {
    this.subtaskId = newSubtaskId;
  }

  public int subtaskId() {
    return subtaskId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("committable", committable)
        .add("jobID", jobID)
        .add("checkpointId", checkpointId)
        .add("subtaskId", subtaskId)
        .toString();
  }
}
