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
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergFilesCommitter implements Committer<IcebergFlinkCommittable>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;
  private final Map<String, String> snapshotProperties;

  // It will have an unique identifier for one job.
  private transient Table table;
  private transient long maxCommittedCheckpointId;
  private transient int continuousEmptyCheckpoints;
  private transient int maxContinuousEmptyCommits;


  private final transient ExecutorService workerPool;

  public IcebergFilesCommitter(TableLoader tableLoader,
                               boolean replacePartitions,
                               Map<String, String> snapshotProperties,
                               Integer workerPoolSize) {

    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
    this.snapshotProperties = snapshotProperties;

    this.workerPool =
        ThreadPools.newWorkerPool("iceberg-worker-pool-" + Thread.currentThread().getName(), workerPoolSize);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    maxContinuousEmptyCommits = PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0,
        MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    maxContinuousEmptyCommits = PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(
        maxContinuousEmptyCommits > 0,
        MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
  }

  @Override
  public void commit(Collection<CommitRequest<IcebergFlinkCommittable>> requests)
      throws IOException, InterruptedException {
    int dataFilesNumRestored = 0;
    int deleteFilesNumRestored = 0;

    int dataFilesNumStored = 0;
    int deleteFilesNumStored = 0;

    long id = 0;
    Collection<CommitRequest<IcebergFlinkCommittable>> restored = Lists.newArrayList();
    Collection<CommitRequest<IcebergFlinkCommittable>> store = Lists.newArrayList();

    for (CommitRequest<IcebergFlinkCommittable> request : requests) {
      if (request.getNumberOfRetries() > 0) {
        if (request.getCommittable().checkpointId() > maxCommittedCheckpointId) {
          restored.add(request);
          WriteResult committable = request.getCommittable().committable();
          dataFilesNumRestored = dataFilesNumRestored + committable.dataFiles().length;
          deleteFilesNumRestored = deleteFilesNumRestored + committable.deleteFiles().length;
        }

      } else {
        if (request.getCommittable().checkpointId() > maxCommittedCheckpointId) {
          id = request.getCommittable().checkpointId();
          store.add(request);
          WriteResult committable = request.getCommittable().committable();
          dataFilesNumStored = dataFilesNumStored + committable.dataFiles().length;
          deleteFilesNumStored = deleteFilesNumStored + committable.deleteFiles().length;
        }
      }
    }

    if (restored.size() > 0) {
      commitResult(dataFilesNumRestored, deleteFilesNumRestored, restored);
    } else {
      commitResult(dataFilesNumStored, deleteFilesNumStored, store);
    }
    this.maxCommittedCheckpointId = id;
  }

  private void commitResult(
      int dataFilesNumRestored, int deleteFilesNumRestored,
      Collection<CommitRequest<IcebergFlinkCommittable>> committableCollection) {
    long totalFiles = (long) dataFilesNumRestored + deleteFilesNumRestored;

    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(committableCollection, deleteFilesNumRestored);
      } else {
        commitDeltaTxn(committableCollection, dataFilesNumRestored, deleteFilesNumRestored);
      }
      continuousEmptyCheckpoints = 0;
    }
  }

  @Override
  public void close() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  private void replacePartitions(Collection<CommitRequest<IcebergFlinkCommittable>> writeResults, int deleteFilesNum) {
    // Partition overwrite does not support delete files.
    int deleteFilesNumNew =
        writeResults.stream().mapToInt(r -> r.getCommittable().committable().deleteFiles().length).sum();
    Preconditions.checkState(deleteFilesNumNew == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);

    int numFiles = 0;
    String flinkJobId = "";
    Long checkpointId = 0L;
    for (CommitRequest<IcebergFlinkCommittable> writeResult : writeResults) {
      WriteResult result = writeResult.getCommittable().committable();
      Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      flinkJobId = writeResult.getCommittable().jobID();
      checkpointId = writeResult.getCommittable().checkpointId();
      numFiles += result.dataFiles().length;
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(dynamicOverwrite, numFiles, 0, "dynamic partition overwrite", flinkJobId, checkpointId);
  }

  private void commitDeltaTxn(Collection<CommitRequest<IcebergFlinkCommittable>> writeResults, int dataFilesNum,
                              int deleteFilesNum) {

    if (deleteFilesNum == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      String flinkJobId = "";
      Long checkpointId = 0L;
      for (CommitRequest<IcebergFlinkCommittable> commitRequest : writeResults) {
        WriteResult result = commitRequest.getCommittable().committable();
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files.");

        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        flinkJobId = commitRequest.getCommittable().jobID();
        checkpointId = commitRequest.getCommittable().checkpointId();
      }
      commitOperation(appendFiles, dataFilesNum, 0, "append", flinkJobId, checkpointId);
      for (CommitRequest<IcebergFlinkCommittable> commitRequest : writeResults) {
        commitRequest.signalAlreadyCommitted();
      }
    } else {
      // To be compatible with iceberg format V2.

      RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
      int numDataFiles = 0;
      int numDeleteFiles = 0;
      String flinkJobId = "";
      Long checkpointId = 0L;
      for (CommitRequest<IcebergFlinkCommittable> commitRequest : writeResults) {
        try {
          WriteResult result = commitRequest.getCommittable().committable();
          // We don't commit the merged result into a single transaction because for the sequential transaction txn1 and
          // txn2, the equality-delete files of txn2 are required to be applied to data files from txn1. Committing the
          // merged one will lead to the incorrect delete semantic.

          // Row delta validations are not needed for streaming changes that write equality deletes. Equality deletes
          // are applied to data in all previous sequence numbers, so retries may push deletes further in the future,
          // but do not affect correctness. Position deletes committed to the table in this path are used only to delete
          // rows from data files that are being added in this commit. There is no way for data files added along with
          // the delete files to be concurrently removed, so there is no need to validate the files referenced by the
          // position delete files that are being committed.

          numDataFiles = numDataFiles + result.dataFiles().length;
          Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

          numDeleteFiles = numDeleteFiles + result.deleteFiles().length;
          Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

          flinkJobId = commitRequest.getCommittable().jobID();
          checkpointId = commitRequest.getCommittable().checkpointId();
        } catch (Exception e) {
          commitRequest.signalFailedWithUnknownReason(e);
        }
      }

      commitOperation(rowDelta, numDataFiles, numDeleteFiles, "rowDelta", flinkJobId, checkpointId);
      // commitRequest.signalAlreadyCommitted();
    }
  }

  private void commitOperation(SnapshotUpdate<?> operation, int numDataFiles, int numDeleteFiles, String description,
                               String newFlinkJobId, long checkpointId) {
    LOG.info("Committing {} with {} data files and {} delete files to table {}", description, numDataFiles,
        numDeleteFiles, table);
    snapshotProperties.forEach(operation::set);
    // custom snapshot metadata properties will be overridden if they conflict with internal ones used by the sink.
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }
}
