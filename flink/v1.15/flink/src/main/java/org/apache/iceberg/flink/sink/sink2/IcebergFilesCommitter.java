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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergFilesCommitter implements Committer<WriteResult> {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

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

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // 'dataFilesPerCheckpoint'.
  // private final List<WriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient Table table;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient long maxCommittedCheckpointId;
  private transient int continuousEmptyCheckpoints;
  private transient int maxContinuousEmptyCommits;
  // There're two cases that we restore from flink checkpoints: the first case is restoring from snapshot created by the
  // same flink job; another case is restoring from snapshot created by another different job. For the second case, we
  // need to maintain the old flink job's id in flink state backend to find the max-committed-checkpoint-id when
  // traversing iceberg table's snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);

  private final Integer workerPoolSize;
  private transient ExecutorService workerPool;

  public IcebergFilesCommitter(
      TableLoader tableLoader,
      boolean replacePartitions,
      Map<String, String> snapshotProperties,
      Integer workerPoolSize) {
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
    this.snapshotProperties = snapshotProperties;
    this.workerPoolSize = workerPoolSize;

    // final String operatorID = getRuntimeContext().getOperatorUniqueID();
    final String operatorID = "";
    this.workerPool = ThreadPools.newWorkerPool("iceberg-worker-pool-" + operatorID, workerPoolSize);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    maxContinuousEmptyCommits = PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(maxContinuousEmptyCommits > 0,
        MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");


  }


  @Override
  public void commit(Collection<CommitRequest<WriteResult>> requests) throws IOException, InterruptedException {

    List<WriteResult> collect = requests.stream().map(CommitRequest::getCommittable).collect(Collectors.toList());
    long currentCheckpointId = Long.MAX_VALUE;
    dataFilesPerCheckpoint.put(currentCheckpointId, writeToManifest(currentCheckpointId, collect));

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, currentCheckpointId);
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


  private void commitUpToCheckpoint(NavigableMap<Long, byte[]> deltaManifestsMap,
      String newFlinkJobId,
      long checkpointId) throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests = SimpleVersionedSerialization
          .readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, e.getValue());
      pendingResults.put(e.getKey(), FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io()));
      manifests.addAll(deltaManifests.manifests());
    }

    int totalFiles = pendingResults.values().stream()
        .mapToInt(r -> r.dataFiles().length + r.deleteFiles().length).sum();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, newFlinkJobId, checkpointId);
      } else {
        commitDeltaTxn(pendingResults, newFlinkJobId, checkpointId);
      }
      continuousEmptyCheckpoints = 0;
    }
    pendingMap.clear();

    // Delete the committed manifests.
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details = MoreObjects.toStringHelper(this)
            .add("flinkJobId", newFlinkJobId)
            .add("checkpointId", checkpointId)
            .add("manifestPath", manifest.path())
            .toString();
        LOG.warn("The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details, e);
      }
    }
  }


  private void replacePartitions(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId,
      long checkpointId) {
    // Partition overwrite does not support delete files.
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();
    Preconditions.checkState(deleteFilesNum == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);

    int numFiles = 0;
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

      numFiles += result.dataFiles().length;
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(dynamicOverwrite, numFiles, 0, "dynamic partition overwrite", newFlinkJobId, checkpointId);
  }

  private void commitDeltaTxn(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId, long checkpointId) {
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();

    if (deleteFilesNum == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);

      int numFiles = 0;
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

        numFiles += result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }

      commitOperation(appendFiles, numFiles, 0, "append", newFlinkJobId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential transaction txn1 and
        // txn2, the equality-delete files of txn2 are required to be applied to data files from txn1. Committing the
        // merged one will lead to the incorrect delete semantic.
        WriteResult result = e.getValue();

        // Row delta validations are not needed for streaming changes that write equality deletes. Equality deletes
        // are applied to data in all previous sequence numbers, so retries may push deletes further in the future,
        // but do not affect correctness. Position deletes committed to the table in this path are used only to delete
        // rows from data files that are being added in this commit. There is no way for data files added along with
        // the delete files to be concurrently removed, so there is no need to validate the files referenced by the
        // position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

        int numDataFiles = result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

        int numDeleteFiles = result.deleteFiles().length;
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        commitOperation(rowDelta, numDataFiles, numDeleteFiles, "rowDelta", newFlinkJobId, e.getKey());
      }
    }
  }


  private void commitOperation(
      SnapshotUpdate<?> operation, int numDataFiles, int numDeleteFiles, String description,
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

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId, List<WriteResult> writeResultList) throws IOException {
    if (writeResultList.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultList).build();
    DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(result,
        () -> manifestOutputFileFactory.create(checkpointId), table.spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }
}
