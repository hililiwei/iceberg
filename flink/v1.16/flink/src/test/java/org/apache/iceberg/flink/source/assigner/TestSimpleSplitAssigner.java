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
package org.apache.iceberg.flink.source.assigner;

import java.util.List;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSimpleSplitAssigner extends TestSplitAssignerBase {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testEmptyInitialization() {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  /** Test a sequence of interactions for StaticEnumerator */
  @Test
  public void testStaticEnumeratorSequence() throws Exception {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 4, 2));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1));
    assertSnapshot(assigner, 2);

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }

  /** Test a sequence of interactions for ContinuousEnumerator */
  @Test
  public void testContinuousEnumeratorSequence() throws Exception {
    SimpleSplitAssigner assigner = new SimpleSplitAssigner();
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);

    List<IcebergSourceSplit> splits1 =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    assertAvailableFuture(assigner, 1, () -> assigner.onDiscoveredSplits(splits1));
    List<IcebergSourceSplit> splits2 =
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    assertAvailableFuture(assigner, 1, () -> assigner.onUnassignedSplits(splits2));

    assigner.onDiscoveredSplits(
        SplitHelpers.createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 2, 1));
    assertSnapshot(assigner, 2);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
    assertSnapshot(assigner, 0);
  }
}
