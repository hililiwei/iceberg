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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A split assigner that assigns splits to subtasks based on the locality of the splits. */
@Internal
public class LocalitySplitAssigner implements SplitAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(LocalitySplitAssigner.class);

  private static final String DEFAULT_HOSTNAME = "hostname";
  private final Map<Set<String>, Deque<IcebergSourceSplit>> pendingSplits;
  private CompletableFuture<Void> availableFuture;

  public LocalitySplitAssigner() {
    this.pendingSplits = Maps.newHashMap();
  }

  public LocalitySplitAssigner(Collection<IcebergSourceSplitState> assignerState) {
    this.pendingSplits = Maps.newHashMap();
    Stream<IcebergSourceSplit> splits = assignerState.stream().map(IcebergSourceSplitState::split);
    addSplits(splits.collect(Collectors.toList()));
  }

  @Override
  public GetSplitResult getNext(@Nullable String hostname) {
    if (pendingSplits.isEmpty()) {
      return GetSplitResult.unavailable();
    }

    Deque<IcebergSourceSplit> icebergSourceSplits =
        hostname == null
            ? getIcebergSourceSplits(DEFAULT_HOSTNAME, pendingSplits)
            : getIcebergSourceSplits(hostname, pendingSplits);
    LOG.info("Get Iceberg source splits for: {}", hostname);

    if (!icebergSourceSplits.isEmpty()) {
      IcebergSourceSplit split = icebergSourceSplits.poll();
      return GetSplitResult.forSplit(split);
    }

    return GetSplitResult.unavailable();
  }

  private Deque<IcebergSourceSplit> getIcebergSourceSplits(
      String hostname, Map<Set<String>, Deque<IcebergSourceSplit>> splitsDeque) {
    if (splitsDeque.isEmpty()) {
      return new ArrayDeque<>();
    }

    Iterator<Map.Entry<Set<String>, Deque<IcebergSourceSplit>>> splitsIterator =
        splitsDeque.entrySet().iterator();
    while (splitsIterator.hasNext()) {
      Map.Entry<Set<String>, Deque<IcebergSourceSplit>> splitsEntry = splitsIterator.next();
      Deque<IcebergSourceSplit> splits = splitsEntry.getValue();
      if (splits.isEmpty()) {
        splitsIterator.remove();
        continue;
      }

      if (splitsEntry.getKey().contains(hostname)) {
        if (splits.size() == 1) {
          splitsIterator.remove();
        }

        return splits;
      }
    }

    if (!splitsDeque.isEmpty()) {
      return splitsDeque.values().stream().findAny().get();
    }

    return new ArrayDeque<>();
  }

  @Override
  public void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    addSplits(splits);
  }

  private synchronized void addSplits(Collection<IcebergSourceSplit> splits) {
    if (splits.isEmpty()) {
      return;
    }

    for (IcebergSourceSplit split : splits) {
      String[] hostnames = split.hostnames();
      if (hostnames == null) {
        hostnames = new String[] {DEFAULT_HOSTNAME};
      }

      Set<String> hosts = Sets.newHashSet(hostnames);
      Deque<IcebergSourceSplit> icebergSourceSplits =
          pendingSplits.computeIfAbsent(hosts, key -> new ArrayDeque<>());
      icebergSourceSplits.add(split);
    }

    // only complete pending future if new splits are discovered
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public synchronized Collection<IcebergSourceSplitState> state() {
    return pendingSplits.values().stream()
        .flatMap(Collection::stream)
        .map(split -> new IcebergSourceSplitState(split, IcebergSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public synchronized int pendingSplitCount() {
    return pendingSplits.values().stream().mapToInt(Deque::size).sum();
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
