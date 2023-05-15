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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.PartitionWriteResult;
import org.apache.iceberg.flink.util.PartitionCommitTriggerUtils;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;

public class PartitionCommitWriter extends BaseTaskWriter<RowData> {
  private final Map<PartitionKey, PartitionedRollingFileWriter> writers = Maps.newHashMap();
  private final Set<PartitionKey> partitionKeys = Sets.newHashSet();
  private final Map<PartitionKey, List<DataFile>> completedDataFiles = Maps.newHashMap();
  private final Map<PartitionKey, List<DeleteFile>> completedDeleteFiles = Maps.newHashMap();
  private final Map<PartitionKey, CharSequenceSet> referencedDataFiles = Maps.newHashMap();
  private final PartitionKey partitionKey;
  private final RowDataWrapper wrapper;
  private final String commitDelayString;
  private final String watermarkZoneID;
  private final String extractorPattern;
  private final String formatterPattern;
  private long watermark;

  protected PartitionCommitWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<RowData> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      String commitDelayString,
      String watermarkZoneID,
      String extractorPattern,
      String formatterPattern) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.commitDelayString = commitDelayString;
    this.watermarkZoneID = watermarkZoneID;
    this.extractorPattern = extractorPattern;
    this.formatterPattern = formatterPattern;
  }

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(RowData row) throws IOException {
    partitionKey.partition(wrapper().wrap(row));

    PartitionedRollingFileWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      partitionKeys.add(copiedKey);
      writer = new PartitionedRollingFileWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    writer.write(row);
  }

  public WriteResult complete(long currentWatermark) throws IOException {
    this.watermark = currentWatermark;
    close();

    Iterator<PartitionKey> partitionKeyIterator = partitionKeys.iterator();

    while (partitionKeyIterator.hasNext()) {
      PartitionKey tempPartitionKey = partitionKeyIterator.next();
      if (PartitionCommitTriggerUtils.isPartitionCommittable(
          watermark,
          tempPartitionKey,
          commitDelayString,
          watermarkZoneID,
          extractorPattern,
          formatterPattern)) {

        PartitionWriteResult.PartitionWriteResultBuilder builder =
            PartitionWriteResult.partitionWriteResultBuilder();

        List<DataFile> completedDataFile = completedDataFiles.remove(tempPartitionKey);
        if (completedDataFile != null) {
          builder.addDataFiles(completedDataFile);
        }

        List<DeleteFile> completedDeleteFile = completedDeleteFiles.remove(tempPartitionKey);
        if (completedDeleteFile != null) {
          builder.addDeleteFiles(completedDeleteFile);
        }

        CharSequenceSet referencedDataFile = referencedDataFiles.remove(tempPartitionKey);
        if (referencedDataFile != null) {
          builder.addReferencedDataFiles();
        }

        builder.partitionKey(tempPartitionKey);
        PartitionWriteResult completePartitionResult = builder.build();

        partitionKeyIterator.remove();
        return completePartitionResult;
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    if (writers.isEmpty()) {
      return;
    }

    for (PartitionKey tempPartitionKey : partitionKeys) {
      if (PartitionCommitTriggerUtils.isPartitionCommittable(
          watermark,
          tempPartitionKey,
          commitDelayString,
          watermarkZoneID,
          extractorPattern,
          formatterPattern)) {
        PartitionedRollingFileWriter writer = writers.get(tempPartitionKey);
        if (writer != null) {
          writer.close();
        }
        writers.remove(tempPartitionKey);
      }
    }
  }

  protected class PartitionedRollingFileWriter extends RollingFileWriter {
    private final PartitionKey partitionKey;

    public PartitionedRollingFileWriter(StructLike partitionKey) {
      super(partitionKey);
      this.partitionKey = (PartitionKey) partitionKey;
    }

    @Override
    protected void complete(DataWriter<RowData> closedWriter) {
      completedDataFiles.compute(
          partitionKey,
          (newPartitionKey, dataFiles) -> {
            if (dataFiles == null) {
              return Lists.newArrayList(closedWriter.toDataFile());
            } else {
              dataFiles.add(closedWriter.toDataFile());
              return dataFiles;
            }
          });
    }
  }
}
