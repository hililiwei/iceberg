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
import java.io.UncheckedIOException;
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
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Tasks;

abstract class BaseDeltaPartitionedTaskWriter extends BaseTaskWriter<RowData> {

  private final PartitionKey partitionKey;

  private final Map<PartitionKey, List<DataFile>> completedDataFiles = Maps.newHashMap();
  private final Map<PartitionKey, List<DeleteFile>> completedDeleteFiles = Maps.newHashMap();
  private final Map<PartitionKey, CharSequenceSet> referencedDataFiles = Maps.newHashMap();

  private final Map<PartitionKey, RowDataDeltaPartitionedWriter> writers = Maps.newHashMap();

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final RowDataWrapper keyWrapper;
  private final RowDataProjection keyProjection;
  private final boolean upsert;

  BaseDeltaPartitionedTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<RowData> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.keyWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(deleteSchema), deleteSchema.asStruct());
    this.keyProjection = RowDataProjection.create(schema, deleteSchema);
    this.upsert = upsert;

    this.partitionKey = new PartitionKey(spec, schema);
  }

  RowDataDeltaPartitionedWriter route(RowData row) {
    partitionKey.partition(wrapper().wrap(row));

    RowDataDeltaPartitionedWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaPartitionedWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(RowData row) throws IOException {
    RowDataDeltaPartitionedWriter writer = route(row);

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        if (upsert) {
          writer.deleteKey(keyProjection.wrap(row));
        }
        writer.write(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break; // UPDATE_BEFORE is not necessary for UPSERT, we do nothing to prevent delete one
          // row twice
        }
        writer.delete(row);
        break;
      case DELETE:
        writer.delete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  @Override
  public WriteResult complete() throws IOException {

      close();

      Preconditions.checkState(failure == null, "Cannot return results from failed writer", failure);

    Set<PartitionKey> partitionKeys = completedDataFiles.keySet();
    // get partiton value, compare with watermark delay

    return WriteResult.builder()
          .addDataFiles(completedDataFiles)
          .addDeleteFiles(completedDeleteFiles)
          .addReferencedDataFiles(referencedDataFiles)
          .build();
  }

  @Override
  public void close() {
    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(RowDataDeltaPartitionedWriter.RowDataDeltaPartitionedWriter::close, IOException.class);

      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delta writer", e);
    }
  }

  protected class RowDataDeltaPartitionedWriter extends BaseEqualityDeltaWriter {

    RowDataDeltaPartitionedWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
      this.dataWriter = new PartitionedRollingFileWriter(partition);
      this.eqDeleteWriter = new PartitionedRollingEqDeleteWriter(partition);
    }

    @Override
    public void write(RowData row) throws IOException {
      super.write(row);
    }

    @Override
    protected StructLike asStructLike(RowData data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(RowData data) {
      return keyWrapper.wrap(data);
    }
  }

  protected class PartitionedRollingFileWriter extends RollingFileWriter {
    protected PartitionKey partitionKey;

    public PartitionedRollingFileWriter(StructLike partitionKey) {
      super(partitionKey);
      this.partitionKey = (PartitionKey) partitionKey;
    }

    @Override
    protected void complete(DataWriter<RowData> closedWriter) {
      completedDataFiles.compute(
          partitionKey,
          (partitionKey1, dataFiles) -> {
            if (dataFiles == null) {
              return Lists.newArrayList(closedWriter.toDataFile());
            } else {
              dataFiles.add(closedWriter.toDataFile());
              return dataFiles;
            }
          });
    }
  }

  protected class PartitionedRollingEqDeleteWriter extends RollingEqDeleteWriter {

    public PartitionedRollingEqDeleteWriter(StructLike partitionKey) {
      super(partitionKey);
    }

    @Override
    protected void complete(EqualityDeleteWriter<RowData> closedWriter) {
      completedDeleteFiles.compute(
          partitionKey,
          (partitionKey1, dataFiles) -> {
            if (dataFiles == null) {
              return Lists.newArrayList();
            } else {
              dataFiles.add(closedWriter.toDeleteFile());
              return dataFiles;
            }
          });
    }
  }
}
