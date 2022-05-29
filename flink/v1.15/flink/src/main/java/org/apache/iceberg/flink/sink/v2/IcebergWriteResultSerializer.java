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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.SerializationUtil;

public class IcebergWriteResultSerializer implements SimpleVersionedSerializer<IcebergFlinkCommittable> {

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(IcebergFlinkCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    byte[] serialize = writeResultSerializer.serialize(committable.committable());
    view.writeUTF(committable.jobID());
    view.writeLong(committable.checkpointId());
    view.writeInt(committable.subtaskId());
    view.writeInt(serialize.length);
    view.write(serialize);
    return out.toByteArray();
  }

  @Override
  public IcebergFlinkCommittable deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        String jobID = view.readUTF();
        long checkpointId = view.readLong();
        int subtaskId = view.readInt();
        int len = view.readInt();
        byte[] buf = new byte[len];
        view.read(buf);
        WriteResult writeResult = writeResultSerializer.deserialize(writeResultSerializer.getVersion(), buf);
        return new IcebergFlinkCommittable(writeResult, jobID, checkpointId, subtaskId);
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }

  private final SimpleVersionedSerializer<WriteResult> writeResultSerializer =
      new SimpleVersionedSerializer<WriteResult>() {
        @Override
        public int getVersion() {
          return 1;
        }

        @Override
        public byte[] serialize(WriteResult obj) throws IOException {
          return SerializationUtil.serializeToBytes(obj);
        }

        @Override
        public WriteResult deserialize(int version, byte[] serialized) throws IOException {
          return SerializationUtil.deserializeFromBytes(serialized);
        }
      };
}
