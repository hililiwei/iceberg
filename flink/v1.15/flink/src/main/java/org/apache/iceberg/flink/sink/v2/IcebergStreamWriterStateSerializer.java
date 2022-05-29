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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergStreamWriterStateSerializer<T> implements SimpleVersionedSerializer<IcebergStreamWriterState> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamWriterStateSerializer.class);

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(IcebergStreamWriterState icebergStreamWriterState) throws IOException {
    byte[] bytes;
    try {
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bo);
      oo.writeObject(icebergStreamWriterState);

      bytes = bo.toByteArray();

      bo.close();
      oo.close();
    } catch (Exception ae) {
      throw ae;
    }
    return bytes;
  }

  @Override
  public IcebergStreamWriterState deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        Object obj = null;
        try {

          // bytearray to object
          ByteArrayInputStream bi = new ByteArrayInputStream(serialized);
          ObjectInputStream oi = new ObjectInputStream(bi);

          obj = oi.readObject();

          bi.close();
          oi.close();
        } catch (ClassNotFoundException e) {
          LOG.error("writer state deserialize failed!", e);
        }
        return (IcebergStreamWriterState) obj;
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }
}
