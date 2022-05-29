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

public class IcebergWriteResultSerializer implements SimpleVersionedSerializer<IcebergFlinkCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteResultSerializer.class);

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(IcebergFlinkCommittable committable) throws IOException {
    byte[] bytes;
    try {
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bo);
      oo.writeObject(committable);

      bytes = bo.toByteArray();

      bo.close();
      oo.close();
    } catch (IOException e) {
      throw e;
    }
    return bytes;
  }

  @Override
  public IcebergFlinkCommittable deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        java.lang.Object obj = null;
        try {

          // bytearray to object
          ByteArrayInputStream bi = new ByteArrayInputStream(serialized);
          ObjectInputStream oi = new ObjectInputStream(bi);

          obj = oi.readObject();

          bi.close();
          oi.close();
        } catch (ClassNotFoundException e) {
          LOG.error("Committable deserialize failed!", e);
        }
        return (IcebergFlinkCommittable) obj;
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }
}
