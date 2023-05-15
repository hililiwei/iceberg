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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class DeltaManifestsSerializer implements SimpleVersionedSerializer<DeltaManifests> {
  private static final int VERSION_1 = 1;
  private static final int VERSION_2 = 2;
  private static final int VERSION_3 = 3;
  private static final byte[] EMPTY_BINARY = new byte[0];

  static final DeltaManifestsSerializer INSTANCE = new DeltaManifestsSerializer();

  @Override
  public int getVersion() {
    return VERSION_3;
  }

  @Override
  public byte[] serialize(DeltaManifests deltaManifests) throws IOException {
    Preconditions.checkNotNull(
        deltaManifests, "DeltaManifests to be serialized should not be null");

    ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(binaryOut);

    byte[] dataManifestBinary = EMPTY_BINARY;
    if (deltaManifests.dataManifest() != null) {
      dataManifestBinary = ManifestFiles.encode(deltaManifests.dataManifest());
    }

    out.writeInt(dataManifestBinary.length);
    out.write(dataManifestBinary);

    byte[] deleteManifestBinary = EMPTY_BINARY;
    if (deltaManifests.deleteManifest() != null) {
      deleteManifestBinary = ManifestFiles.encode(deltaManifests.deleteManifest());
    }

    out.writeInt(deleteManifestBinary.length);
    out.write(deleteManifestBinary);

    CharSequence[] referencedDataFiles = deltaManifests.referencedDataFiles();
    out.writeInt(referencedDataFiles.length);
    for (CharSequence referencedDataFile : referencedDataFiles) {
      out.writeUTF(referencedDataFile.toString());
    }

    byte[] partitionKeys;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
        objectOutputStream.writeObject(deltaManifests.partitionKey());
      }
      partitionKeys = byteArrayOutputStream.toByteArray();
    }

    out.writeInt(partitionKeys.length);
    out.write(partitionKeys);

    return binaryOut.toByteArray();
  }

  @Override
  public DeltaManifests deserialize(int version, byte[] serialized) throws IOException {
    if (version == VERSION_1) {
      return deserializeV1(serialized);
    } else if (version == VERSION_2) {
      return deserializeV2(serialized);
    } else if (version == VERSION_3) {
      return deserializeV3(serialized);
    } else {
      throw new RuntimeException("Unknown serialize version: " + version);
    }
  }

  private DeltaManifests deserializeV1(byte[] serialized) throws IOException {
    return new DeltaManifests(ManifestFiles.decode(serialized), null);
  }

  private DeltaManifests deserializeV2(byte[] serialized) throws IOException {
    ByteArrayInputStream binaryIn = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(binaryIn);

    ManifestFile dataManifest = deserializeManifestFile(in);
    ManifestFile deleteManifest = deserializeDeleteManifestFile(in);
    CharSequence[] referencedDataFiles = deserializeReferencedDataFiles(in);

    return new DeltaManifests(dataManifest, deleteManifest, referencedDataFiles);
  }

  private DeltaManifests deserializeV3(byte[] serialized) throws IOException {
    ByteArrayInputStream binaryIn = new ByteArrayInputStream(serialized);
    DataInputStream in = new DataInputStream(binaryIn);

    ManifestFile dataManifest = deserializeManifestFile(in);
    ManifestFile deleteManifest = deserializeDeleteManifestFile(in);
    CharSequence[] referencedDataFiles = deserializeReferencedDataFiles(in);
    PartitionKey partitionKey = deserializePartitionKey(in);

    return new DeltaManifests(dataManifest, deleteManifest, referencedDataFiles, partitionKey);
  }

  private static CharSequence[] deserializeReferencedDataFiles(DataInputStream in)
      throws IOException {
    int referenceDataFileNum = in.readInt();
    CharSequence[] referencedDataFiles = new CharSequence[referenceDataFileNum];
    for (int i = 0; i < referenceDataFileNum; i++) {
      referencedDataFiles[i] = in.readUTF();
    }

    return referencedDataFiles;
  }

  private static ManifestFile deserializeDeleteManifestFile(DataInputStream in) throws IOException {
    int deleteManifestSize = in.readInt();
    if (deleteManifestSize > 0) {
      byte[] deleteManifestBinary = new byte[deleteManifestSize];
      Preconditions.checkState(in.read(deleteManifestBinary) == deleteManifestSize);

      return ManifestFiles.decode(deleteManifestBinary);
    }

    return null;
  }

  private static ManifestFile deserializeManifestFile(DataInputStream in) throws IOException {
    int dataManifestSize = in.readInt();
    if (dataManifestSize > 0) {
      byte[] dataManifestBinary = new byte[dataManifestSize];
      Preconditions.checkState(in.read(dataManifestBinary) == dataManifestSize);

      return ManifestFiles.decode(dataManifestBinary);
    }

    return null;
  }

  private static PartitionKey deserializePartitionKey(DataInputStream in) throws IOException {
    int partitionKeyLength = in.readInt();
    byte[] bytes = new byte[partitionKeyLength];
    in.readFully(bytes);
    PartitionKey partitionKey;
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
        partitionKey = (PartitionKey) objectInputStream.readObject();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to deserialize partition key", e);
      }
    }

    return partitionKey;
  }
}
