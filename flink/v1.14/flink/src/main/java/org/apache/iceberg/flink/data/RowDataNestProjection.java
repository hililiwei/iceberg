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

package org.apache.iceberg.flink.data;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;

public class RowDataNestProjection implements RowData {
  private final FieldGetter[] getters;
  private final boolean nestFlat;
  int[][] projectedFields;
  private RowData rowData;

  private RowDataNestProjection(
      RowType rowType,// 嵌套类型上层文件类型，每次不同
      Types.StructType rowStruct,// 嵌套类型上层文件类型，每次不同
      Types.StructType projectType, // 这里是个嵌套类型，每次不同
      int[][] projectedFields, // 这里是打平的字段路径，每次不同
      boolean nestFlat) { // 这里是总出口，每次都相同，就是返回值的类型
    this.projectedFields = projectedFields;
    this.nestFlat = nestFlat;

    this.getters = new FieldGetter[projectedFields.length];
    for (int i = 0; i < getters.length; i++) {
      int[] projectedFieldOne = projectedFields[i];
      int fieldIndex = projectedFieldOne[0];

      Types.NestedField projectField = rowStruct.fields().get(fieldIndex);

      getters[i] = createFieldGetter(rowType, fieldIndex, projectType, projectField,
          i, projectedFields, projectedFieldOne);
    }
  }

  private RowDataNestProjection(
      RowType rowType,// 嵌套类型上层文件类型，每次不同
      Types.StructType projectType, // 这里是个嵌套类型，每次不同
      int[][] projectedFields, // 这里是打平的字段路径，每次不同
      boolean nestFlat) {
    this.projectedFields = projectedFields;
    this.nestFlat = nestFlat;

    this.getters = new FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      int[] projectedFieldOne = projectedFields[0];
      int fieldIndex = projectedFieldOne[0];
      if (i == fieldIndex) {
        Types.NestedField projectField = projectType.fields().get(i);

        getters[i] = createFieldGetter(rowType, i, projectType, projectField,
            i, projectedFields, projectedFieldOne);
      }
    }
  }

  private static FieldGetter createFieldGetter(
      RowType rowType, // 上层RowType，每次不同
      int rowTypePosition, // 字段在rowStruct的下标
      Types.StructType projectType, // 当前投影,这是真正取出的数据类型
      Types.NestedField projectField, // 当前字段
      int projectFieldIndex, // 在projectedFields中的下标路径
      int[][] projectedFields,// 这里是打平的字段路径，每次不同
      int[] projectedFieldOne
  ) {
    switch (projectField.type().typeId()) {
      case STRUCT:
        RowType nestedRowType = (RowType) rowType.getTypeAt(rowTypePosition);

        if (projectedFields == null || projectedFields[projectFieldIndex].length <= 1) {
          return RowData.createFieldGetter(
              rowType.getTypeAt(rowTypePosition),
              projectType.fields().indexOf(projectField));
        }

        return row -> {
          int fieldCount = nestedRowType.getFieldCount();

          int[] target = new int[projectedFieldOne.length - 1];
          System.arraycopy(projectedFieldOne, 1, target, 0, target.length);
          int[][] temp = {target};

          int rowIndex = projectType.fields().indexOf(projectField);
          RowData nestedRow = rowIndex < 0 ? null : row.getRow(rowIndex, fieldCount);

          return RowDataNestProjection
              .create(nestedRowType, projectField.type().asStructType(), temp, true)
              .wrap(nestedRow);
        };
      case MAP:
        // Types.MapType projectedMap = projectField.type().asMapType();
        // Types.MapType originalMap = rowField.type().asMapType();
        //
        // boolean keyProjectable = !projectedMap.keyType().isNestedType() ||
        //     projectedMap.keyType().equals(originalMap.keyType());
        // boolean valueProjectable = !projectedMap.valueType().isNestedType() ||
        //     projectedMap.valueType().equals(originalMap.valueType());
        // Preconditions.checkArgument(keyProjectable && valueProjectable,
        //     "Cannot project a partial map key or value with non-primitive type. Trying to project <%s> out of <%s>",
        //     projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectType.fields().indexOf(projectField));

      case LIST:
        // Types.ListType projectedList = projectField.type().asListType();
        // // Types.ListType originalList = rowField.type().asListType();
        //
        // boolean elementProjectable = !projectedList.elementType().isNestedType() ||
        //     projectedList.elementType().equals(originalList.elementType());
        // Preconditions.checkArgument(elementProjectable,
        //     "Cannot project a partial list element with non-primitive type. Trying to project <%s> out of <%s>",
        //     projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectType.fields().indexOf(projectField));
      default:
        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectType.fields().indexOf(projectField));
    }
  }

  public static RowDataNestProjection create(
      Schema schema,
      Schema projectedSchema,
      int[][] projectedFields) {
    return RowDataNestProjection.create(FlinkSchemaUtil.convert(schema), schema.asStruct(), projectedSchema.asStruct(),
        projectedFields, true);
  }

  public static RowDataNestProjection create(
      RowType rowType, Types.StructType schema,
      Types.StructType projectedSchema, int[][] projectedFields, boolean nestFlat) {
    return new RowDataNestProjection(rowType, schema, projectedSchema, projectedFields, nestFlat);
  }

  public static RowDataNestProjection create(
      RowType rowType, Types.StructType projectedSchema,
      int[][] projectedFields, boolean nestFlat) {
    return new RowDataNestProjection(rowType, projectedSchema, projectedFields, nestFlat);
  }

  public RowData wrap(RowData row) {
    this.rowData = row;
    return this;
  }

  private Object getValue(int pos) {
    Object fieldValue = getters[pos].getFieldOrNull(rowData);
    while (nestFlat && fieldValue != null && fieldValue.getClass().equals(RowDataNestProjection.class)) {
      RowDataNestProjection rowDataNest = (RowDataNestProjection) fieldValue;
      fieldValue = rowDataNest.getters[rowDataNest.projectedFields[0][0]].getFieldOrNull(rowDataNest);
    }
    return fieldValue;
  }

  @Override
  public int getArity() {
    return getters.length;
  }

  @Override
  public RowKind getRowKind() {
    return rowData.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    throw new UnsupportedOperationException("Cannot set row kind in the RowDataProjection");
  }

  @Override
  public boolean isNullAt(int pos) {
    return rowData == null || getValue(pos) == null;
  }

  @Override
  public boolean getBoolean(int pos) {
    return (boolean) getValue(pos);
  }

  @Override
  public byte getByte(int pos) {
    return (byte) getValue(pos);
  }

  @Override
  public short getShort(int pos) {
    return (short) getValue(pos);
  }

  @Override
  public int getInt(int pos) {
    return (int) getValue(pos);
  }

  @Override
  public long getLong(int pos) {
    return (long) getValue(pos);
  }

  @Override
  public float getFloat(int pos) {
    return (float) getValue(pos);
  }

  @Override
  public double getDouble(int pos) {
    return (double) getValue(pos);
  }

  @Override
  public StringData getString(int pos) {
    return (StringData) getValue(pos);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return (DecimalData) getValue(pos);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return (TimestampData) getValue(pos);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> RawValueData<T> getRawValue(int pos) {
    return (RawValueData<T>) getValue(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return (byte[]) getValue(pos);
  }

  @Override
  public ArrayData getArray(int pos) {
    return (ArrayData) getValue(pos);
  }

  @Override
  public MapData getMap(int pos) {
    return (MapData) getValue(pos);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return (RowData) getValue(pos);
  }
}
