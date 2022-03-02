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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Projects a (possibly nested) row data type, converts the original field obtained from the file to the flink output.
 *
 * <p>For example, original data with fields: {@code [id:Int,st:Struct[a:String,b:String] ...]}, Output
 * projection Field: {@code [id,st.a]} with path: [[0],[1,0]], For the {@code st}(nested type) field, {@code a} is
 * projected by taking it out of the nested body as an independent field.
 */
public class RowDataNestProjection implements RowData {
  private final FieldGetter[] getters;
  private final int[][] projectedFields;
  private RowData rowData;

  private RowDataNestProjection(
      RowType rowType,
      Types.StructType schema,
      Types.StructType rowStruct,
      Types.StructType projectType,
      int[][] projectedFields) {
    this.projectedFields = projectedFields;

    this.getters = new FieldGetter[projectedFields.length];
    for (int i = 0; i < getters.length; i++) {
      int[] projectedFieldOne = projectedFields[i];
      int fieldIndex = projectedFieldOne[0];

      Types.NestedField projectField = rowStruct.fields().get(fieldIndex);
      Types.NestedField rowField = schema.field(projectField.fieldId());

      getters[i] = createFieldGetter(rowType, rowField, fieldIndex, projectType, projectField, i, projectedFields,
          projectedFieldOne);
    }
  }

  private RowDataNestProjection(
      RowType rowType,
      Types.StructType schema,
      Types.StructType projectType,
      int[][] projectedFields) {
    this.projectedFields = projectedFields;

    this.getters = new FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      int[] projectedFieldOne = projectedFields[0];
      int fieldIndex = projectedFieldOne[0];
      if (i == fieldIndex) {
        Types.NestedField projectField = projectType.fields().get(i);
        Types.NestedField rowField = schema.field(projectField.fieldId());

        getters[i] = createFieldGetter(rowType, rowField, i, projectType, projectField, i, projectedFields,
            projectedFieldOne);
      }
    }
  }

  private static FieldGetter createFieldGetter(
      RowType rowType,
      Types.NestedField rowField,
      int rowTypePosition,
      Types.StructType projectFieldsType,
      Types.NestedField projectField,
      int projectFieldPosition,
      int[][] projectedFields,
      int[] projectedFieldOne) {
    switch (projectField.type().typeId()) {
      case STRUCT:
        if (projectedFields == null || projectedFields[projectFieldPosition].length <= 1) {
          return RowData.createFieldGetter(
              rowType.getTypeAt(rowTypePosition),
              projectFieldsType.fields().indexOf(projectField));
        }
        RowType nestedRowType = (RowType) rowType.getTypeAt(rowTypePosition);
        int[] target = new int[projectedFieldOne.length - 1];
        System.arraycopy(projectedFieldOne, 1, target, 0, target.length);
        int[][] temp = {target};
        int rowIndex = projectFieldsType.fields().indexOf(projectField);
        return row -> {
          RowData nestedRow = rowIndex < 0 ? null : row.getRow(rowIndex, nestedRowType.getFieldCount());
          return RowDataNestProjection
              .create(nestedRowType, rowField.type().asStructType(), projectField.type().asStructType(), temp)
              .wrap(nestedRow);
        };
      case MAP:
        Types.MapType projectedMap = projectField.type().asMapType();
        Types.MapType originalMap = rowField.type().asMapType();

        boolean keyProjectable = !projectedMap.keyType().isNestedType() ||
            projectedMap.keyType().equals(originalMap.keyType());
        boolean valueProjectable = !projectedMap.valueType().isNestedType() ||
            projectedMap.valueType().equals(originalMap.valueType());
        Preconditions.checkArgument(keyProjectable && valueProjectable,
            "Cannot project a partial map key or value with non-primitive type. Trying to project <%s> out of <%s>",
            projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));

      case LIST:
        Types.ListType projectedList = projectField.type().asListType();
        Types.ListType originalList = rowField.type().asListType();

        boolean elementProjectable = !projectedList.elementType().isNestedType() ||
            projectedList.elementType().equals(originalList.elementType());
        Preconditions.checkArgument(elementProjectable,
            "Cannot project a partial list element with non-primitive type. Trying to project <%s> out of <%s>",
            projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));
      default:
        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));
    }
  }

  public static RowDataNestProjection create(Schema schema, Schema projectedSchema, int[][] projectedFields) {
    return RowDataNestProjection.create(FlinkSchemaUtil.convert(schema), schema.asStruct(), schema.asStruct(),
        projectedSchema.asStruct(),
        projectedFields);
  }

  public static RowDataNestProjection create(
      RowType rowType,
      Types.StructType schema,
      Types.StructType rowStructType,
      Types.StructType projectedSchema,
      int[][] projectedFields) {
    return new RowDataNestProjection(rowType, schema, rowStructType, projectedSchema, projectedFields);
  }

  public static RowDataNestProjection create(
      RowType rowType, Types.StructType schema, Types.StructType projectedSchema,
      int[][] projectedFields) {
    return new RowDataNestProjection(rowType, schema, projectedSchema, projectedFields);
  }

  public RowData wrap(RowData row) {
    this.rowData = row;
    return this;
  }

  private Object getValue(int pos) {
    Object fieldValue = getters[pos].getFieldOrNull(rowData);
    while (fieldValue != null && fieldValue.getClass().equals(RowDataNestProjection.class)) {
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
    throw new UnsupportedOperationException("Cannot set row kind in the RowDataNestProjection");
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
