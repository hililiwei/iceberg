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

import java.util.Map;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class RowDataProjection implements RowData {
  private final FieldGetter[] getters;
  private final int[][] projectedFields;
  private final Map<Integer, Object> valueCache = Maps.newHashMap();
  private RowData rowData;

  /**
   * Projects a (possibly nested) row data type, converts the original field obtained from the file to the flink output.
   * This projection will not project the nested children types of repeated types like lists and maps, but does
   * project nested type, flattening it out.
   *
   * <p>
   * For example, original data with fields: {@code [id:Int,st:Struct[a:String,b:String] ...]}, Output
   * projection Field: {@code [id,st.a]} with path: [[0],[1,0]], For the {@code st}(nested type) field, {@code a} is
   * projected by taking it out of the nested body as an independent field.
   *
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @param projectedFields result fields index of the projected rows
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(Schema schema, Schema projectedSchema, int[][] projectedFields) {
    return new RowDataProjection(FlinkSchemaUtil.convert(schema), schema.asStruct(), projectedSchema.asStruct(),
        projectedFields);
  }

  /**
   * Projects a (possibly nested) row data type, converts the original field obtained from the file to the flink output.
   * This projection will not project the nested children types of repeated types like lists and maps, but does
   * project nested type, flattening it out.
   *
   * <p>
   * For example, original data with fields: {@code [id:Int,st:Struct[a:String,b:String] ...]}, Output
   * projection Field: {@code [id,st.a]} with path: [[0],[1,0]], For the {@code st}(nested type) field, {@code a} is
   * projected by taking it out of the nested body as an independent field.
   *
   * @param rowType origin rows type
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @param projectedFields result fields index of the projected rows
   * @param projectedField  field index of the projected fields
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(RowType rowType, Types.StructType schema, Types.StructType projectedSchema,
                                         int[][] projectedFields, int[] projectedField) {
    return new RowDataProjection(rowType, schema, projectedSchema, projectedFields, projectedField);
  }

  /**
   * Creates a projecting wrapper for {@link RowData} rows.
   * <p>
   * This projection will not project the nested children types of repeated types like lists and maps.
   *
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(Schema schema, Schema projectedSchema) {
    return RowDataProjection.create(FlinkSchemaUtil.convert(schema), schema.asStruct(), projectedSchema.asStruct());
  }

  /**
   * Creates a projecting wrapper for {@link RowData} rows.
   * <p>
   * This projection will not project the nested children types of repeated types like lists and maps.
   *
   * @param rowType flink row type of rows wrapped by this projection
   * @param schema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static RowDataProjection create(RowType rowType, Types.StructType schema, Types.StructType projectedSchema) {
    return new RowDataProjection(rowType, schema, projectedSchema);
  }

  private RowDataProjection(RowType rowType, Types.StructType schema, Types.StructType projectFieldsType,
                            int[][] newProjectedFields) {
    this.projectedFields = newProjectedFields;
    this.getters = new FieldGetter[projectedFields.length];

    for (int i = 0; i < getters.length; i++) {
      int[] projectedFieldOne = projectedFields[i];
      int fieldIndex = projectedFieldOne[0];
      Types.NestedField projectField = schema.fields().get(fieldIndex);
      Types.NestedField rowField = schema.field(projectField.fieldId());
      getters[i] = createFieldGetter(rowType, rowField, fieldIndex, projectFieldsType, projectField, projectedFieldOne);
    }
  }

  private RowDataProjection(RowType rowType, Types.StructType schema, Types.StructType projectFieldsType,
                            int[][] newProjectedFields, int[] projectedField) {
    this.projectedFields = newProjectedFields;
    this.getters = new FieldGetter[projectFieldsType.fields().size()];

    int fieldIndex = projectedField[0];
    Types.NestedField projectField = projectFieldsType.fields().get(fieldIndex);
    Types.NestedField rowField = schema.field(projectField.fieldId());
    getters[fieldIndex] =
        createFieldGetter(rowType, rowField, fieldIndex, projectFieldsType, projectField, projectedField);
  }

  private RowDataProjection(RowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
    this.projectedFields = null;

    Map<Integer, Integer> fieldIdToPosition = Maps.newHashMap();
    for (int i = 0; i < rowStruct.fields().size(); i++) {
      fieldIdToPosition.put(rowStruct.fields().get(i).fieldId(), i);
    }

    this.getters = new RowData.FieldGetter[projectType.fields().size()];
    for (int i = 0; i < getters.length; i++) {
      Types.NestedField projectField = projectType.fields().get(i);
      Types.NestedField rowField = rowStruct.field(projectField.fieldId());

      Preconditions.checkNotNull(rowField,
          "Cannot locate the project field <%s> in the iceberg struct <%s>", projectField, rowStruct);

      getters[i] = createFieldGetter(rowType, fieldIdToPosition.get(projectField.fieldId()), rowField, projectField);
    }
  }

  private static FieldGetter createFieldGetter(RowType rowType,
                                               Types.NestedField rowField,
                                               int rowTypePosition,
                                               Types.StructType projectFieldsType,
                                               Types.NestedField projectField,
                                               int[] projectedField) {
    switch (projectField.type().typeId()) {
      case STRUCT:
        if (projectedField.length <= 1) {
          return RowData.createFieldGetter(rowType.getTypeAt(rowTypePosition),
              projectFieldsType.fields().indexOf(projectField));
        }

        RowType nestedRowType = (RowType) rowType.getTypeAt(rowTypePosition);

        int[] projectedFieldPath = new int[projectedField.length - 1];
        System.arraycopy(projectedField, 1, projectedFieldPath, 0, projectedFieldPath.length);
        int[][] projectedFieldPaths = new int[][] {projectedFieldPath};

        int rowIndex = projectFieldsType.fields().indexOf(projectField);

        return row -> {
          if (row.getClass().equals(RowDataProjection.class)) {
            return ((RowDataProjection) row).getValue(rowIndex);
          }

          RowData nestedRow = rowIndex < 0 ? null : row.getRow(rowIndex, nestedRowType.getFieldCount());
          return RowDataProjection
              .create(nestedRowType, rowField.type().asStructType(),
                  projectField.type().asStructType(), projectedFieldPaths, projectedFieldPath)
              .wrap(nestedRow);
        };

      case MAP:
        checkRowAndProjectMap(projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));

      case LIST:
        checkRowAndProjectList(projectField, rowField);

        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));

      default:
        return RowData.createFieldGetter(
            rowType.getTypeAt(rowTypePosition),
            projectFieldsType.fields().indexOf(projectField));
    }
  }

  private static RowData.FieldGetter createFieldGetter(RowType rowType, int position, Types.NestedField rowField,
                                                       Types.NestedField projectField) {
    Preconditions.checkArgument(rowField.type().typeId() == projectField.type().typeId(),
        "Different iceberg type between row field <%s> and project field <%s>", rowField, projectField);

    switch (projectField.type().typeId()) {
      case STRUCT:
        RowType nestedRowType = (RowType) rowType.getTypeAt(position);
        return row -> {
          RowData nestedRow = row.isNullAt(position) ? null : row.getRow(position, nestedRowType.getFieldCount());
          return RowDataProjection
              .create(nestedRowType, rowField.type().asStructType(), projectField.type().asStructType())
              .wrap(nestedRow);
        };

      case MAP:
        checkRowAndProjectMap(projectField, rowField);

        return RowData.createFieldGetter(rowType.getTypeAt(position), position);

      case LIST:
        checkRowAndProjectList(projectField, rowField);

        return RowData.createFieldGetter(rowType.getTypeAt(position), position);

      default:
        return RowData.createFieldGetter(rowType.getTypeAt(position), position);
    }
  }

  public RowData wrap(RowData row) {
    this.rowData = row;
    this.valueCache.clear();
    return this;
  }

  private Object getValue(int pos) {
    return valueCache.computeIfAbsent(pos, key -> {
      Object value = getters[key].getFieldOrNull(rowData);
      while (value != null && projectedFields != null && value.getClass().equals(RowDataProjection.class)) {
        RowDataProjection rowDataNest = (RowDataProjection) value;
        value = rowDataNest.getters[rowDataNest.projectedFields[0][0]].getFieldOrNull(rowDataNest);
      }
      return value;
    });
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

  private static void checkRowAndProjectList(Types.NestedField projectField, Types.NestedField rowField) {
    Types.ListType projectedList = projectField.type().asListType();
    Types.ListType originalList = rowField.type().asListType();

    boolean elementProjectable = !projectedList.elementType().isNestedType() ||
        projectedList.elementType().equals(originalList.elementType());
    Preconditions.checkArgument(elementProjectable,
        "Cannot project a partial list element with non-primitive type. Trying to project <%s> out of <%s>",
        projectField, rowField);
  }

  private static void checkRowAndProjectMap(Types.NestedField projectField, Types.NestedField rowField) {
    Types.MapType projectedMap = projectField.type().asMapType();
    Types.MapType originalMap = rowField.type().asMapType();

    boolean keyProjectable = !projectedMap.keyType().isNestedType() ||
        projectedMap.keyType().equals(originalMap.keyType());
    boolean valueProjectable = !projectedMap.valueType().isNestedType() ||
        projectedMap.valueType().equals(originalMap.valueType());
    Preconditions.checkArgument(keyProjectable && valueProjectable,
        "Cannot project a partial map key or value with non-primitive type. Trying to project <%s> out of <%s>",
        projectField, rowField);
  }
}
