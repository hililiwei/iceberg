/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.sink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;

public class FlinkSinkCommon {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSinkCommon.class);

  @VisibleForTesting
  public static List<Integer> checkAndGetEqualityFieldIds(
      Table table,
      List<String> equalityFieldColumns) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
    if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
      Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
      for (String column : equalityFieldColumns) {
        org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
        Preconditions.checkNotNull(field, "Missing required equality field column '%s' in table schema %s",
            column, table.schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
        LOG.warn("The configured equality field column IDs {} are not matched with the schema identifier field IDs" +
                " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet, table.schema().identifierFieldIds());
      }
      equalityFieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return equalityFieldIds;
  }

  public static void upsertCheck(Table table,List<String> equalityFieldColumns,

      List<Integer> equalityFieldIds,
      boolean upsertMode,
      boolean overwrite) {
    // Validate the equality fields and partition fields if we enable the upsert mode.
    if (upsertMode) {
      Preconditions.checkState(!overwrite,
          "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
      Preconditions.checkState(!equalityFieldIds.isEmpty(),
          "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
      if (!table.spec().isUnpartitioned()) {
        for (PartitionField partitionField : table.spec().fields()) {
          Preconditions.checkState(
              equalityFieldIds.contains(partitionField.sourceId()),
              "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
              partitionField, equalityFieldColumns);
        }
      }
    }
  }

  public static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
      TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
      // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
      // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
      // schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  public static DataStream<RowData> distributeDataStream(DataStream<RowData> input,
      Map<String, String> properties,
      List<Integer> equalityFieldIds,
      PartitionSpec partitionSpec,
      Schema iSchema,
      RowType flinkRowType,
      DistributionMode distributionMode,
      List<String> equalityFieldColumns) {
    DistributionMode writeMode;
    if (distributionMode == null) {
      // Fallback to use distribution mode parsed from table properties if don't specify in job level.
      String modeName = PropertyUtil.propertyAsString(properties,
          WRITE_DISTRIBUTION_MODE,
          WRITE_DISTRIBUTION_MODE_NONE);

      writeMode = DistributionMode.fromName(modeName);
    } else {
      writeMode = distributionMode;
    }

    LOG.info("Write distribution mode is '{}'", writeMode.modeName());
    switch (writeMode) {
      case NONE:
        if (equalityFieldIds.isEmpty()) {
          return input;
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set");
          return input.keyBy(new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        }

      case HASH:
        if (equalityFieldIds.isEmpty()) {
          if (partitionSpec.isUnpartitioned()) {
            LOG.warn("Fallback to use 'none' distribution mode, because there are no equality fields set " +
                "and table is unpartitioned");
            return input;
          } else {
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }
        } else {
          if (partitionSpec.isUnpartitioned()) {
            LOG.info("Distribute rows by equality fields, because there are equality fields set " +
                "and table is unpartitioned");
            return input.keyBy(new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
          } else {
            for (PartitionField partitionField : partitionSpec.fields()) {
              Preconditions.checkState(equalityFieldIds.contains(partitionField.sourceId()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' " +
                      "should be included in equality fields: '%s'", partitionField, equalityFieldColumns);
            }
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }
        }

      case RANGE:
        if (equalityFieldIds.isEmpty()) {
          LOG.warn("Fallback to use 'none' distribution mode, because there are no equality fields set " +
              "and {}=range is not supported yet in flink", WRITE_DISTRIBUTION_MODE);
          return input;
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set " +
              "and{}=range is not supported yet in flink", WRITE_DISTRIBUTION_MODE);
          return input.keyBy(new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        }

      default:
        throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + writeMode);
    }
  }
}
