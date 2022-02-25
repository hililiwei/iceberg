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

package org.apache.iceberg.example;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public class ExampleUtil {

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.required(
          1,
          "level",
          Types.StringType.get()),
      Types.NestedField.required(
          2,
          "event_time",
          Types.TimestampType.withZone()),
      Types.NestedField.required(
          3,
          "message",
          Types.StringType.get())
  );

  public static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
      .hour("event_time")
      .identity("level")
      .build();

  public static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("logging", "logs");
}
