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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.PartitionSpec;

@SuppressWarnings({"checkstyle:HideUtilityClassConstructor", "checkstyle:BanSystemOut"})
public class QuickstartExample {

  public static void main(String[] args) throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "level", Types.StringType.get()),
        Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
        Types.NestedField.required(3, "message", Types.StringType.get()),
        Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .hour("event_time")
        .identity("level")
        .build();

    File warehouse = File.createTempFile("warehouse", null);
    warehouse.delete();

    Configuration conf = new Configuration();
    String warehousePath = warehouse.getPath();
    HadoopCatalog catalog = new HadoopCatalog(conf, "file://" + warehousePath);
    TableIdentifier name = TableIdentifier.of("logging", "logs");
    Table table = catalog.createTable(name, schema, spec);

    final DataFile FILE_A = DataFiles.builder(spec)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("event_time_hour=10/level=ERROR")
        .withRecordCount(1)
        .build();

    table.newRowDelta()
        .addRows(FILE_A)
        .commit();
  }
}
