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

package org.apache.flink.table.connector.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

/**
 * Provider that consumes a Java {@link DataStream} as a runtime implementation for {@link
 * DynamicTableSink}.
 *
 * <p>Note: This provider is only meant for advanced connector developers. Usually, a sink should
 * consist of a single entity expressed via {@link OutputFormatProvider} or {@link
 * SinkFunctionProvider}.
 */
public interface DataStreamSinkProvider extends DynamicTableSink.SinkRuntimeProvider {

  /**
   * Consumes the given Java {@link DataStream} and returns the sink transformation {@link
   * DataStreamSink}.
   */
  DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream);
}
