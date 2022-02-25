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

package org.apache.iceberg.example.scala.flink

import java.io.File
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment
import org.apache.iceberg.FileFormat

object FlinkExample {
  private val CATALOG_NAME = "test_catalog"
  private val DATABASE_NAME = "test_db"
  private val TABLE_NAME = "test_table"
  private val TABLE_NAME2 = "test_table2"

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inBatchMode
      .build

    val env = TableEnvironment.create(settings)
    env.getConfig.getConfiguration.setBoolean("table.exec.iceberg.infer-source-parallelism", false)
    val tEnv = env

    val warehouseFile = File.createTempFile("warehouse", null)
    warehouseFile.delete
    val warehouse = args(0)

    tEnv.executeSql(
      s"""
         |CREATE CATALOG $CATALOG_NAME
         |WITH (
         | 'type'='iceberg',
         | 'catalog-type'='hadoop',
         | 'warehouse'='$warehouse')
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |USE CATALOG $CATALOG_NAME
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE DATABASE $DATABASE_NAME
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |USE $DATABASE_NAME
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE $TABLE_NAME
         |  (id INT,
         |   data VARCHAR,
         |   d DOUBLE)
         |PARTITIONED BY (d)
         |WITH (
         |  'write.format.default'='${FileFormat.AVRO.name}')
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE $TABLE_NAME2 LIKE $TABLE_NAME
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |ALTER TABLE $TABLE_NAME2 SET ('write.format.default'='${FileFormat.PARQUET.name}')
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |SHOW TABLES
         |""".stripMargin).print()

    tEnv.executeSql(
      s"""
         |INSERT INTO $TABLE_NAME
         |VALUES
         |  (1,'iceberg',10),
         |  (2,'b',20),
         |  (3,CAST(NULL AS VARCHAR),30)
         |""".stripMargin)
    tEnv.sqlQuery(
      s"""
         |SELECT * FROM  $TABLE_NAME
         |""".stripMargin)
      .execute()
      .print()

  }
}
