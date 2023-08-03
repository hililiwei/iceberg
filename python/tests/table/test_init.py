# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint:disable=redefined-outer-name
from typing import Any, Dict

import pytest
from sortedcontainers import SortedList

from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    In,
)
from pyiceberg.io import PY_IO_IMPL, load_file_io
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    StaticTable,
    Table,
    _match_deletes_to_datafile,
    _SchemaUpdate,
)
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER, TableMetadataV2
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import (
    PRIMITIVE_TYPES,
    BooleanType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


@pytest.fixture
def table(example_table_metadata_v2: Dict[str, Any]) -> Table:
    table_metadata = TableMetadataV2(**example_table_metadata_v2)
    return Table(
        identifier=("database", "table"),
        metadata=table_metadata,
        metadata_location=f"{table_metadata.location}/uuid.metadata.json",
        io=load_file_io(),
        catalog=NoopCatalog("NoopCatalog"),
    )


def test_schema(table: Table) -> None:
    assert table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_schemas(table: Table) -> None:
    assert table.schemas() == {
        0: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            schema_id=0,
            identifier_field_ids=[],
        ),
        1: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            schema_id=1,
            identifier_field_ids=[1, 2],
        ),
    }


def test_spec(table: Table) -> None:
    assert table.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0
    )


def test_specs(table: Table) -> None:
    assert table.specs() == {
        0: PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
    }


def test_sort_order(table: Table) -> None:
    assert table.sort_order() == SortOrder(
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        SortField(
            source_id=3,
            transform=BucketTransform(num_buckets=4),
            direction=SortDirection.DESC,
            null_order=NullOrder.NULLS_LAST,
        ),
        order_id=3,
    )


def test_sort_orders(table: Table) -> None:
    assert table.sort_orders() == {
        3: SortOrder(
            SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
            SortField(
                source_id=3,
                transform=BucketTransform(num_buckets=4),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST,
            ),
            order_id=3,
        )
    }


def test_location(table: Table) -> None:
    assert table.location() == "s3://bucket/test/location"


def test_current_snapshot(table: Table) -> None:
    assert table.current_snapshot() == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id(table: Table) -> None:
    assert table.snapshot_by_id(3055729675574597004) == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id_does_not_exist(table: Table) -> None:
    assert table.snapshot_by_id(-1) is None


def test_snapshot_by_name(table: Table) -> None:
    assert table.snapshot_by_name("test") == Snapshot(
        snapshot_id=3051729675574597004,
        parent_snapshot_id=None,
        sequence_number=0,
        timestamp_ms=1515100955770,
        manifest_list="s3://a/b/1.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=None,
    )


def test_snapshot_by_name_does_not_exist(table: Table) -> None:
    assert table.snapshot_by_name("doesnotexist") is None


def test_history(table: Table) -> None:
    assert table.history() == [
        SnapshotLogEntry(snapshot_id=3051729675574597004, timestamp_ms=1515100955770),
        SnapshotLogEntry(snapshot_id=3055729675574597004, timestamp_ms=1555100955770),
    ]


def test_table_scan_select(table: Table) -> None:
    scan = table.scan()
    assert scan.selected_fields == ("*",)
    assert scan.select("a", "b").selected_fields == ("a", "b")
    assert scan.select("a", "c").select("a").selected_fields == ("a",)


def test_table_scan_row_filter(table: Table) -> None:
    scan = table.scan()
    assert scan.row_filter == AlwaysTrue()
    assert scan.filter(EqualTo("x", 10)).row_filter == EqualTo("x", 10)
    assert scan.filter(EqualTo("x", 10)).filter(In("y", (10, 11))).row_filter == And(EqualTo("x", 10), In("y", (10, 11)))


def test_table_scan_ref(table: Table) -> None:
    scan = table.scan()
    assert scan.use_ref("test").snapshot_id == 3051729675574597004


def test_table_scan_ref_does_not_exists(table: Table) -> None:
    scan = table.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.use_ref("boom")

    assert "Cannot scan unknown ref=boom" in str(exc_info.value)


def test_table_scan_projection_full_schema(table: Table) -> None:
    scan = table.scan()
    assert scan.select("x", "y", "z").projection() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_table_scan_projection_single_column(table: Table) -> None:
    scan = table.scan()
    assert scan.select("y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_table_scan_projection_single_column_case_sensitive(table: Table) -> None:
    scan = table.scan()
    assert scan.with_case_sensitive(False).select("Y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_table_scan_projection_unknown_column(table: Table) -> None:
    scan = table.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.select("a").projection()

    assert "Could not find column: 'a'" in str(exc_info.value)


def test_static_table_same_as_table(table: Table, metadata_location: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table.metadata


def test_static_table_gz_same_as_table(table: Table, metadata_location_gz: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location_gz)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table.metadata


def test_static_table_io_does_not_exist(metadata_location: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_metadata(metadata_location, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_match_deletes_to_datafile() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=0,  # Older than the data
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_2.data_file,
    }


def test_match_deletes_to_datafile_duplicate_number() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_1.data_file,
        delete_entry_2.data_file,
    }


def test_add_column(table_schema_simple: Schema) -> None:
    update = _SchemaUpdate(table_schema_simple)
    update.add_column(name="b", type_var=IntegerType())
    apply_schema: Schema = update.apply()
    assert len(apply_schema.fields) == 4

    assert apply_schema == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="b", field_type=IntegerType(), required=False),
    )
    assert apply_schema.schema_id == 0
    assert apply_schema.highest_field_id == 4


def test_add_primitive_type_column(table_schema_simple: Schema) -> None:
    for name, type_ in PRIMITIVE_TYPES.items():
        field_name = f"new_column_{name}"
        update = _SchemaUpdate(table_schema_simple)
        update.add_column(parent=None, name=field_name, type_var=type_, doc=f"new_column_{name}")
        new_schema = update.apply()

        field: NestedField = new_schema.find_field(field_name)
        assert field.field_type == type_
        assert field.doc == f"new_column_{name}"


def test_add_nested_type_column(table_schema_simple: Schema) -> None:
    # add struct type column
    field_name = "new_column_struct"
    update = _SchemaUpdate(table_schema_simple, last_column_id=table_schema_simple.highest_field_id)
    struct_ = StructType(
        NestedField(1, "lat", DoubleType()),
        NestedField(2, "long", DoubleType()),
    )
    update.add_column(parent=None, name=field_name, type_var=struct_)
    schema_ = update.apply()
    field: NestedField = schema_.find_field(field_name)
    assert field.field_type == StructType(
        NestedField(5, "lat", DoubleType()),
        NestedField(6, "long", DoubleType()),
    )
    assert schema_.highest_field_id == 6


def test_add_nested_map_type_column(table_schema_simple: Schema) -> None:
    # add map type column
    field_name = "new_column_map"
    update = _SchemaUpdate(table_schema_simple, last_column_id=table_schema_simple.highest_field_id)
    map_ = MapType(1, StringType(), 2, IntegerType(), False)
    update.add_column(parent=None, name=field_name, type_var=map_)
    new_schema = update.apply()
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == MapType(5, StringType(), 6, IntegerType(), False)
    assert new_schema.highest_field_id == 6


def test_add_nested_list_type_column(table_schema_simple: Schema) -> None:
    # add list type column
    field_name = "new_column_list"
    update = _SchemaUpdate(table_schema_simple)
    list_ = ListType(
        element_id=101,
        element_type=StructType(
            NestedField(102, "lat", DoubleType()),
            NestedField(103, "long", DoubleType()),
        ),
        element_required=False,
    )
    update.add_column(parent=None, name=field_name, type_var=list_)
    new_schema = update.apply()
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == ListType(
        element_id=5,
        element_type=StructType(
            NestedField(6, "lat", DoubleType()),
            NestedField(7, "long", DoubleType()),
        ),
        element_required=False,
    )
    assert new_schema.highest_field_id == 7


def test_add_field_to_map_key(table_schema_nested_with_struct_key_map: Schema) -> None:
    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(table_schema_nested_with_struct_key_map)
        update.add_column(name="b", type_var=IntegerType(), parent="location.key").apply()
    assert "Cannot add fields to map keys" in str(exc_info.value)


def test_add_already_exists(table_schema_nested: Schema) -> None:
    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(table_schema_nested)
        update.add_column("foo", IntegerType())
    assert "already exists: foo" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(table_schema_nested)
        update.add_column(name="latitude", type_var=IntegerType(), parent="location")
    assert "already exists: location.lat" in str(exc_info.value)


def test_add_to_non_struct_type(table_schema_simple: Schema) -> None:
    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(table_schema_simple)
        update.add_column(name="lat", type_var=IntegerType(), parent="foo")
    assert "Cannot add column to non-struct type" in str(exc_info.value)


def test_add_required_column() -> None:
    schema_ = Schema(
        NestedField(field_id=1, name="a", field_type=BooleanType(), required=False), schema_id=1, identifier_field_ids=[]
    )

    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(schema_, last_column_id=1)
        update.add_required_column(name="data", type_var=IntegerType())
    assert "Incompatible change: cannot add required column: data" in str(exc_info.value)

    new_schema = (
        _SchemaUpdate(schema_, last_column_id=1)
        .allow_incompatible_changes()
        .add_required_column(name="data", type_var=IntegerType())
        .apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, name="a", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="data", field_type=IntegerType(), required=True),
        schema_id=0,
        identifier_field_ids=[],
    )


def test_add_required_column_case_insensitive() -> None:
    schema_ = Schema(
        NestedField(field_id=1, name="id", field_type=BooleanType(), required=False), schema_id=1, identifier_field_ids=[]
    )

    with pytest.raises(ValueError) as exc_info:
        update = _SchemaUpdate(schema_, last_column_id=1)
        update.allow_incompatible_changes().case_sensitive(False).add_required_column(name="ID", type_var=IntegerType())
    assert "already exists: ID" in str(exc_info.value)

    new_schema = (
        _SchemaUpdate(schema_, last_column_id=1)
        .allow_incompatible_changes()
        .add_required_column(name="ID", type_var=IntegerType())
        .apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, name="id", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="ID", field_type=IntegerType(), required=True),
        schema_id=0,
        identifier_field_ids=[],
    )
