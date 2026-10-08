import json

import pytest

from dbt_coverage import CoverageReport, CoverageType, load_files


@pytest.mark.parametrize("resource_type", ["source", "model", "seed", "snapshot"])
@pytest.mark.parametrize("test_field", ["column_name", "kwargs_column_name", "arg"])
@pytest.mark.parametrize(
    "catalog_name,manifest_name",
    [
        ("Top Performer", '"Top Performer"'),
        ('A "quoted" column', '"A ""quoted"" column"'),
        ("CUSTOMER_ID", "Customer_Id"),
        ("Top Performer", "top performer"),
        ('A "quoted" column', 'A "quoted" column'),
        ('Leading"', 'Leading"'),
        ('"Trailing', '"Trailing'),
    ],
)
def test_column_matching(tmp_path, resource_type, test_field, catalog_name, manifest_name):
    table_id = f"{resource_type}.project.staff"
    table = {
        "unique_id": table_id,
        "resource_type": resource_type,
        "schema": "public",
        "name": "staff",
        "original_file_path": "models/staff.sql",
        "columns": {manifest_name: {"name": manifest_name, "description": "Documented"}},
    }
    test = {
        "resource_type": "test",
        "depends_on": {"nodes": [table_id]},
        "test_metadata": {"name": "not_null", "kwargs": {}},
    }
    if test_field == "column_name":
        test["column_name"] = manifest_name
    else:
        key = "column_name" if test_field == "kwargs_column_name" else "arg"
        test["test_metadata"]["kwargs"][key] = manifest_name
    bucket = "sources" if resource_type == "source" else "nodes"
    manifest = {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
        "sources": {},
        "nodes": {"test.project.staff": test},
    }
    manifest[bucket][table_id] = table
    catalog = {"sources": {}, "nodes": {}}
    catalog[bucket][table_id] = {
        "unique_id": table_id,
        "columns": {catalog_name: {"name": catalog_name}},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))

    result = load_files(tmp_path, tmp_path)

    for cov_type in (CoverageType.DOC, CoverageType.TEST):
        report = CoverageReport.from_catalog(result, cov_type)
        assert report.coverage == 1
        assert report.hits == 1
        assert report.total == {CoverageReport.ColumnRef("public.staff", catalog_name.lower())}
