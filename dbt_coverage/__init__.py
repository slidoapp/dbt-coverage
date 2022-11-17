from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import Dict, Set, List, Optional

import typer


SUPPORTED_MANIFEST_SCHEMA_VERSIONS = [
    "https://schemas.getdbt.com/dbt/manifest/v4.json",
    "https://schemas.getdbt.com/dbt/manifest/v5.json",
    "https://schemas.getdbt.com/dbt/manifest/v6.json",
    "https://schemas.getdbt.com/dbt/manifest/v7.json",
]

app = typer.Typer(help="Compute coverage of dbt-managed data warehouses.")


class CoverageType(Enum):
    DOC = "doc"
    TEST = "test"


class CoverageFormat(str, Enum):
    STRING_TABLE = "string"
    MARKDOWN_TABLE = "markdown"


@dataclass
class Column:
    """Dataclass containing the information about the docs and tests of a database column."""

    name: str
    doc: bool = None
    test: bool = None

    @staticmethod
    def from_node(node) -> Column:
        return Column(node["name"].lower())

    @staticmethod
    def is_valid_doc(doc):
        return doc is not None and doc != ""

    @staticmethod
    def is_valid_test(tests):
        return tests is not None and tests


@dataclass
class Table:
    """Dataclass containing the information about a database table and its columns."""

    name: str
    unique_id: str
    original_file_path: None
    columns: Dict[str, Column]

    @staticmethod
    def from_node(node) -> Table:
        columns = [Column.from_node(col) for col in node["columns"].values()]
        return Table(
            f"{node['metadata']['schema']}.{node['metadata']['name']}".lower(),
            node["unique_id"],
            None,
            {col.name: col for col in columns},
        )

    def update_original_file_path(self, manifest: Manifest) -> None:
        """
        Update Table's ``original_file_path`` attribute by retrieving this information from a
        Manifest.

        :param manifest: the Manifest used which contains the ``original_file_path`` for a Table
        :returns: None
        """
        old_original_file_path_value = self.original_file_path

        manifest_attributes = vars(manifest)
        for attribute_type_name, attribute_type_dict in manifest_attributes.items():
            for (
                attribute_instance_name,
                attribute_instance,
            ) in attribute_type_dict.items():

                if self.unique_id in attribute_instance.values():
                    self.original_file_path = attribute_instance["original_file_path"]

        if (
            self.original_file_path is None
            or self.original_file_path == old_original_file_path_value
        ):
            logging.info(
                f"original_file_path value not found in manifest for {self.unique_id}"
            )

    def get_column(self, column_name):
        return self.columns.get(column_name)


@dataclass
class Catalog:
    """Dataclass containing the information about a database catalog, its tables and columns."""

    tables: Dict[str, Table]

    def filter_catalog(self, model_path_filter: List[str]) -> Catalog:
        """
        Filter ``Catalog``'s ``tables`` attribute to ``Tables`` that have the``model_path_filter``
        value at the start of their ``original_file_path``.

        :param model_path_filter: the model_path string(s) to filter tables on, (matches using
        the ``startswith`` operator)

        :returns: Catalog
        :raises ValueError: if no ``Table`` in the ``tables`` Catalog attribute have an
        ``original_file_path`` that contains any ``model_path_filter`` value
        """
        filtered_tables = {}

        original_tables_dict = {key: val for key, val in self.tables.items()}
        for key, table in original_tables_dict.items():
            for path in model_path_filter:
                if table.original_file_path.startswith(path):
                    filtered_tables[key] = table
                    break

        if len(filtered_tables) < 1:
            logging.error("len(filtered_tables) < 1", exc_info=True)
            raise ValueError(
                "After filtering the Catalog contains no tables. Ensure your model_path_filter "
                "is correct"
            )
        else:
            logging.info(
                "Successfully filtered tables. Total tables post-filtering: %d tables",
                len(filtered_tables),
            )

            return Catalog(tables=filtered_tables)

    @staticmethod
    def from_nodes(nodes):
        tables = [Table.from_node(table) for table in nodes]
        return Catalog({table.name: table for table in tables})

    def get_table(self, table_name):
        return self.tables.get(table_name)


@dataclass
class Manifest:
    sources: Dict[str, Dict[str, Dict[str, Dict]]]
    models: Dict[str, Dict[str, Dict[str, Dict]]]
    seeds: Dict[str, Dict[str, Dict[str, Dict]]]
    snapshots: Dict[str, Dict[str, Dict[str, Dict]]]
    tests: Dict[str, Dict[str, List[Dict]]]

    @classmethod
    def from_nodes(cls, manifest_nodes: Dict[str, Dict]) -> Manifest:
        """Constructs a ``Manifest`` by parsing from manifest.json nodes."""

        sources = [
            table
            for table in manifest_nodes.values()
            if table["resource_type"] == "source"
        ]
        sources = {
            cls._full_table_name(table): {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "unique_id": table["unique_id"],
            }
            for table in sources
        }

        models = [
            table
            for table in manifest_nodes.values()
            if table["resource_type"] == "model"
        ]
        models = {
            cls._full_table_name(table): {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "unique_id": table["unique_id"],
            }
            for table in models
        }

        seeds = [
            table
            for table in manifest_nodes.values()
            if table["resource_type"] == "seed"
        ]
        seeds = {
            cls._full_table_name(table): {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "unique_id": table["unique_id"],
            }
            for table in seeds
        }

        snapshots = [
            table
            for table in manifest_nodes.values()
            if table["resource_type"] == "snapshot"
        ]
        snapshots = {
            cls._full_table_name(table): {
                "columns": cls._normalize_column_names(table["columns"]),
                "original_file_path": cls._normalize_path(table["original_file_path"]),
                "unique_id": table["unique_id"],
            }
            for table in snapshots
        }

        tests = cls._parse_tests(manifest_nodes)

        return Manifest(sources, models, seeds, snapshots, tests)

    @classmethod
    def _parse_tests(
        cls, manifest_nodes: Dict[str, Dict]
    ) -> Dict[str, Dict[str, List[Dict]]]:
        """Parses tests from manifest.json nodes.

        The logic is taken from the dbt-docs official source code:
        https://github.com/dbt-labs/dbt-docs/blob/02731092389b18d69649fdc322d969b5d5b61b20/src/app/services/project_service.js#L155-L221
        """

        id_to_table_name = {
            table_id: cls._full_table_name(table)
            for table_id, table in manifest_nodes.items()
            if table["resource_type"] in ["source", "model", "seed", "snapshot"]
        }

        tests = {}
        for node in manifest_nodes.values():
            if node["resource_type"] != "test" or "test_metadata" not in node:
                continue

            depends_on = node["depends_on"]["nodes"]
            if not depends_on:
                continue

            if node["test_metadata"]["name"] == "relationships":
                table_id = depends_on[-1]
            else:
                table_id = depends_on[0]
            table_name = id_to_table_name[table_id]

            column_name = (
                node.get("column_name")
                or node["test_metadata"]["kwargs"].get("column_name")
                or node["test_metadata"]["kwargs"].get("arg")
            )
            if not column_name:
                continue

            column_name = column_name.lower()
            table_tests = tests.setdefault(table_name, {})
            column_tests = table_tests.setdefault(column_name, [])
            column_tests.append(node)

        return tests

    @staticmethod
    def _full_table_name(table):
        return f'{table["schema"]}.{table["name"]}'.lower()

    @staticmethod
    def _normalize_column_names(columns):
        for col in columns.values():
            col["name"] = col["name"].lower()
        return {col["name"]: col for col in columns.values()}

    @staticmethod
    def _normalize_path(path: str) -> str:
        return str(Path(path).as_posix())


@dataclass
class CoverageReport:
    """
    Dataclass containing the information about the coverage of an entity, along with its nested
    subentities.

    Attributes:
        report_type: ``Type`` of the entity that the report represents, either CATALOG, TABLE or
            COLUMN.
        entity_name: In case of TABLE and COLUMN reports, the name of the respective entity.
        covered: Collection of names of columns in the entity that are documented.
        total: Collection of names of all columns in the entity.
        misses: Collection of names of all columns in the entity that are not documented.
        coverage: Percentage of documented columns.
        subentities: ``CoverageReport``s for each subentity of the entity, i.e. for ``Table``s of
            ``Catalog`` and for ``Column``s of ``Table``s.
    """

    class EntityType(Enum):
        CATALOG = "catalog"
        TABLE = "table"
        COLUMN = "column"

    @dataclass(frozen=True)
    class ColumnRef:
        table_name: str
        column_name: str

    entity_type: EntityType
    cov_type: CoverageType
    entity_name: Optional[str]
    covered: Set[ColumnRef]
    total: Set[ColumnRef]
    misses: Set[ColumnRef] = field(init=False)
    coverage: float = field(init=False)
    subentities: Dict[str, CoverageReport]

    def __post_init__(self):
        if self.covered is not None and self.total is not None:
            self.misses = self.total - self.covered
            self.coverage = len(self.covered) / len(self.total)
        else:
            self.misses = None
            self.coverage = None

    @classmethod
    def from_catalog(cls, catalog: Catalog, cov_type: CoverageType):
        subentities = {
            table.name: CoverageReport.from_table(table, cov_type)
            for table in catalog.tables.values()
        }
        covered = set(
            col for table_report in subentities.values() for col in table_report.covered
        )
        total = set(
            col for table_report in subentities.values() for col in table_report.total
        )

        return CoverageReport(
            cls.EntityType.CATALOG, cov_type, None, covered, total, subentities
        )

    @classmethod
    def from_table(cls, table: Table, cov_type: CoverageType):
        subentities = {
            col.name: CoverageReport.from_column(col, cov_type)
            for col in table.columns.values()
        }
        covered = set(
            replace(col, table_name=table.name)
            for col_report in subentities.values()
            for col in col_report.covered
        )
        total = set(
            replace(col, table_name=table.name)
            for col_report in subentities.values()
            for col in col_report.total
        )

        return CoverageReport(
            cls.EntityType.TABLE, cov_type, table.name, covered, total, subentities
        )

    @classmethod
    def from_column(cls, column: Column, cov_type: CoverageType):
        if cov_type == CoverageType.DOC:
            covered = column.doc
        elif cov_type == CoverageType.TEST:
            covered = column.test
        else:
            raise ValueError(f"Unsupported cov_type {cov_type}")

        covered = {CoverageReport.ColumnRef(None, column.name)} if covered else set()
        total = {CoverageReport.ColumnRef(None, column.name)}

        return CoverageReport(
            cls.EntityType.COLUMN, cov_type, column.name, covered, total, {}
        )

    def to_markdown_table(self):
        if self.entity_type == CoverageReport.EntityType.TABLE:
            return (
                f"| {self.entity_name:70} | {len(self.covered):5}/{len(self.total):<5} | "
                f"{self.coverage * 100:5.1f}% |"
            )
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()

            buf.write("# Coverage report\n")
            buf.write("| Model | Columns Covered | % |\n")
            buf.write("|:------|----------------:|:-:|\n")
            for _, table_cov in sorted(self.subentities.items()):
                buf.write(table_cov.to_markdown_table() + "\n")
            buf.write(
                f"| {'Total':70} | {len(self.covered):5}/{len(self.total):<5} | "
                f"{self.coverage * 100:5.1f}% |\n"
            )

            return buf.getvalue()
        else:
            raise TypeError(
                f"Unsupported report_type for to_markdown_table method: "
                f"{type(self.entity_type)}"
            )

    def to_formatted_string(self):
        if self.entity_type == CoverageReport.EntityType.TABLE:
            return (
                f"{self.entity_name:50} {len(self.covered):5}/{len(self.total):<5} "
                f"{self.coverage * 100:5.1f}%"
            )
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()

            buf.write("Coverage report\n")
            buf.write("=" * 69 + "\n")
            for _, table_cov in sorted(self.subentities.items()):
                buf.write(table_cov.to_formatted_string() + "\n")
            buf.write("=" * 69 + "\n")
            buf.write(
                f"{'Total':50} {len(self.covered):5}/{len(self.total):<5} "
                f"{self.coverage * 100:5.1f}%\n"
            )

            return buf.getvalue()
        else:
            raise TypeError(
                f"Unsupported report_type for to_formatted_string method: "
                f"{type(self.entity_type)}"
            )

    def to_dict(self):
        if self.entity_type == CoverageReport.EntityType.COLUMN:
            return {
                "name": self.entity_name,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
            }
        elif self.entity_type == CoverageReport.EntityType.TABLE:
            return {
                "name": self.entity_name,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
                "columns": [
                    col_report.to_dict() for col_report in self.subentities.values()
                ],
            }
        elif self.entity_type == CoverageReport.EntityType.CATALOG:
            return {
                "cov_type": self.cov_type.value,
                "covered": len(self.covered),
                "total": len(self.total),
                "coverage": self.coverage,
                "tables": [
                    table_report.to_dict() for table_report in self.subentities.values()
                ],
            }
        else:
            raise TypeError(
                f"Unsupported report_type for to_dict method: "
                f"{type(self.entity_type)}"
            )

    @staticmethod
    def from_dict(report, cov_type: CoverageType):
        if "tables" in report:
            subentities = {
                table_report["name"]: CoverageReport.from_dict(table_report, cov_type)
                for table_report in report["tables"]
            }
            return CoverageReport(
                CoverageReport.EntityType.CATALOG,
                cov_type,
                None,
                set(col for tbl in subentities.values() for col in tbl.covered),
                set(col for tbl in subentities.values() for col in tbl.total),
                subentities,
            )
        elif "columns" in report:
            table_name = report["name"]
            subentities = {
                col_report["name"]: CoverageReport.from_dict(col_report, cov_type)
                for col_report in report["columns"]
            }
            return CoverageReport(
                CoverageReport.EntityType.TABLE,
                cov_type,
                table_name,
                set(
                    replace(col, table_name=table_name)
                    for col_report in subentities.values()
                    for col in col_report.covered
                ),
                set(
                    replace(col, table_name=table_name)
                    for col_report in subentities.values()
                    for col in col_report.total
                ),
                subentities,
            )
        else:
            column_name = report["name"]
            return CoverageReport(
                CoverageReport.EntityType.COLUMN,
                cov_type,
                column_name,
                {CoverageReport.ColumnRef(None, column_name)}
                if report["covered"] > 0
                else set(),
                {CoverageReport.ColumnRef(None, column_name)},
                {},
            )

    def to_json(self):
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class CoverageDiff:
    """Dataclass summarizing the difference of two coverage reports, mainly its decrease."""

    before: Optional[CoverageReport]
    after: CoverageReport
    new_misses: Dict[str, CoverageDiff] = field(init=False)

    def __post_init__(self):
        assert self.before is None or self.before.cov_type == self.after.cov_type, (
            f"Cannot compare reports with different cov_types: {self.before.cov_type} and "
            f"{self.after.cov_type}"
        )
        assert (
            self.before is None or self.before.entity_type == self.after.entity_type
        ), (
            f"Cannot compare reports with different report_types: {self.before.report_type} and "
            f"{self.after.entity_type}"
        )

        self.new_misses = self.find_new_misses()

    def find_new_misses(self):
        if self.after.entity_type == CoverageReport.EntityType.COLUMN:
            return None

        new_misses = self.after.misses - (
            self.before.misses if self.before is not None else set()
        )

        res: Dict[str, CoverageDiff] = {}
        for new_miss in new_misses:
            new_misses_entity_name = (
                new_miss.table_name
                if self.after.entity_type == CoverageReport.EntityType.CATALOG
                else new_miss.column_name
            )
            before_entity = (
                self.before.subentities.get(new_misses_entity_name)
                if self.before is not None
                else None
            )
            after_entity = self.after.subentities[new_misses_entity_name]
            res[new_misses_entity_name] = CoverageDiff(before_entity, after_entity)

        return res

    def summary(self):
        buf = io.StringIO()

        if self.after.entity_type != CoverageReport.EntityType.CATALOG:
            raise TypeError(
                f"Unsupported report_type for summary method: "
                f"{self.after.entity_type}"
            )

        buf.write(f"{'':10}{'before':>10}{'after':>10}{'+/-':>15}\n")
        buf.write("=" * 45 + "\n")
        buf.write(
            f"{'Coverage':10}{self.before.coverage:10.2%}{self.after.coverage:10.2%}"
            f"{(self.after.coverage - self.before.coverage):+15.2%}\n"
        )
        buf.write("=" * 45 + "\n")

        add_del = (
            f"{len(set(self.after.subentities) - set(self.before.subentities)):+d}/"
            f"{-len(set(self.before.subentities) - set(self.after.subentities)):+d}"
        )
        buf.write(
            f"{'Tables':10}{len(self.before.subentities):10d}"
            f"{len(self.after.subentities):10d}"
            f"{add_del:>15}\n"
        )

        add_del = (
            f"{len(self.after.total - self.before.total):+d}/"
            f"{-len(self.before.total - self.after.total):+d}"
        )
        buf.write(
            f"{'Columns':10}{len(self.before.total):10d}{len(self.after.total):10d}"
            f"{add_del:>15}\n"
        )
        buf.write("=" * 45 + "\n")

        add_del = (
            f"{len(self.after.covered - self.before.covered):+d}/"
            f"{-len(self.before.covered - self.after.covered):+d}"
        )
        buf.write(
            f"{'Hits':10}{len(self.before.covered):10d}{len(self.after.covered):10d}"
            f"{add_del:>15}\n"
        )

        add_del = (
            f"{len(self.after.misses - self.before.misses):+d}/"
            f"{-len(self.before.misses - self.after.misses):+d}"
        )
        buf.write(
            f"{'Misses':10}{len(self.before.misses):10d}{len(self.after.misses):10d}"
            f"{add_del:>15}\n"
        )

        buf.write("=" * 45 + "\n")

        return buf.getvalue()

    def new_misses_summary(self):
        if self.after.entity_type == CoverageReport.EntityType.COLUMN:
            return self._new_miss_summary_row()

        elif self.after.entity_type == CoverageReport.EntityType.TABLE:
            buf = io.StringIO()

            buf.write(self._new_miss_summary_row())
            for col in self.new_misses.values():
                buf.write(col.new_misses_summary())

            return buf.getvalue()

        elif self.after.entity_type == CoverageReport.EntityType.CATALOG:
            buf = io.StringIO()
            buf.write("=" * 94 + "\n")
            buf.write(self._new_miss_summary_row())
            buf.write("=" * 94 + "\n")
            for table in self.new_misses.values():
                buf.write(table.new_misses_summary())
                buf.write("=" * 94 + "\n")

            return buf.getvalue()

        else:
            raise TypeError(
                f"Unsupported report_type for new_misses_summary method: "
                f"{self.after.entity_type}"
            )

    def _new_miss_summary_row(self):
        if self.after.entity_type == CoverageReport.EntityType.CATALOG:
            title_prefix = ""
        elif self.after.entity_type == CoverageReport.EntityType.TABLE:
            title_prefix = "- "
        elif self.after.entity_type == CoverageReport.EntityType.COLUMN:
            title_prefix = "-- "
        else:
            raise TypeError(
                f"Unsupported report_type for _new_miss_summary_row method: "
                f"{type(self.after.entity_type)}"
            )

        title = (
            "Catalog"
            if self.after.entity_type == CoverageReport.EntityType.CATALOG
            else self.after.entity_name
        )
        title = title_prefix + title

        before_covered = len(self.before.covered) if self.before is not None else "-"
        before_total = len(self.before.total) if self.before is not None else "-"
        before_coverage = (
            f"({self.before.coverage:.2%})" if self.before is not None else "(-)"
        )
        after_covered = len(self.after.covered)
        after_total = len(self.after.total)
        after_coverage = f"({self.after.coverage:.2%})"

        buf = io.StringIO()
        buf.write(f"{title:50}")
        buf.write(f"{before_covered:>5}/{before_total:<5}{before_coverage:^9}")
        buf.write(" -> ")
        buf.write(f"{after_covered:>5}/{after_total:<5}{after_coverage:^9}\n")

        return buf.getvalue()


def check_manifest_version(manifest_json):
    manifest_version = manifest_json["metadata"]["dbt_schema_version"]
    if manifest_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        logging.warning(
            "Unsupported manifest.json version %s, unexpected behavior can occur. Supported "
            "versions: %s. See "
            "https://github.com/slidoapp/dbt-coverage/tree/main#supported-dbt-versions for more "
            "details.",
            manifest_version,
            SUPPORTED_MANIFEST_SCHEMA_VERSIONS,
        )


def load_catalog(project_dir: Path, run_artifacts_dir: Path) -> Catalog:
    if run_artifacts_dir is None:
        catalog_path = project_dir / "target/catalog.json"
    else:
        catalog_path = run_artifacts_dir / "catalog.json"

    if not catalog_path.exists():
        raise FileNotFoundError(
            "catalog.json not found in target/ or in custom path - "
            "before using dbt-coverage, run: dbt docs generate"
        )

    with open(catalog_path) as f:
        catalog_json = json.load(f)

    catalog_nodes = {**catalog_json["sources"], **catalog_json["nodes"]}
    catalog = Catalog.from_nodes(catalog_nodes.values())

    logging.info("Successfully loaded %d tables from catalog", len(catalog.tables))

    return catalog


def load_manifest(project_dir: Path, run_artifacts_dir: Path) -> Manifest:
    if run_artifacts_dir is None:
        manifest_path = project_dir / "target/manifest.json"
    else:
        manifest_path = run_artifacts_dir / "manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"manifest.json not found in target or in custom path - "
            "before using dbt-coverage, run a dbt command that creates manifest artifact "
            "(see: https://docs.getdbt.com/reference/artifacts/manifest-json)"
        )

    with open(manifest_path) as f:
        manifest_json = json.load(f)

    check_manifest_version(manifest_json)

    manifest_nodes = {**manifest_json["sources"], **manifest_json["nodes"]}
    manifest = Manifest.from_nodes(manifest_nodes)

    return manifest


def load_files(project_dir: Path, run_artifacts_dir: Path) -> Catalog:
    if run_artifacts_dir is None:
        logging.info(
            "Loading catalog and manifest files from project dir: %s", project_dir
        )
    else:
        logging.info(
            "Loading catalog and manifest files from custom dir: %s", run_artifacts_dir
        )

    catalog = load_catalog(project_dir, run_artifacts_dir)
    manifest = load_manifest(project_dir, run_artifacts_dir)

    for table_name in catalog.tables:
        catalog_table = catalog.get_table(table_name)
        catalog_table.update_original_file_path(manifest)
        manifest_source_table = manifest.sources.get(table_name, {"columns": {}})
        manifest_model_table = manifest.models.get(table_name, {"columns": {}})
        manifest_seed_table = manifest.seeds.get(table_name, {"columns": {}})
        manifest_snapshot_table = manifest.snapshots.get(table_name, {"columns": {}})
        manifest_table_tests = manifest.tests.get(table_name, {})

        for catalog_column in catalog_table.columns.values():
            manifest_source_column = manifest_source_table["columns"].get(
                catalog_column.name
            )
            manifest_model_column = manifest_model_table["columns"].get(
                catalog_column.name
            )
            manifest_seed_column = manifest_seed_table["columns"].get(
                catalog_column.name
            )
            manifest_snapshot_column = manifest_snapshot_table["columns"].get(
                catalog_column.name
            )
            manifest_column_tests = manifest_table_tests.get(catalog_column.name)

            manifest_column = (
                manifest_source_column
                or manifest_model_column
                or manifest_seed_column
                or manifest_snapshot_column
                or {}
            )
            doc = manifest_column.get("description")
            catalog_column.doc = Column.is_valid_doc(doc)
            catalog_column.test = Column.is_valid_test(manifest_column_tests)

    return catalog


def compute_coverage(catalog: Catalog, cov_type: CoverageType):
    logging.info("Computing coverage for %d tables", len(catalog.tables))
    coverage_report = CoverageReport.from_catalog(catalog, cov_type)
    logging.info("Coverage computed successfully")
    return coverage_report


def compare_reports(report, compare_report):
    diff = CoverageDiff(compare_report, report)

    print(diff.summary())
    print(diff.new_misses_summary())

    return diff


def read_coverage_report(path: Path):
    with open(path) as f:
        report = json.load(f)
    report = CoverageReport.from_dict(report, CoverageType(report["cov_type"]))

    return report


def write_coverage_report(coverage_report: CoverageReport, path: Path):
    logging.info("Writing coverage report to %s", path)
    with open(path, "w") as f:
        f.write(coverage_report.to_json())
    logging.info("Report successfully written to %s", path)


def fail_under(coverage_report: CoverageReport, min_coverage: float):
    if coverage_report.coverage < min_coverage:
        raise RuntimeError(
            f"Measured coverage {coverage_report.coverage:.3f} "
            f"lower than min required coverage {min_coverage}"
        )


def fail_compare(coverage_report: CoverageReport, compare_path: Path):
    compare_report = read_coverage_report(compare_path)

    diff = compare_reports(coverage_report, compare_report)

    if diff.after.coverage < diff.before.coverage:
        raise RuntimeError(
            f"Coverage decreased from {diff.before.coverage:.2%} to "
            f"{diff.after.coverage:.2%}"
        )


def do_compute(
    project_dir: Path = Path("."),
    run_artifacts_dir: Path = None,
    cov_report: Path = Path("coverage.json"),
    cov_type: CoverageType = CoverageType.DOC,
    cov_fail_under: float = None,
    cov_fail_compare: Path = None,
    model_path_filter: Optional[List[str]] = None,
    cov_format: CoverageFormat = CoverageFormat.STRING_TABLE,
):
    """
    Computes coverage for a dbt project.

    Use this method in your Python code to bypass typer.
    """

    catalog = load_files(project_dir, run_artifacts_dir)

    if len(model_path_filter) >= 1:
        catalog = catalog.filter_catalog(model_path_filter)

    coverage_report = compute_coverage(catalog, cov_type)

    if cov_format == CoverageFormat.MARKDOWN_TABLE:
        print(coverage_report.to_markdown_table())
    else:
        print(coverage_report.to_formatted_string())

    write_coverage_report(coverage_report, cov_report)

    if cov_fail_under is not None:
        fail_under(coverage_report, cov_fail_under)

    if cov_fail_compare is not None:
        fail_compare(coverage_report, cov_fail_compare)

    return coverage_report


def do_compare(report: Path, compare_report: Path):
    """
    Compares two coverage reports generated by the ``compute`` command.

    Use this method in your Python code to bypass typer.
    """

    report = read_coverage_report(report)
    compare_report = read_coverage_report(compare_report)

    diff = compare_reports(report, compare_report)

    return diff


@app.command()
def compute(
    project_dir: Path = typer.Option(".", help="dbt project directory path."),
    run_artifacts_dir: Path = typer.Option(
        None, help="custom directory path for " "catalog and manifest files"
    ),
    cov_report: Path = typer.Option(
        "coverage.json", help="Output coverage report path."
    ),
    cov_type: CoverageType = typer.Argument(..., help="Type of coverage to compute."),
    cov_fail_under: float = typer.Option(
        None, help="Fail if coverage is lower than " "provided threshold."
    ),
    cov_fail_compare: Path = typer.Option(
        None,
        help="Path to coverage report to compare "
        "with and fail if current coverage "
        "is lower. Normally used to prevent "
        "coverage drop between subsequent "
        "tests.",
    ),
    model_path_filter: Optional[List[str]] = typer.Option(
        None, help="The model_path " "string(s) to " "filter tables " "on."
    ),
    cov_format: CoverageFormat = typer.Option(
        CoverageFormat.STRING_TABLE,
        help="The output format to print, either " "`string` or `markdown`",
    ),
):
    """Compute coverage for project in PROJECT_DIR from catalog.json and manifest.json."""

    return do_compute(
        project_dir,
        run_artifacts_dir,
        cov_report,
        cov_type,
        cov_fail_under,
        cov_fail_compare,
        model_path_filter,
        cov_format,
    )


@app.command()
def compare(
    report: Path = typer.Argument(..., help="Path to coverage report."),
    compare_report: Path = typer.Argument(
        ..., help="Path to another coverage report to " "compare with."
    ),
):
    """Compare two coverage reports generated by the compute command."""

    return do_compare(report, compare_report)


@app.callback()
def main(
    verbose: bool = typer.Option(False, help="Turn verbose logging messages on or off.")
):
    if verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)


if __name__ == "__main__":
    app()
