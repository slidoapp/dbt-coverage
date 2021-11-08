from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Set, Tuple, List, Optional

import typer

logging.basicConfig(level=logging.INFO)

app = typer.Typer(help="Compute coverage of dbt-managed data warehouses.")


class CoverageType(Enum):
    DOC = 'doc'
    TEST = 'test'


class EntityWithCoverage:
    def coverage(self, cov_type: CoverageType) -> Tuple[Set[str], Set[str]]:
        raise NotImplementedError()


@dataclass
class Column(EntityWithCoverage):
    """Dataclass containing the information about the docs and tests of a database column."""

    name: str
    doc: bool = None
    test: bool = None

    @staticmethod
    def from_node(node) -> Column:
        return Column(
            node['name']
        )

    def coverage(self, cov_type: CoverageType) -> Tuple[Set[str], Set[str]]:
        if cov_type == CoverageType.DOC:
            covered = self.doc
        elif cov_type == CoverageType.TEST:
            covered = self.test
        else:
            raise ValueError(f"Unsupported cov_type {cov_type}")

        covered = {self.name} if covered else set()
        total = {self.name}
        return covered, total

    @staticmethod
    def is_valid_doc(doc):
        return doc is not None and doc != ''

    @staticmethod
    def is_valid_test(tests):
        return tests is not None and tests


@dataclass
class Table(EntityWithCoverage):
    """Dataclass containing the information about a database table and its columns."""

    name: str
    columns: Dict[str, Column]

    @staticmethod
    def from_node(node) -> Table:
        return Table(
            node['metadata']['name'],
            {col['name']: Column.from_node(col) for col in node['columns'].values()}
        )

    def get_column(self, column_name):
        return self.columns.get(column_name)

    def coverage(self, cov_type: CoverageType):
        coverages = [col.coverage(cov_type) for col in self.columns.values()]
        covered = set(f'{self.name}.{col}' for covered, _ in coverages for col in covered)
        total = set(f'{self.name}.{col}' for _, total in coverages for col in total)
        return covered, total


@dataclass
class Catalog(EntityWithCoverage):
    """Dataclass containing the information about a database catalog, its tables and columns."""

    tables: Dict[str, Table]

    @staticmethod
    def from_nodes(nodes):
        return Catalog({node['metadata']['name']: Table.from_node(node) for node in nodes})

    def get_table(self, table_name):
        return self.tables.get(table_name)

    def coverage(self, cov_type: CoverageType):
        coverages = [table.coverage(cov_type) for table in self.tables.values()]
        covered = set(col for covered, _ in coverages for col in covered)
        total = set(col for _, total in coverages for col in total)
        return covered, total


@dataclass
class Manifest:
    sources: Dict[str, Dict[str, Dict]]
    models: Dict[str, Dict[str, Dict]]
    tests: Dict[str, Dict[str, List[Dict]]]

    @classmethod
    def from_nodes(cls, manifest_nodes: Dict[str: Dict]) -> Manifest:
        id_to_table_name = {table_id: table['name'] for table_id, table in manifest_nodes.items()
                            if table['resource_type'] in ['source', 'model']}
        sources = {table['name']: table['columns'] for table in manifest_nodes.values()
                   if table['resource_type'] == 'source'}
        models = {table['name']: table['columns'] for table in manifest_nodes.values()
                  if table['resource_type'] == 'model'}
        tests = {}
        for node in manifest_nodes.values():
            if node['resource_type'] != 'test' or 'schema' not in node['tags']:
                continue

            depends_on = node['depends_on']['nodes']
            if node['test_metadata']['name'] == 'relationships':
                table_id = depends_on[len(depends_on) - 1]
            else:
                table_id = depends_on[0]
            table_name = id_to_table_name[table_id]
            column_name = node['column_name'] or node['test_metadata']['kwargs']['column_name'] \
                or node['test_metadata']['kwargs']['arg']

            table_tests = tests.setdefault(table_name, {})
            column_tests = table_tests.setdefault(column_name, [])
            column_tests.append(node)

        return Manifest(sources, models, tests)


@dataclass
class CoverageReport:
    """
    Dataclass containing the information about the coverage of an entity, along with its nested
    subentities.

    Attributes:
        entity: ``Catalog``, ``Table`` or ``Column`` that the coverage report is calculated for.
        covered: Collection of names of columns in the entity that are documented.
        total: Collection of names of all columns in the entity.
        misses: Collection of names of all columns in the entity that are not documented.
        coverage: Percentage of documented columns.
        subentities: ``CoverageReport``s for each subentity of the entity, i.e. for ``Table``s of
            ``Catalog`` and for ``Column``s of ``Table``s.
    """

    entity: Union[Catalog, Table, Column]
    covered: Set[str]
    total: Set[str]
    misses: Set[str] = field(init=False)
    coverage: float = field(init=False)
    subentities: Dict[str, CoverageReport]

    def __post_init__(self):
        if self.covered is not None and self.total is not None:
            self.misses = self.total - self.covered
            self.coverage = len(self.covered) / len(self.total)
        else:
            self.misses = None
            self.coverage = None

    @staticmethod
    def from_catalog(catalog: Catalog, cov_type: CoverageType):
        cov = catalog.coverage(cov_type)
        return CoverageReport(
            catalog,
            cov[0],
            cov[1],
            {table.name: CoverageReport.from_table(table, cov_type)
             for table in catalog.tables.values()}
        )

    @staticmethod
    def from_table(table: Table, cov_type: CoverageType):
        cov = table.coverage(cov_type)
        return CoverageReport(
            table,
            cov[0],
            cov[1],
            {col.name: CoverageReport.from_column(col, cov_type) for col in table.columns.values()}
        )

    @staticmethod
    def from_column(column: Column, cov_type: CoverageType):
        cov = column.coverage(cov_type)
        return CoverageReport(
            column,
            cov[0],
            cov[1],
            {}
        )

    def to_formatted_string(self):
        if isinstance(self.entity, Table):
            return f"{self.entity.name:50} {len(self.covered):5}/{len(self.total):<5} " \
                   f"{self.coverage * 100:5.1f}%"
        elif isinstance(self.entity, Catalog):
            buf = io.StringIO()

            buf.write("Coverage report\n")
            buf.write('=' * 69 + "\n")
            for _, table_cov in sorted(self.subentities.items()):
                buf.write(table_cov.to_formatted_string() + "\n")
            buf.write('=' * 69 + "\n")
            buf.write(f"{'Total':50} {len(self.covered):5}/{len(self.total):<5} "
                      f"{self.coverage * 100:5.1f}%\n")

            return buf.getvalue()
        else:
            raise TypeError(f"Unsupported entity type for to_formatted_string method: "
                            f"{type(self.entity)}")

    def to_dict(self):
        if isinstance(self.entity, Column):
            return {
                'name': self.entity.name,
                'covered': len(self.covered),
                'total': len(self.total),
                'coverage': self.coverage
            }
        elif isinstance(self.entity, Table):
            return {
                'name': self.entity.name,
                'covered': len(self.covered),
                'total': len(self.total),
                'coverage': self.coverage,
                'columns': [col_report.to_dict() for col_report in self.subentities.values()]
            }
        elif isinstance(self.entity, Catalog):
            return {
                'covered': len(self.covered),
                'total': len(self.total),
                'coverage': self.coverage,
                'tables': [table_report.to_dict() for table_report in self.subentities.values()]
            }
        else:
            raise TypeError(f"Unsupported entity type for to_dict method: {type(self.entity)}")

    @staticmethod
    def from_dict(report):
        if 'tables' in report:
            subentities = {table_report['name']: CoverageReport.from_dict(table_report) for
                           table_report in report['tables']}
            return CoverageReport(
                Catalog(None),
                set(f"{tbl.entity.name}.{col}"
                    for tbl in subentities.values()
                    for col in tbl.covered),
                set(f"{tbl.entity.name}.{col}"
                    for tbl in subentities.values()
                    for col in tbl.total),
                subentities
            )
        elif 'columns' in report:
            table_name = report['name']
            subentities = {col_report['name']: CoverageReport.from_dict(col_report)
                           for col_report in report['columns']}
            return CoverageReport(
                Table(table_name, None),
                set(col.entity.name for col in subentities.values() for _ in col.covered),
                set(col.entity.name for col in subentities.values() for _ in col.total),
                subentities
            )
        else:
            return CoverageReport(
                Column(report['name']),
                {report['name']} if report['covered'] > 0 else set(),
                {report['name']},
                {}
            )

    def to_json(self):
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class CoverageDiff:
    """Dataclass summarizing the difference of two coverage reports, mainly its decrease."""

    before: CoverageReport
    after: CoverageReport
    new_misses: Dict[str, CoverageDiff] = field(init=False)

    # @formatter:off
    def __post_init__(self):
        assert self.before.entity is None or type(self.before.entity) == type(self.after.entity)  # noqa
        self.new_misses = self.find_new_misses()
    # @formatter:on

    def find_new_misses(self):
        if isinstance(self.after.entity, Column):
            return None

        new_misses_names = self.after.misses - self.before.misses \
            if self.before.misses is not None \
            else self.after.misses
        new_misses_entity_names = set(miss.split('.', maxsplit=1)[0] for miss in new_misses_names)

        res: Dict[str, CoverageDiff] = {}
        for new_miss_entity_name in new_misses_entity_names:
            res[new_miss_entity_name] = CoverageDiff(
                self.before.subentities.get(new_miss_entity_name,
                                            CoverageReport(None, None, None, {})),
                self.after.subentities[new_miss_entity_name]
            )

        return res

    def summary(self):
        buf = io.StringIO()

        if not isinstance(self.after.entity, Catalog):
            raise TypeError(f"Unsupported entity type for summary method: "
                            f"{type(self.after.entity)}")

        buf.write(f"{'':10}{'before':>10}{'after':>10}{'+/-':>15}\n")
        buf.write('=' * 45 + "\n")
        buf.write(f"{'Coverage':10}{self.before.coverage:10.2%}{self.after.coverage:10.2%}"
                  f"{(self.after.coverage - self.before.coverage):+15.2%}\n")
        buf.write('=' * 45 + "\n")

        add_del = f"{len(set(self.after.subentities) - set(self.before.subentities)):+d}/" \
                  f"{-len(set(self.before.subentities) - set(self.after.subentities)):+d}"
        buf.write(f"{'Tables':10}{len(self.before.subentities):10d}"
                  f"{len(self.after.subentities):10d}"
                  f"{add_del:>15}\n")

        add_del = f"{len(self.after.total - self.before.total):+d}/" \
                  f"{-len(self.before.total - self.after.total):+d}"
        buf.write(f"{'Columns':10}{len(self.before.total):10d}{len(self.after.total):10d}"
                  f"{add_del:>15}\n")
        buf.write('=' * 45 + "\n")

        add_del = f"{len(self.after.covered - self.before.covered):+d}/" \
                  f"{-len(self.before.covered - self.after.covered):+d}"
        buf.write(f"{'Hits':10}{len(self.before.covered):10d}{len(self.after.covered):10d}"
                  f"{add_del:>15}\n")

        add_del = f"{len(self.after.misses - self.before.misses):+d}/" \
                  f"{-len(self.before.misses - self.after.misses):+d}"
        buf.write(f"{'Misses':10}{len(self.before.misses):10d}{len(self.after.misses):10d}"
                  f"{add_del:>15}\n")

        buf.write('=' * 45 + "\n")

        return buf.getvalue()

    def new_misses_summary(self):
        if isinstance(self.after.entity, Column):
            return self._new_miss_summary_row()

        elif isinstance(self.after.entity, Table):
            buf = io.StringIO()

            buf.write(self._new_miss_summary_row())
            for col in self.new_misses.values():
                buf.write(col.new_misses_summary())

            return buf.getvalue()

        elif isinstance(self.after.entity, Catalog):
            buf = io.StringIO()
            buf.write("=" * 94 + '\n')
            buf.write(self._new_miss_summary_row())
            buf.write("=" * 94 + '\n')
            for table in self.new_misses.values():
                buf.write(table.new_misses_summary())
                buf.write("=" * 94 + '\n')

            return buf.getvalue()

        else:
            raise TypeError(f"Unsupported entity type for new_misses_summary method: "
                            f"{type(self.after.entity)}")

    def _new_miss_summary_row(self):
        if isinstance(self.after.entity, Catalog):
            title_prefix = ''
        elif isinstance(self.after.entity, Table):
            title_prefix = '- '
        elif isinstance(self.after.entity, Column):
            title_prefix = '-- '
        else:
            raise TypeError(f"Unsupported entity type for _new_miss_summary_row method: "
                            f"{type(self.after.entity)}")

        title = "Catalog" if isinstance(self.after.entity, Catalog) else self.after.entity.name
        title = title_prefix + title

        before_covered = len(self.before.covered) if self.before.covered is not None else '-'
        before_total = len(self.before.total) if self.before.total is not None else '-'
        after_covered = len(self.after.covered)
        after_total = len(self.after.total)
        if self.before.coverage is not None:
            before_coverage = f"({self.before.coverage:.2%})"
        else:
            before_coverage = "(-)"
        after_coverage = f"({self.after.coverage:.2%})"

        buf = io.StringIO()
        buf.write(f"{title:50}")
        buf.write(f"{before_covered:>5}/{before_total:<5}{before_coverage:^9}")
        buf.write(" -> ")
        buf.write(f"{after_covered:>5}/{after_total:<5}{after_coverage:^9}\n")

        return buf.getvalue()


def load_catalog(project_dir: Path) -> Catalog:
    with open(project_dir / 'target/catalog.json') as f:
        catalog_json = json.load(f)

    catalog_nodes = {**catalog_json['sources'], **catalog_json['nodes']}
    catalog = Catalog.from_nodes(catalog_nodes.values())

    logging.info("Successfully loaded %d catalog nodes", len(catalog_nodes))

    return catalog


def load_manifest(project_dir: Path) -> Manifest:
    with open(project_dir / 'target/manifest.json') as f:
        manifest_json = json.load(f)

    manifest_nodes = {**manifest_json['sources'], **manifest_json['nodes']}
    manifest = Manifest.from_nodes(manifest_nodes)

    return manifest


def load_files(project_dir: Path) -> Catalog:
    logging.info("Loading catalog and manifest files from project dir: %s", project_dir)

    catalog = load_catalog(project_dir)
    manifest = load_manifest(project_dir)

    for table_name in catalog.tables:
        catalog_table = catalog.get_table(table_name)
        manifest_source_table = manifest.sources.get(table_name, {})
        manifest_model_table = manifest.models.get(table_name, {})
        manifest_table_tests = manifest.tests.get(table_name, {})

        for catalog_column in catalog_table.columns.values():
            manifest_source_column = manifest_source_table.get(catalog_column.name)
            manifest_model_column = manifest_model_table.get(catalog_column.name)
            manifest_column_tests = manifest_table_tests.get(catalog_column.name)

            doc = (manifest_source_column or manifest_model_column or {}).get('description')
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
        compare_report = json.load(f)
    compare_report = CoverageReport.from_dict(compare_report)

    return compare_report


def write_coverage_report(coverage_report: CoverageReport, path: Path):
    logging.info("Writing coverage report to %s", path)
    with open(path, 'w') as f:
        f.write(coverage_report.to_json())
    logging.info("Report successfully written to %s", path)


def fail_under(coverage_report: CoverageReport, min_coverage: float):
    if coverage_report.coverage < min_coverage:
        raise RuntimeError(f"Measured coverage {coverage_report.coverage:.3f} "
                           f"lower than min required coverage {min_coverage}")


def fail_compare(coverage_report: CoverageReport, compare_path: Path):
    compare_report = read_coverage_report(compare_path)

    diff = compare_reports(coverage_report, compare_report)

    if diff.after.coverage < diff.before.coverage:
        raise RuntimeError(f"Coverage decreased from {diff.before.coverage:.2%} to "
                           f"{diff.after.coverage:.2%}")


def do_compute(project_dir: Path = Path('.'), cov_report: Path = Path('coverage.json'),
               cov_type: CoverageType = CoverageType.DOC, cov_fail_under: float = None,
               cov_fail_compare: Path = None):
    """
    Computes coverage for a dbt project.

    Use this method in your Python code to bypass typer.
    """

    catalog = load_files(project_dir)
    coverage_report = compute_coverage(catalog, cov_type)

    print(coverage_report.to_formatted_string())

    write_coverage_report(coverage_report, cov_report)

    if cov_fail_under is not None:
        fail_under(coverage_report, cov_fail_under)

    if cov_fail_compare is not None:
        fail_compare(coverage_report, cov_fail_compare)


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
def compute(project_dir: Path = typer.Option('.', help="dbt project directory path."),
            cov_report: Path = typer.Option('coverage.json', help="Output coverage report path."),
            cov_type: CoverageType = typer.Argument(..., help="Type of coverage to compute."),
            cov_fail_under: float = typer.Option(None, help="Fail if coverage is lower than "
                                                            "provided threshold."),
            cov_fail_compare: Path = typer.Option(None, help="Path to coverage report to compare "
                                                             "with and fail if current coverage "
                                                             "is lower. Normally used to prevent "
                                                             "coverage drop between subsequent "
                                                             "tests.")):
    """Compute coverage for project in PROJECT_DIR from catalog.json and manifest.json."""

    do_compute(project_dir, cov_report, cov_type, cov_fail_under, cov_fail_compare)


@app.command()
def compare(report: Path = typer.Argument(..., help="Path to coverage report."),
            compare_report: Path = typer.Argument(..., help="Path to another coverage report to "
                                                            "compare with.")):
    """Compare two coverage reports generated by the compute command."""

    return do_compare(report, compare_report)


if __name__ == '__main__':
    app()
