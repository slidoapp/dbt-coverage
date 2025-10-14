import pytest

from dbt_coverage import Catalog, CoverageDiff, CoverageReport, CoverageType


@pytest.mark.parametrize("cov_type", [CoverageType.DOC, CoverageType.TEST])
def test_coverage_report_with_zero_tables(cov_type):
    """Tests that CoverageReport handles a catalog with 0 tables without division by zero."""

    empty_catalog = Catalog(tables={})

    coverage_report = CoverageReport.from_catalog(empty_catalog, cov_type)

    assert len(coverage_report.covered) == 0
    assert coverage_report.hits == 0
    assert len(coverage_report.total) == 0
    assert coverage_report.coverage is None
    assert len(coverage_report.misses) == 0
    assert coverage_report.subentities == {}


@pytest.mark.parametrize("cov_type", [CoverageType.DOC, CoverageType.TEST])
def test_coverage_diff_with_zero_tables(cov_type):
    empty_catalog = Catalog(tables={})
    coverage_report_1 = CoverageReport.from_catalog(empty_catalog, cov_type)
    coverage_report_2 = CoverageReport.from_catalog(empty_catalog, cov_type)

    diff = CoverageDiff(coverage_report_1, coverage_report_2)

    assert len(diff.new_misses) == 0
