import pytest

from dbt_coverage import Catalog, CoverageReport, CoverageType


@pytest.mark.parametrize("cov_type", [CoverageType.DOC, CoverageType.TEST])
def test_coverage_report_with_zero_tables(cov_type):
    """Tests that CoverageReport handles a catalog with 0 tables without division by zero."""

    empty_catalog = Catalog(tables={})

    coverage_report = CoverageReport.from_catalog(empty_catalog, cov_type)

    assert len(coverage_report.covered) == 0
    assert len(coverage_report.total) == 0
    assert coverage_report.coverage is None
    assert coverage_report.misses is None
    assert coverage_report.subentities == {}
