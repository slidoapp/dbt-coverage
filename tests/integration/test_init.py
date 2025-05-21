import os
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

import psycopg2
import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dbt_coverage import CoverageType, do_compare, do_compute

DBT_PROJECT_DIR = Path("tests/integration/jaffle_shop")
PATCHES_DIR = Path("tests/integration/patches")
DBT_ARGS = [
    "--profiles-dir",
    "tests/integration/profiles",
    "--project-dir",
    DBT_PROJECT_DIR,
]


def apply_patch(patch_file: Path):
    class ApplyPatch:
        def __init__(self, patch_file: Path):
            # We get the absolute path since the patch is run with a changed cwd
            self.patch_file = patch_file.absolute()

        def __enter__(self):
            subprocess.run(
                ["patch", "-p1", f"-i{self.patch_file}"], cwd=DBT_PROJECT_DIR, check=True
            )

        def __exit__(self, exc_type, exc_val, exc_tb):
            subprocess.run(
                ["patch", "-R", "-p1", f"-i{self.patch_file}"], cwd=DBT_PROJECT_DIR, check=True
            )

    return ApplyPatch(patch_file)


@pytest.fixture(scope="session")
def docker_compose_command():
    return "docker compose"


@pytest.fixture(scope="session")
def docker_compose_file():
    return "tests/integration/docker-compose.yml"


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "dbt-coverage-integration-tests"


@pytest.fixture(scope="session")
def docker_setup():
    return "up -d --wait"


@pytest.fixture(scope="session")
def postgres_service(docker_ip, docker_services):
    port = docker_services.port_for("postgres", 5432)
    return docker_ip, port


@pytest.fixture(scope="session")
def setup_postgres(postgres_service):
    conn = psycopg2.connect(
        host=postgres_service[0], port=postgres_service[1], user="postgres", password="example"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE jaffle_shop")


@pytest.fixture(scope="session")
def session_setup_dbt(setup_postgres):
    """Runs dbt and dbt docs generate.

    This is a session fixture that can be used to accelerate tests if no tests change the models.
    """

    run_dbt()


@pytest.fixture()
def setup_dbt(session_setup_dbt):
    """Runs dbt and dbt docs generate before and after the test.

    Use with tests that temporarily change the models.
    """

    # Setting up dbt before running the test is only necessary if it is the first test to run,
    # or else it will all be already setup by the previous test.
    # We replace the dbt setup before running the test by including session_setup_dbt fixture,
    # which covers the case in which this tests runs first.

    yield

    run_dbt()


def run_dbt():
    # Workaround for a bug - https://github.com/dbt-labs/dbt-core/issues/9138
    env = {**os.environ, "DBT_CLEAN_PROJECT_FILES_ONLY": "false"}
    subprocess.run(["dbt", "clean", *DBT_ARGS], env=env, check=True)

    subprocess.run(["dbt", "seed", *DBT_ARGS], check=True)
    subprocess.run(["dbt", "run", *DBT_ARGS], check=True)
    subprocess.run(["dbt", "docs", "generate", *DBT_ARGS], check=True)


def test_compute_doc(session_setup_dbt):
    report = do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC)

    assert len(report.covered) == 15
    assert len(report.total) == 38


def test_compute_test(session_setup_dbt):
    report = do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.TEST)

    assert len(report.covered) == 14
    assert len(report.total) == 38


def test_compute_path_filter(session_setup_dbt):
    report = do_compute(
        DBT_PROJECT_DIR,
        cov_type=CoverageType.DOC,
        model_path_filter=["models/staging"],
    )

    assert len(report.subentities) == 3
    assert all("stg" in table for table in report.subentities)
    assert len(report.covered) == 0
    assert len(report.total) == 11


def test_compute_path_exclusion_filter(session_setup_dbt):
    report = do_compute(
        DBT_PROJECT_DIR,
        cov_type=CoverageType.DOC,
        model_path_exclusion_filter=["models/staging"],
    )

    assert len(report.subentities) == 5
    assert not any("stg" in table for table in report.subentities)
    assert len(report.covered) == 15
    assert len(report.total) == 27


def test_compute_both_path_filters(session_setup_dbt):
    report = do_compute(
        DBT_PROJECT_DIR,
        cov_type=CoverageType.DOC,
        model_path_filter=["models/staging"],
        model_path_exclusion_filter=["models/staging/stg_customers"],
    )

    assert len(report.subentities) == 2
    assert all("stg" in table for table in report.subentities)
    assert "jaffle_shop.stg_customers" not in report.subentities
    assert len(report.covered) == 0
    assert len(report.total) == 8


def test_compare_no_change(session_setup_dbt):
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f1.name))
        do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f2.name))

        diff = do_compare(Path(f2.name), Path(f1.name))

    assert len(diff.new_misses) == 0


def test_compare_new_column(setup_dbt):
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f1.name))
        with apply_patch(PATCHES_DIR / "test_compare_new_column.patch"):
            run_dbt()
            do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f2.name))

        diff = do_compare(Path(f2.name), Path(f1.name))

    newly_missed_tables = diff.new_misses
    assert set(newly_missed_tables) == {"jaffle_shop.customers"}
    newly_missed_table = list(newly_missed_tables.values())[0]
    newly_missed_columns = set(newly_missed_table.new_misses.keys())
    assert newly_missed_columns == {"new_column"}


def test_compare_new_table(setup_dbt):
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f1.name))
        with apply_patch(PATCHES_DIR / "test_compare_new_table.patch"):
            run_dbt()
            do_compute(DBT_PROJECT_DIR, cov_type=CoverageType.DOC, cov_report=Path(f2.name))

        diff = do_compare(Path(f2.name), Path(f1.name))

    newly_missed_tables = diff.new_misses
    assert set(newly_missed_tables) == {"jaffle_shop.new_table"}
    newly_missed_table = list(newly_missed_tables.values())[0]
    newly_missed_columns = set(newly_missed_table.new_misses.keys())
    assert newly_missed_columns == {"col_1", "col_2", "col_3"}
