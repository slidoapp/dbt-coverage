from pathlib import Path

import psycopg2
import pytest
from dbt.cli.main import dbtRunner, dbtRunnerResult
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dbt_coverage import do_compute, CoverageType


@pytest.fixture(scope="session")
def docker_compose_file():
    return "tests/integration/docker-compose.yml"


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


@pytest.fixture
def setup_dbt(setup_postgres):
    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(
        [
            "seed",
            "--profiles-dir",
            "tests/integration/profiles",
            "--project-dir",
            "tests/integration/jaffle_shop",
            "--target-path",
            "tests/integration/jaffle_shop",
        ]
    )
    print(res.result)

    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(
        [
            "run",
            "--profiles-dir",
            "tests/integration/profiles",
            "--project-dir",
            "tests/integration/jaffle_shop",
            "--target-path",
            "tests/integration/jaffle_shop",
        ]
    )
    print(res.result)

    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(
        [
            "docs",
            "generate",
            "--profiles-dir",
            "tests/integration/profiles",
            "--project-dir",
            "tests/integration/jaffle_shop",
            "--target-path",
            "tests/integration/jaffle_shop/target",
        ]
    )
    print(res.result)


def test_compute_doc(setup_dbt):
    res = do_compute(Path("tests/integration/jaffle_shop"), cov_type=CoverageType.DOC)

    assert len(res.covered) == 15
    assert len(res.total) == 38


def test_compute_test(setup_dbt):
    res = do_compute(Path("tests/integration/jaffle_shop"), cov_type=CoverageType.TEST)

    assert len(res.covered) == 14
    assert len(res.total) == 38
