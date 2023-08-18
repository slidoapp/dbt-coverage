import subprocess
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dbt_coverage import do_compute, CoverageType

DBT_PROJECT_PATH = "tests/integration/jaffle_shop"
DBT_ARGS = [
    "--profiles-dir",
    "tests/integration/profiles",
    "--project-dir",
    DBT_PROJECT_PATH,
]


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


@pytest.fixture
def setup_dbt(setup_postgres):
    subprocess.run(["dbt", "clean", *DBT_ARGS], check=True)
    subprocess.run(["dbt", "seed", *DBT_ARGS], check=True)
    subprocess.run(["dbt", "run", *DBT_ARGS], check=True)
    subprocess.run(["dbt", "docs", "generate", *DBT_ARGS], check=True)


def test_compute_doc(setup_dbt):
    res = do_compute(Path(DBT_PROJECT_PATH), cov_type=CoverageType.DOC)

    assert len(res.covered) == 15
    assert len(res.total) == 38


def test_compute_test(setup_dbt):
    res = do_compute(Path(DBT_PROJECT_PATH), cov_type=CoverageType.TEST)

    assert len(res.covered) == 14
    assert len(res.total) == 38
