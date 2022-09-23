import pytest

from observer import repo


@pytest.fixture
def init_db():
    repo.DB.session.sql(f"create database IF NOT EXISTS {repo.DB().database_name()}")
    yield
    repo.DB.session.sql(f"drop database IF EXISTS {repo.DB().database_name()} CASCADE")
