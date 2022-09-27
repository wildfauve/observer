import pytest

from tests.shared import *

from observer import repo


@pytest.fixture
def init_db(spark_session_for_testing_fixture):
    spark_session_for_testing_fixture.sql("create database IF NOT EXISTS observerDB")
    yield
    spark_session_for_testing_fixture.sql("drop database IF EXISTS observerDB CASCADE")
