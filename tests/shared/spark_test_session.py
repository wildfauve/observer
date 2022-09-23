from typing import Callable
import pytest
import pyspark
from pyspark.sql import SparkSession
from delta import *

from observer.util import fn


@pytest.fixture
def spark_session_for_testing_fixture():
    return session(spark_delta_session, spark_session_config)


def spark_session_for_testing():
    return session(spark_delta_session, spark_session_config)


# Std Spark Session
def create_session():
    return SparkSession.builder.appName("CBORBuilder").enableHiveSupport().getOrCreate()


# Delta Spark Session
def spark_delta_session():
    return configure_spark_with_delta_pip(delta_builder()).getOrCreate()


def session(create_fn: Callable = create_session, config_adder_fn: Callable = fn.identity) -> SparkSession:
    sp = create_fn()
    config_adder_fn(sp)
    return sp


def delta_builder():
    return (pyspark.sql.SparkSession.builder.appName("test_delta_session")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))


def spark_session_config(spark):
    spark.conf.set('hive.exec.dynamic.partition', "true")
    spark.conf.set('hive.exec.dynamic.partition.mode', "nonstrict")
