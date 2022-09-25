import pytest
from rdflib import Namespace

from observer import observer
from observer.util import error


class MyInputHiveTable(observer.HiveTable):
    pass


def it_sets_namespace_for_job():
    observer.define_namespace(observer.SparkJob, "https://example.nz/jobs/job")

    spark_run = observer.Run(observer.SparkJob())

    assert spark_run.job_identity() == Namespace("https://example.nz/jobs/job")


def it_sets_namespace_for_hive_table():
    observer.define_namespace(observer.Hive, 'https://example.nz/service/datasets/dataset/')

    table = MyInputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1")

    assert table.dataset_identity() == Namespace('https://example.nz/service/datasets/dataset/')


def it_raises_a_config_error_when_namespace_not_uri():
    with pytest.raises(error.ObserverConfigError):
        observer.define_namespace(observer.Hive, 'not-a_uri')

def it_raises_a_config_error_when_namespace_applied_to_wrong_class():
    with pytest.raises(error.ObserverConfigError):
        observer.define_namespace(observer.DataSet, 'not-a_uri')
