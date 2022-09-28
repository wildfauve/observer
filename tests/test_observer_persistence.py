from tests.shared import *

from observer import observer
from observer import repo


class RunOfMySparkJob(observer.Run):
    pass


class MyInputHiveTable(observer.HiveTable):
    pass


class MyOutputHiveTable(observer.HiveTable):
    pass


class MyOutputHiveTable2(observer.HiveTable):
    pass


def setup_module():
    observer.define_namespace(observer.Hive, 'https://example.nz/service/datasets/dataset/')
    observer.define_namespace(observer.SparkJob, 'https://example.nz/service/jobs/job/')
    observer.define_namespace(observer.ObjectStore, 'https://example.nz/service/datasets/batchFile/')


def it_persists_the_observer_to_hive_using_emit(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)
    create_full_run(obs)

    obs.emit()

    df = emitter.repo.read()

    rows = [row[0] for row in df.select(df.run).collect()]

    assert len(rows) == 1
    assert rows[0].type == 'https://example.nz/ontology/Lineage/SparkJob'
    assert rows[0].hasTrace == 'https://example.com/service/jobs/job/trace_uuid'


def it_persists_the_observer_on_completion(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    job_run = create_full_run(obs)

    job_run.complete_and_emit()

    df = emitter.repo.read()

    assert df.count() == 1

    rows = [row[0] for row in df.select(df.run).collect()]

    assert len(rows) == 1
    assert rows[0].type == 'https://example.nz/ontology/Lineage/SparkJob'
    assert rows[0].hasTrace == 'https://example.com/service/jobs/job/trace_uuid'


def it_persists_a_single_run(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    job_run1 = create_full_run(obs)
    _job_run2 = create_full_run(obs)

    job_run1.complete_and_emit()

    df = emitter.repo.read()

    assert df.count() == 1


def it_emits_multiple_inputs_and_outputs(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    job_run = create_full_run(obs)

    job_run.complete_and_emit()

    df = obs.read()

    row = df.select(df.hasInputs, df.hasOutputs).collect()[0]

    assert len(row[0]) == 2
    assert len(row[1]) == 2


def create_obs(emitter):
    return observer.observer_factory("test", observer.SparkJob(), emitter)


#
#
#
def it_builds_multiple_rows_from_multiple_runs(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    _job_run1 = create_full_run(obs)
    _job_run2 = create_full_run(obs)

    obs.emit()

    df = obs.read()

    assert df.count() == 2


def it_idempotently_emits_runs(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    job_run1 = create_full_run(obs)
    _job_run2 = create_full_run(obs)

    job_run1.complete_and_emit()

    obs.emit()

    df = obs.read()

    assert df.count() == 2


def test_read_observer_by_state_and_run(spark_session_for_testing_fixture, init_db):
    emitter = observer.ObserverHiveEmitter(session=spark_session_for_testing_fixture,
                                           db_name='observerdb',
                                           table_name='observertable',
                                           table_format="delta")
    obs = create_obs(emitter)

    job_run1 = create_full_run(obs)
    job_run2 = create_full_run(obs)

    obs.emit()

    df = obs.filter_by_inputs_run_state(run_state='STATE_COMPLETE', input_locations=['file_loc'])

    rows = df.select(df.hasInputs.hasLocation).collect()

    assert len(rows) == 2
    assert [row[0][0] for row in rows] == ['file_loc', 'file_loc']




def create_run(obs=None, emitter=None):
    if obs:
        return obs.run_factory(RunOfMySparkJob)
    return create_obs(emitter).run_factory(RunOfMySparkJob)


def create_full_run(obs=None, emitter=None):
    run = create_run(obs, emitter)
    (run.start()
     .add_trace('https://example.com/service/jobs/job/trace_uuid')
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_input(dataset=MyInputHiveTable(table_name="myInputTable1", fully_qualified_name="myDB.myInputTable1"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .has_output(dataset=MyOutputHiveTable2(table_name="myOutputTable2", fully_qualified_name="myDB.myOutputTable2"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED"))
     .complete())

    return run
