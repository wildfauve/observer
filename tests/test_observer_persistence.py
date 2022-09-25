from tests.shared import *

from rdflib import Namespace, URIRef

from observer import observer
from observer import repo


class RunOfMySparkJob(observer.Run):
    pass


class MyInputHiveTable(observer.HiveTable):
    pass


class MyOutputHiveTable(observer.HiveTable):
    pass


def setup_module():
    observer.define_namespace(observer.Hive, 'https://example.nz/service/datasets/dataset/')
    observer.define_namespace(observer.SparkJob, 'https://example.nz/service/jobs/job/')
    observer.define_namespace(observer.ObjectStore, 'https://example.nz/service/datasets/batchFile/')
    repo.DB.init_session(spark_session_for_testing(), repo.CONFIG)



def it_persists_the_observer_to_hive_using_emit(init_db):
    emitter = observer.ObserverHiveEmitter(repo.DB.session)
    obs = create_obs(emitter)
    create_full_run_from_obs(obs)

    obs.emit()

    df = emitter.repo.read()

    rows = [row[0] for row in df.select(df.run).collect()]

    assert len(rows) == 1
    assert rows[0].type == 'https://example.nz/ontology/Lineage/SparkJob'
    assert rows[0].hasTrace == 'https://example.com/service/jobs/job/trace_uuid'


def it_persists_the_observer_on_completion(init_db):
    emitter = observer.ObserverHiveEmitter(repo.DB.session)
    obs = create_obs(emitter)

    job_run = create_full_run_from_obs(obs)

    job_run.complete_and_emit()

    df = emitter.repo.read()

    rows = [row[0] for row in df.select(df.run).collect()]

    assert len(rows) == 1
    assert rows[0].type == 'https://example.nz/ontology/Lineage/SparkJob'
    assert rows[0].hasTrace == 'https://example.com/service/jobs/job/trace_uuid'


#
#
#
def create_obs(emitter):
    return observer.observer_factory("test", observer.SparkJob(), emitter)


def create_run(session):
    return create_obs(session).run_factory(RunOfMySparkJob)


def create_full_run_from_obs(obs):
    run = obs.run_factory(RunOfMySparkJob)
    (run.start()
     .add_trace(URIRef('https://example.com/service/jobs/job/trace_uuid'))
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED")))

    return run


def create_full_run(session):
    run = create_run(session)
    (run.start()
     .add_trace(URIRef('https://example.com/service/jobs/job/trace_uuid'))
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED"))
     .complete())

    return run
