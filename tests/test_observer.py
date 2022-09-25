from rdflib import URIRef, Namespace

from observer import observer


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


def it_creates_an_observable():
    emitter = observer.ObserverNoopEmitter()

    obs = observer.observer_factory("test", observer.SparkJob(), emitter)

    assert obs.observer_identity() == Namespace('https://example.nz/service/jobs/job/')


def it_created_a_run_from_the_observer():
    obs = create_obs()

    job_run = obs.run_factory(RunOfMySparkJob)

    assert "https://example.nz/service/jobs/job/" in str(job_run.identity())


def it_adds_a_run_input():
    job_run = create_run()

    input_table = MyInputHiveTable(table_name="myTable", fully_qualified_name="myDB.myTable")

    job_run.has_input(dataset=input_table)

    assert job_run.input.identity() == URIRef('https://example.nz/service/datasets/dataset/myDB.myTable')


def it_adds_multiple_run_outputs():
    job_run = create_run()

    output_table1 = MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myTable1")
    output_table2 = MyOutputHiveTable(table_name="myOutputTable2", fully_qualified_name="myDB.myTable2")

    job_run.has_output(dataset=output_table1)
    job_run.has_output(dataset=output_table2)

    outputs = set([output.identity() for output in job_run.outputs])

    expected_outputs = set([URIRef('https://example.nz/service/datasets/dataset/myDB.myTable1'),
                            URIRef('https://example.nz/service/datasets/dataset/myDB.myTable2')])

    assert outputs == expected_outputs


def it_builds_table_from_run():
    job_run = create_full_run()

    cells = job_run.to_table()

    assert len(cells) == 1

    run, inp, outputs, metrics = cells[0]

    run_id, job_type, job_id, trace, t1, t2, state = run
    input_id, input_type, location, _ = inp
    table_id, table_type, db_table_name, table_name = outputs[0]

    assert 'https://example.nz/service/jobs/job/' in run_id
    assert 'https://example.nz/service/datasets/batchFile/' in input_id
    assert table_id == 'https://example.nz/service/datasets/dataset/myDB.myOutputTable1'


#
#
#
def create_obs():
    emitter = observer.ObserverNoopEmitter()
    return observer.observer_factory("test", observer.SparkJob(), emitter)


def create_run():
    return create_obs().run_factory(RunOfMySparkJob)


def create_full_run():
    run = create_run()
    (run.start()
     .add_trace(observer.uri_ref('https://example.com/service/jobs/job/trace_uuid'))
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED"))
     .complete())

    return run
