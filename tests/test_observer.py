from rdflib import URIRef, Namespace

from observer import observer


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

    assert job_run.inputs[0].identity() == URIRef('https://example.nz/service/datasets/dataset/myDB.myTable')


def test_run_takes_multiple_inputs():
    job_run = create_run()

    job_run.has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
    job_run.has_input(dataset=MyInputHiveTable(table_name="myInputTable1", fully_qualified_name="myDB.myOutputTable1"))

    assert len(job_run.inputs) == 2


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


def it_generates_multiple_runs_from_the_same_observer():
    obs = create_obs()

    run1 = obs.run_factory(RunOfMySparkJob)
    run2 = obs.run_factory(RunOfMySparkJob)

    assert obs.runs == [run1, run2]


def it_builds_table_from_run():
    job_run = create_full_run()

    cells = job_run.to_table()

    assert len(cells) == 5

    run_time, run, inps, outputs, metrics = cells

    run_id, job_type, job_id, trace, t1, t2, state = run
    input_id, input_type, location, _ = inps[0]
    table_id, table_type, db_table_name, table_name = outputs[0]

    assert 'https://example.nz/service/jobs/job/' in run_id
    assert 'https://example.nz/service/datasets/batchFile/' in input_id
    assert table_id == 'https://example.nz/service/datasets/dataset/myDB.myOutputTable1'


def it_sets_trace_id():
    job_run = create_full_run()

    assert job_run.trace_id() == "https://example.com/service/jobs/job/trace_uuid"



#
#
#
def create_obs():
    emitter = observer.ObserverNoopEmitter()
    return observer.observer_factory("test", observer.SparkJob(), emitter)


def create_run(obs=None):
    if obs:
        return obs.run_factory(RunOfMySparkJob)
    return create_obs().run_factory(RunOfMySparkJob)


def create_full_run(run=None, obs=None):
    if not run:
        run = create_run(obs)
    (run.start()
     .add_trace('https://example.com/service/jobs/job/trace_uuid')
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_input(dataset=MyInputHiveTable(table_name="myInputTable1", fully_qualified_name="myDB.myInputTable1"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .has_output(dataset=MyOutputHiveTable2(table_name="myOutputTable2", fully_qualified_name="myDB.myOutputTable2"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED"))
     .complete())

    return run
