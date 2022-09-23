from typing import Tuple, Callable
import pendulum
from rdflib import Namespace, URIRef
from uuid import uuid4
from functools import reduce

from observer import repo
from observer.util import error, monad
from observer.domain import schema, metrics, structure

RunColumn = structure.Column(schema=schema.Run)
InputsColumn = structure.Column(schema=schema.InputStorageDataSet)
OutputsColumn = structure.Column(schema=schema.OutputsStorageDataSet)
MetricsColumn = structure.Column(schema=schema.Metrics)


class Observable:
    sfo_lin = Namespace("https://example.nz/ontology/Lineage/")

    def __init__(self):
        pass

    def uriref_to_str(self, ref: URIRef) -> str:
        return ref.toPython()


class DataSet(Observable):
    type_of = None


class HiveTable(DataSet):
    type_of = Observable.sfo_lin.HiveTable
    dataset_identity = None

    @classmethod
    def namespace(cls, ns):
        cls.dataset_identity = ns

    def __init__(self, table_name, fully_qualified_name):
        self.table_name = table_name
        self.fully_qualified_name = fully_qualified_name

    def identity(self):
        return self.dataset_identity.term(self.fully_qualified_name)

    def to_props(self):
        return (
            self.uriref_to_str(self.identity()),
            self.uriref_to_str(self.type_of),
            self.fully_qualified_name,
            self.table_name
        )


class StoreFileLocation(DataSet):
    type_of = Observable.sfo_lin.AzureDataLakeStoreFile
    dataset_identity = None

    @classmethod
    def namespace(cls, ns):
        cls.dataset_identity = ns


class StoreFile(StoreFileLocation):

    def __init__(self, location):
        self.uuid = str(uuid4())
        self.location = location

    def identity(self):
        return self.dataset_identity.term(self.uuid)

    def to_props(self):
        return (
            self.uriref_to_str(self.identity()),
            self.uriref_to_str(self.type_of),
            self.location,
            None  # name
        )


class Job(Observable):
    job_identity = None

    @classmethod
    def namespace(cls, ns):
        cls.job_identity = ns


class SparkJob(Job):
    type_of = Observable.sfo_lin.SparkJob


class Run(Observable):
    """
    A Spark job has the ability to process 0 or more batch filestructure.  Each batch file process is an instance of a "Run".
    Each batch file contains trace (causal id) data.  While a Spark job may process many files, each file is not necessarily from
    the same trace, hence why the batch is the unit of work.
    """

    def __init__(self, job, parent_observer=None):
        self.uuid = str(uuid4())
        self.trace = None
        self.job = job
        self.parent_observer = parent_observer
        self.current_state = None
        self.state_transitions = []
        self.outputs = []
        self.metrics = {}
        self.start_time = None
        self.end_time = None
        self.input = None

    def add_trace(self, trace):
        self.trace = trace
        return self

    def trace_id(self):
        if not self.trace:
            return None
        return self.coerse_uri(self.trace)

    def start(self):
        self.start_time = pendulum.now('UTC')
        return self

    def complete(self):
        self.end_time = pendulum.now('UTC')
        return self

    def job_identity(self):
        return self.job.job_identity

    def identity(self):
        return self.job_identity().term(self.uuid)

    def has_input(self, dataset: DataSet):
        self.input = dataset
        return self

    def with_state_transition(self, transition_fn: Callable):
        new_current_state, event = transition_fn(self.current_state)
        self.state_transitions.append({'from': self.current_state, 'with': event, 'to': new_current_state})
        self.current_state = new_current_state
        return self

    def has_output(self, dataset: DataSet):
        self.outputs.append(dataset)
        return self

    def input_as_props(self) -> Tuple:
        return (structure.Cell(column=InputsColumn, props=self.input.to_props())),

    def outputs_as_props(self):
        return (structure.Cell(column=OutputsColumn, props=[output.to_props() for output in self.outputs])),

    def metric_factory(self, metric_type, name, **kwargs):
        """
        Creates and returns an initialised metric.  The metric is saved against the run, and can be used outside it.
        If the metric already exists by name, it is returned rather than recreated.

        :param metric_type:  The type must be supported by a create_<metric_type> function on the provider.
        :param name: name of the metric
        :param kwargs: Must be valid additonal args to pass to the metric's create function.
        :return:
        """
        if self.metrics.get(name, None):
            return self.metrics.get(name)
        metric = getattr(self.metric_provider(), "create_{}".format(metric_type))(**{**{'name': name}, **kwargs})
        self.metrics[name] = metric
        self.all_metrics_dimensions()
        return metric

    def metric_by_name(self, name):
        return self.metrics.get(name, None)

    def metric_provider(self):
        return metrics.metric_provider()

    def all_metrics_dimensions(self):
        return reduce(self.metric_dimensions, self.metrics.items(), {})

    def metric_dimensions(self, acc, metric: Tuple[str, Callable]):
        metric_name, metric_fn = metric
        acc.update({metric_name: metric_fn.dimensions})
        return acc

    def collect_metrics(self):
        return (structure.Cell(column=MetricsColumn, props=[obj.to_json() for name, obj in self.metrics.items()]),)

    def build_rows(self, rows):
        return [tuple(cell.props for cell in row) for row in rows]

    def to_table(self):
        return self.build_rows([self.to_props()])

    def to_props(self):
        run_cell = structure.Cell(column=RunColumn, props=(
            self.uriref_to_str(self.identity()),
            self.uriref_to_str(self.job.type_of),
            str(self.job_identity()),
            self.uriref_to_str(self.trace) if self.trace else None,
            self.start_time.to_iso8601_string() if self.start_time else None,
            self.end_time.to_iso8601_string() if self.end_time else None,
            self.current_state))

        return (run_cell,) + self.input_as_props() + self.outputs_as_props() + self.collect_metrics()

    def __str__(self):
        return """
        UUID: {uuid}
        Identity: {idt}
        JobIdentity: {jid}
        StartTime: {st}
        EndTime: {et}
        Trace: {sp}
        StateTransitions: {stt}
        """.format(uuid=self.uuid,
                   idt=self.uriref_to_str(self.identity()),
                   jid=self.job_identity(),
                   st=self.start_time,
                   et=self.end_time,
                   sp=self.trace,
                   stt=self.state_transitions)

    def serialise(self):
        """
        Required for the logger interface.  Collects both observer and run info.
        """
        obs_data = self.parent_observer.serialise() if self.parent_observer else {}
        return {**{'trace_id': self.trace_id()}, **obs_data}


class Observer(Observable):
    """
    An Observer observes the individual Job run.  It holds the run's identity, but does not mediate the components
    of the run, that is the batches.
    """

    def __init__(self, env, job: Job, emitter):
        self.env = env
        self.job = job
        self.trace_id = str(uuid4())
        self.emitter = emitter
        self.run = None

    def emit(self, run: Run = None):
        if not self.emitter:
            return self
        if run:
            self.emitter.emit(run)
        else:
            self.emitter.emit(self.run)
        return self

    def observer_identity(self):
        return self.job.job_identity

    def identity(self):
        return self.observer_identity().term(self.trace_id)

    def run_factory(self, run):
        self.run = run(job=self.job, parent_observer=self)
        return self.run

    def serialise(self):
        """
        Required for the logger interface
        """
        return {'env': self.env,
                'trace_id': self.uriref_to_str(self.identity()),
                'time': pendulum.now().to_iso8601_string()}


class ObserverHiveEmitter:

    def __init__(self, session):
        if not self.session_is_spark_session(session):
            raise error.ObserverConfigError('Session provided is not a Spark Session.  Reconfigure Hive Emitter')
        self.repo = repo.REPO(repo.DB.init_session(session, repo.CONFIG))

    @monad.monadic_try(error_cls=error.ObserverError)
    def emit(self, run: Run):
        return self.repo.upsert(self.create_df(run))

    def create_df(self, run: Run):
        return self.repo.create_df(run.to_table(), schema.schema)

    def session_is_spark_session(self, session):
        return hasattr(session, 'createDataFrame')


class ObserverNoopEmitter:

    @monad.monadic_try(error_cls=error.ObserverError)
    def emit(self, run: Run):
        pass


def observer_factory(env, job, emitter) -> Observer:
    return Observer(env=env, job=job, emitter=emitter)
