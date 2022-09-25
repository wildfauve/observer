from typing import Tuple, Callable, Protocol
import pendulum
from rdflib import Namespace, URIRef
from uuid import uuid4
from functools import reduce
import pyspark

from observer import repo
from observer.util import error, monad, validate
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


class Hive(DataSet):
    dataset_namespace = None

    @classmethod
    def namespace(cls, namespace):
        cls.dataset_namespace = namespace
        return cls

    def namespace_uri(self):
        if not isinstance(self.dataset_namespace, Namespace):
            raise error.ObserverConfigError("Namespace not configured and not configured with correct type")
        return self.dataset_namespace


class HiveTable(Hive):
    type_of = Observable.sfo_lin.HiveTable

    def __init__(self, table_name, fully_qualified_name):
        self.table_name = table_name
        self.fully_qualified_name = fully_qualified_name

    def dataset_identity(self):
        return self.namespace_uri()

    def identity(self):
        return self.dataset_identity().term(self.fully_qualified_name)

    def to_props(self):
        return (
            self.uriref_to_str(self.identity()),
            self.uriref_to_str(self.type_of),
            self.fully_qualified_name,
            self.table_name
        )


class ObjectStore(DataSet):
    @classmethod
    def namespace(cls, namespace):
        cls.dataset_namespace = namespace
        return cls

    def dataset_identity(self):
        return self.namespace_uri()

    pass


class ObjectStoreFile(ObjectStore):
    type_of = Observable.sfo_lin.AzureDataLakeStoreFile

    def __init__(self, location):
        self.uuid = str(uuid4())
        self.location = location

    def identity(self):
        return self.dataset_namespace.term(self.uuid)

    def to_props(self):
        return (
            self.uriref_to_str(self.identity()),
            self.uriref_to_str(self.type_of),
            self.location,
            None  # name
        )


class Job(Observable):
    job_namespace = None

    @classmethod
    def namespace(cls, ns):
        cls.job_namespace = ns

    def namespace_uri(self):
        if not isinstance(self.job_namespace, Namespace):
            raise error.ObserverConfigError("Namespace not configured and not configured with correct type")
        return self.job_namespace


class SparkJob(Job):
    type_of = Observable.sfo_lin.SparkJob


class Run(Job):
    """
    A Spark job has the ability to process 0 or more batches or streams.  Each dataset process is an instance of a "Run".
    Each dataset contains trace (causal id) data.  While a Spark job may process many files, each file is not
    necessarily from the same trace, hence why the batch is the unit of work.
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

    def complete_and_emit(self):
        self.end_time = pendulum.now('UTC')
        self.parent_observer.emit()
        return self

    def job_identity(self):
        return self.job.namespace_uri()

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

class Emitter(Protocol):

    def __init__(self, session: pyspark.sql.session):
        ...

    def emit(self) -> monad.EitherMonad:
        ...

class ObserverHiveEmitter(Emitter):

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


class ObserverNoopEmitter(Emitter):

    @monad.monadic_try(error_cls=error.ObserverError)
    def emit(self, run: Run):
        pass


class Observer(Observable):
    """
    An Observer observes the individual Job run.  It holds the run's identity, but does not mediate the components
    of the run, that is the batches.
    """

    def __init__(self, env: str, job: Job, emitter: Emitter):
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
        return self.job.namespace_uri()

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



def observer_factory(env: str, job: Job, emitter: Emitter) -> Observer:
    return Observer(env=env, job=job, emitter=emitter)


def define_namespace(cls, uri: str) -> None:
    if not cls in [SparkJob, Hive, ObjectStore]:
        raise error.ObserverConfigError("Namespace must be configured on SparkJob, Hive, or ObjectStore")
    if not validate.valid_uri(uri):
        raise error.ObserverConfigError("Namespace must be a valid URI")
    cls.namespace(ns(uri))


def ns(uri: str) -> Namespace:
    return Namespace(uri)

def uri_ref(uri: str) -> URIRef:
    return URIRef(uri)
