from pyspark.sql.types import StructType, StringType

from observer.domain import schema_util as su

ds_input_struct = StructType([
    su.at_id,
    su.at_type,
    su.build_string_field('run.sfo-lin:hasInput.sfo-lin:hasLocation', nullable=False),
    su.build_string_field('run.sfo-lin:hasInput.sfo-lin:hasName', nullable=True),
])

ds_output_struct = StructType([
    su.at_id,
    su.at_type,
    su.build_string_field('run.sfo-lin:hasOutputs.sfo-lin:hasLocation', nullable=False),
    su.build_string_field('run.sfo-lin:hasOutputs.sfo-lin:hasName', nullable=True),
])

run_struct = StructType([
    su.at_id,
    su.at_type,
    su.build_string_field('run.sfo-lin:isRunOf', nullable=False),
    su.build_string_field('run.sfo-lin:hasTrace', nullable=True),
    su.build_string_field('run.sfo-lin:hasStartTime', nullable=False),
    su.build_string_field('run.sfo-lin:hasEndTime', nullable=True),
    su.build_string_field('run.sfo-lin:hasRunState', nullable=True)
])

Run = su.build_struct_field('run', run_struct, nullable=False)

InputStorageDataSet = su.build_struct_field('run.sfo-lin:hasInput', ds_input_struct, nullable=True)
OutputsStorageDataSet = su.build_array_field('run.sfo-lin:hasOutputs', ds_output_struct, nullable=True)
Metrics = su.build_array_field('run.sfo-lin:hasMetrics', StringType(), nullable=True)

schema = StructType([
    Run,
    InputStorageDataSet,
    OutputsStorageDataSet,
    Metrics
])
