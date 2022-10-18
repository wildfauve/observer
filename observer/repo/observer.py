from typing import Optional
from pyspark.sql import dataframe, types
from pyspark.sql import functions as F
from delta.tables import *


class Observer:
    config_root = "observer"

    def __init__(self, db):
        self.db = db

    def read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return self.db.session.table(self.db_table_name())

    def filter_by_inputs_run_state(self, run_state, input_locations: List[str]) -> List[types.Row]:
        df = self.read()

        if not df:
            return []

        in_location_fn = (lambda locs: lambda x: x.isin(locs))(input_locations)

        return (df.filter(
            (df.run.hasRunState == run_state) & (F.exists(df.hasInputs.hasLocation, in_location_fn)))
                .select(df.run, df.hasInputs))

    def create_df(self, rows, schema):
        return self.db.session.createDataFrame(rows, schema)

    def upsert(self, df, *partition_columns):
        return (df.write.format(self.db.table_format)
                .partitionBy(partition_columns)
                .mode("append")
                .saveAsTable(self.db_table_name()))

    def table_exists(self) -> bool:
        return self.db.table_exists()

    def db_table_name(self):
        return self.db.db_table_name()

    def table_name(self):
        return self.db.table_name
