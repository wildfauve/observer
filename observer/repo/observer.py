from typing import Optional
from pyspark.sql import dataframe, types
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

        return (df.filter(
            (df.run.hasRunState == run_state) & (df.hasInput.hasLocation.isin(input_locations)))
                .select(df.hasInput.hasLocation)
                .collect())

    def create_df(self, rows, schema):
        return self.db.session.createDataFrame(rows, schema)

    def upsert(self, df):
        return (df.write.format(self.db.table_format())
                .mode("append")
                .saveAsTable(self.db_table_name()))

    def table_exists(self) -> bool:
        return self.db.table_exists(self.table_name())

    def db_table_name(self):
        return self.db.db_table_name(self.config_root)

    def table_name(self):
        return self.db.table_name(self.config_root)
