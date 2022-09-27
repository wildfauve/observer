from observer.util import fn


class Db:
    session = None
    db_name = None
    table_name = None
    table_format = None

    @classmethod
    def init_session(cls, session, db_name, table_name, table_format):
        if cls.session:
            return cls()
        cls.session = session
        cls.table_name = table_name
        cls.db_name = db_name
        cls.table_format = table_format
        return cls()

    def table_exists(self):
        return self.table_name in self.list_tables()

    def db_table_name(self):
        return f"{self.db_name}.{self.table_name}"

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.db_name)]
