from observer.util import fn


class Db:
    table_config_term = "table"
    db_table_config_term = "fully_qualified"
    session = None
    config = None

    @classmethod
    def init_session(cls, session, config):
        if cls.session and cls.config:
            return cls()
        cls.session = session
        cls.config = config
        return cls()

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.database_name())]

    def database_name(self):
        return fn.deep_get(self.config, ['database_name'])

    def table_name(self, root):
        return fn.deep_get(self.config, [root, self.table_config_term])

    def db_table_name(self, root):
        return fn.deep_get(self.config, [root, self.db_table_config_term])

    def table_format(self):
        return fn.deep_get(self.config, ['table_format'])

    def db_path(self):
        return fn.deep_get(self.config, ['db_path'])
