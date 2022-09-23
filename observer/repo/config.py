TABLE_FORMAT = 'delta'
DATABASE_NAME = 'portfolio_domain'
DATABASE_PATH = "dbfs:/user/hive/warehouse/portfolio_domain.db"

config = {
    'table_format': 'delta',
    'database_name': DATABASE_NAME,
    'db_path': DATABASE_PATH,
    'batch_source_folder': '/dbfs/mnt/cbor',
    'observer': {
        'table': 'observer',
        'fully_qualified': f"{DATABASE_NAME}.observer"
    }
}
