from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

DATABASE_URI = "mssql+pyodbc://sa:XcO3bDsymu-A746-WA8rbkhX!2P_G@mssql-poc-u8681.vm.elestio.app:18698/Square?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# Define tables
customer_history_table = Table(
    'customer_history', metadata,
    autoload_with=engine,
    schema='Yotpo'
)

orders_table = Table(
    'order_ids', metadata,
    autoload_with=engine,
    schema='Yotpo'
)

history_items_table = Table(
    'history_items', metadata,
    autoload_with=engine,
    schema='Yotpo'
)
