import pandas as pd
from prefect.client import Secret
from sqlalchemy import create_engine


class PostgresDataLoader:
    
    def __init__(self, port: int, db: str) -> None:
        self.db = db
        self.port = port
    
    def _get_connection_string(self) -> str:
        user = Secret("POSTGRES_USER").get()
        pwd = Secret("POSTGRES_PASS").get()
        return f"postgresql://{user}:{pwd}@localhost:{self.port}/{self.db}"


    def get_df_from_sql_query(self, table_or_query: str) -> pd.DataFrame:
        db = self._get_connection_string()
        engine = create_engine(db)
        return pd.read_sql(table_or_query, engine)


    def load_df_to_db(self, df: pd.DataFrame, table_name: str, schema: str = "raw_data") -> None:
        conn_string = self._get_connection_string()
        db_engine = create_engine(conn_string)
        conn = db_engine.connect()
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        df.head(n=0).to_sql(name=table_name, con=db_engine, if_exists='replace')
        df.to_sql(name=table_name, schema=schema, con=db_engine, index=False, if_exists='append')
        conn.close()