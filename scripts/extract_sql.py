import pandas as pd
import os
from datetime import datetime
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DB_CONFIG = {
    'host': 'db-fonte',
    'port': 5432,
    'database': 'banvic',
    'user': 'data_engineer',
    'password': 'v3rysecur&pas5w0rd'
}

def get_connection():
    
    try:
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        return engine
    
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {str(e)}")
        raise

def extract_sql_data(execution_date: str = None):

    try:

        if not execution_date:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        year, month, day = execution_date.split('-')
        out_dir = f'/opt/airflow/extracted_data/{year}-{month}-{day}/sql'
        os.makedirs(out_dir, exist_ok=True)

        engine = get_connection()

        tables = pd.read_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
            engine
        )['table_name'].tolist()

        if not tables:
            raise ValueError("Nenhuma tabela encontrada no banco")

        extracted = []

        for t in tables:
            df = pd.read_sql(f"SELECT * FROM {t}", engine)
            path = f"{out_dir}/{t}.csv"
            df.to_csv(path, index=False)
            extracted.append(path)
            logger.info(f"{t}: {len(df)} registros")

        return extracted
    
    except Exception as e:
        logger.error(f"Erro na extração SQL: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    extract_sql_data()