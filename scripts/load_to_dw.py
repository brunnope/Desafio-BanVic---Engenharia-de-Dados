import pandas as pd
import os
import glob
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
import hashlib


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DW_CONFIG = {
    'host': 'data-warehouse',
    'port': 5432,
    'database': 'dw_banvic',
    'user': 'data_engineer',
    'password': 'v3rysecur&pas5w0rd'
}

def get_dw_connection():

    try:
        conn_string = f"postgresql://{DW_CONFIG['user']}:{DW_CONFIG['password']}@{DW_CONFIG['host']}:{DW_CONFIG['port']}/{DW_CONFIG['database']}"
        engine = create_engine(conn_string)

        return engine
    
    except Exception as e:
        logger.error(f"Erro ao conectar no Data Warehouse: {str(e)}")
        raise

def batch_id(execution_date):
    return hashlib.md5(execution_date.encode()).hexdigest()[:8]

def load_to_dw(execution_date: str = None):
    try:

        if not execution_date:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        engine = get_dw_connection()

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw_metadata"))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_metadata.batches (
                    batch_id TEXT PRIMARY KEY,
                    execution_date DATE,
                    loaded_at TIMESTAMP DEFAULT now()
                )
            """))

            bid = batch_id(execution_date)

            exists = conn.execute(
                text("SELECT 1 FROM dw_metadata.batches WHERE batch_id=:b"),
                {'b': bid}
            ).fetchone()

            if exists:
                logger.info(f"Batch {bid} já processado. Pulando.")
                return

        year, month, day = execution_date.split('-')
        base = f'/opt/airflow/extracted_data/{year}-{month}-{day}'
        csvs = glob.glob(f"{base}/csv/*.csv") + glob.glob(f"{base}/sql/*.csv")

        total = 0
        for csv in csvs:
            df = pd.read_csv(csv)
            if df.empty:
                continue

            table_name = os.path.basename(csv).replace('.csv','')

            if '/csv/' in csv:
                table_name = f"csv_{table_name}"
            elif '/sql/' in csv:
                table_name = f"sql_{table_name}"

            df['_batch_id'] = bid
            df['_execution_date'] = execution_date


            with engine.begin() as conn:
                table_exists = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'staging'
                            AND table_name = :t
                        )
                    """),
                    {"t": table_name}
                ).scalar()

                if table_exists:
                    conn.execute(
                        text(f"DELETE FROM staging.{table_name} WHERE _batch_id=:b"),
                        {"b": bid}
                    )


            df.to_sql(f"staging.{table_name}", engine, if_exists='append', index=False)
            total += len(df)
            logger.info(f"{table_name}: {len(df)} registros carregados")

       
        engine.dispose()
        logger.info(f"Carregados {total} registros no batch {bid}")

        return {"batch_id": bid, "total": total}
    
    except Exception as e:
        logger.error(f"Erro no carregamento do DW: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    load_to_dw()