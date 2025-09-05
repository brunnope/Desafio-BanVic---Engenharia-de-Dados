import pandas as pd
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_csv_data(execution_date: str = None):

    try:
        
        if not execution_date:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        src = '/opt/airflow/data/transacoes.csv'
        year, month, day = execution_date.split('-')
        out_dir = f'/opt/airflow/extracted_data/{year}-{month}-{day}/csv'
        os.makedirs(out_dir, exist_ok=True)

        if not os.path.exists(src):
            raise FileNotFoundError(f"Arquivo não encontrado: {src}")

        df = pd.read_csv(src)
        if df.empty:
            raise ValueError("CSV vazio")

        out_file = f"{out_dir}/transacoes.csv"
        df.to_csv(out_file, index=False)
        logger.info(f"Extraído {len(df)} registros -> {out_file}")

        return out_file
    
    except Exception as e:
        logger.error(f"Erro na extração do CSV: {str(e)}")
        raise

if __name__ == "__main__":
    extract_csv_data()