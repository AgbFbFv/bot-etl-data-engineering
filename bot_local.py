import os
import requests
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine

# Configuración de Logs profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("DataEngineeringBot")

class ETLPipeline:
    def __init__(self, api_url, raw_dir, processed_dir, db_path):
        self.api_url = api_url
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.db_engine = create_engine(f"sqlite:///{db_path}")
        
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def extract(self):
        """Etapa 1: Extract - Ingesta de API REST"""
        logger.info(f"Iniciando extracción desde API: {self.api_url}")
        response = requests.get(self.api_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file_path = os.path.join(self.raw_dir, f"events_{timestamp}.json")

        import json
        with open(raw_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        logger.info(f"Datos crudos guardados en: {raw_file_path}")
        return raw_file_path, data

    def transform(self, data):
        """Etapa 2: Transform - Procesamiento y Agregación con Pandas"""
        logger.info("Iniciando transformación de datos con Pandas...")
        df = pd.DataFrame(data)
        
        if 'type' not in df.columns:
            logger.warning("El campo 'type' no fue encontrado en los datos de entrada.")
            df['type'] = 'UNKNOWN_EVENT'

        summary_df = df.groupby('type').size().reset_index(name='total_events')
        summary_df.rename(columns={'type': 'event_type'}, inplace=True)
        summary_df['created_at'] = datetime.now()

        logger.info("Transformación finalizada exitosamente.")
        return summary_df

    def load(self, summary_df):
        """Etapa 3: Load - Exportación a CSV y Carga en Base de Datos SQL"""
        csv_path = os.path.join(self.processed_dir, "events_summary.csv")
        summary_df.to_csv(csv_path, index=False)
        logger.info(f"Resumen guardado en CSV: {csv_path}")

        summary_df.to_sql(
            name="metrics_events_summary",
            con=self.db_engine,
            if_exists="append",
            index=False
        )
        logger.info("Resumen insertado exitosamente en la tabla SQL 'metrics_events_summary'.")

if __name__ == "__main__":
    logger.info("=== INICIANDO EJECUCIÓN DEL BOT DE DATOS (ETL PIPELINE) ===")
    
    pipeline = ETLPipeline(
        api_url="https://api.github.com/events",
        raw_dir="./raw_data",
        processed_dir="./processed_data",
        db_path="etl_data.db"
    )

    _, raw_data = pipeline.extract()
    processed_df = pipeline.transform(raw_data)
    pipeline.load(processed_df)

    logger.info("=== FINALIZADA EJECUCIÓN DEL BOT CON ÉXITO ===")