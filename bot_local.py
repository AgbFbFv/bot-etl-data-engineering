import os
import sys
import logging
import json
from datetime import datetime
import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("DataEngineeringBot")

class Config:
    API_SOURCE_URL = "https://api.github.com/events"

class DataIngestorBot:
    def __init__(self, config):
        self.config = config

    def fetch_data_from_api(self):
        logger.info(f"Iniciando extracción desde API: {self.config.API_SOURCE_URL}")
        response = requests.get(self.config.API_SOURCE_URL, timeout=30)
        response.raise_for_status()
        return response.json()

    def save_raw(self, data, filename):
        os.makedirs("./raw_data", exist_ok=True)
        output_path = f"./raw_data/{filename}"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Datos crudos guardados en: {output_path}")
        return output_path

class LocalTransformationBot:
    def run_pipeline(self, input_json_path):
        logger.info(f"Procesando archivo con Pandas/Python: {input_json_path}")
        
        # Cargar JSON en un DataFrame
        with open(input_json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        df = pd.DataFrame(raw_data)
        
        # Transformación y Agregación equivalente a Spark SQL
        df['event_type'] = df['type'].str.upper()
        df['ingestion_timestamp'] = datetime.now()
        
        # Agrupación por tipo de evento
        summary_df = df.groupby('event_type').size().reset_index(name='total_events')
        summary_df = summary_df.sort_values(by='total_events', ascending=False)
        
        print("\n--- RESULTADO DE LA TRANSFORMACIÓN DE DATOS ---")
        print(summary_df.head(10).to_string(index=False))
        print("------------------------------------------------\n")
        
        os.makedirs("./processed_data", exist_ok=True)
        output_csv = "./processed_data/events_summary.csv"
        summary_df.to_csv(output_csv, index=False)
        logger.info(f"Resumen guardado exitosamente en: {output_csv}")
        return output_csv

class DataPipelineBot:
    def execute(self):
        logger.info("=== INICIANDO EJECUCIÓN DEL BOT DE DATOS ===")
        config = Config()
        ingestor = DataIngestorBot(config)
        
        raw_data = ingestor.fetch_data_from_api()
        file_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = ingestor.save_raw(raw_data, f"events_{file_ts}.json")

        transformer = LocalTransformationBot()
        transformer.run_pipeline(raw_file)

        logger.info("=== FINALIZADA EJECUCIÓN DEL BOT CON ÉXITO ===")

if __name__ == "__main__":
    bot = DataPipelineBot()
    bot.execute()
