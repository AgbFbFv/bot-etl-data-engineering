import os
import sys
import logging
import json
from datetime import datetime, timedelta
import requests

# Importación de librerías para PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, upper
except ImportError:
    print("Warning: PySpark no esta instalado localmente. Ejecutando en modo prueba.")

# Configuración de Logging
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

class PySparkTransformationBot:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("GCP_PySpark_ETL_Bot") \
            .getOrCreate()

    def run_pipeline(self, input_json_path):
        logger.info(f"Procesando archivo con PySpark: {input_json_path}")
        df_raw = self.spark.read.option("multiline", "true").json(input_json_path)
        
        df_cleaned = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                           .withColumn("event_type", upper(col("type")))

        df_cleaned.createOrReplaceTempView("raw_events")
        df_aggregated = self.spark.sql("""
            SELECT 
                event_type,
                COUNT(id) as total_events
            FROM raw_events
            GROUP BY event_type
        """)
        
        df_aggregated.show(5)
        output_parquet = "./processed_data/events_summary.parquet"
        df_aggregated.write.mode("overwrite").parquet(output_parquet)
        return output_parquet

    def stop(self):
        self.spark.stop()

class DataPipelineBot:
    def execute(self):
        logger.info("=== INICIANDO EJECUCIÓN DEL BOT DE DATOS ===")
        config = Config()
        ingestor = DataIngestorBot(config)
        
        raw_data = ingestor.fetch_data_from_api()
        file_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = ingestor.save_raw(raw_data, f"events_{file_ts}.json")

        transformer = PySparkTransformationBot()
        try:
            transformer.run_pipeline(raw_file)
        finally:
            transformer.stop()

        logger.info("=== FINALIZADA EJECUCIÓN DEL BOT ===")

if __name__ == "__main__":
    bot = DataPipelineBot()
    bot.execute()
