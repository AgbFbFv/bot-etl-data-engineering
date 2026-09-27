# Enterprise ETL Data Pipeline (GCP & Airflow Ready)

Pipeline de Extracción, Transformación y Carga (ETL) modular en Python para ingesta de APIs en tiempo real, orquestado con Apache Airflow y validado mediante Integración Continua (CI/CD).

---

## Arquitectura del Sistema

```text
[ API REST ] --(Extract)--> [ Raw JSON ] --(Transform)--> [ Pandas DataFrames ] --(Load)--> [ SQLite / BigQuery ]
                                                                                               +--> [ CSV Reports ]
bot-etl-data-engineering/
+-- .github/workflows/ci.yml # Pipeline de Integración Continua (GitHub Actions)
+-- dags/etl_github_dag.py   # DAG de Apache Airflow
+-- raw_data/                 # Almacenamiento de JSONs crudos
+-- processed_data/           # Exportación de reportes procesados
+-- bot_local.py              # Clase principal ETLPipeline (Extract, Transform, Load)
+-- test_pipeline.py          # Pruebas unitarias automatizadas con pytest
+-- etl_data.db               # Base de datos relacional SQLite
+-- requirements.txt          # Gestión de dependencias
+-- README.md                 # Documentación del proyectos