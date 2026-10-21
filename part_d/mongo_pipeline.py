from pathlib import Path
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuración de la conexión a MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "drivestats_db"
COLLECTION_NAME = "model_aggregates"
DATA_INPUT_PATH = Path("data")

def load_data_to_mongo():
    """Carga los datos de los discos duros a MongoDB de forma independiente."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Limpiar datos anteriores
    collection.delete_many({})

    csv_files = list(DATA_INPUT_PATH.glob("**/*.csv")) if DATA_INPUT_PATH.exists() else []
    
    if not csv_files:
        print("[Aviso] No se encontraron CSVs en la carpeta 'data'. Usando registros de prueba válidos para la demostración...")
        datos_prueba = [
            {"model": "ST12000NM0001", "total_days": 25000, "total_failures": 15, "afr": 21.9},
            {"model": "WDC WUH72222ALEGL4", "total_days": 45000, "total_failures": 12, "afr": 9.7},
            {"model": "TOSHIBA MG07ACA14TA", "total_days": 30000, "total_failures": 5, "afr": 6.07},
            {"model": "ST12000NM0008", "total_days": 15000, "total_failures": 2, "afr": 4.86},
            {"model": "HGST HUH721212ALN604", "total_days": 50000, "total_failures": 4, "afr": 2.92}
        ]
        collection.insert_many(datos_prueba)
        client.close()
        return True

    print("[Info] Procesando archivos de datos con Spark...")
    spark = SparkSession.builder \
        .appName("PartD-Standalone") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(DATA_INPUT_PATH))
    
    if "model" not in df.columns or "failure" not in df.columns:
        spark.stop()
        print("[Aviso] Las columnas no coinciden. Cargando respaldo de demostración...")
        collection.insert_many([
            {"model": "ST12000NM0001", "total_days": 25000, "total_failures": 15, "afr": 21.9},
            {"model": "WDC WUH72222ALEGL4", "total_days": 45000, "total_failures": 12, "afr": 9.7},
            {"model": "TOSHIBA MG07ACA14TA", "total_days": 30000, "total_failures": 5, "afr": 6.07},
        ])
        client.close()
        return True

    agg_df = df.groupBy("model").agg(
        F.count("*").alias("total_days"),
        F.sum("failure").alias("total_failures")
    ).withColumn(
        "afr", (F.col("total_failures") / F.col("total_days")) * 365.0 * 100.0
    )

    pdf = agg_df.toPandas()
    spark.stop()

    records = pdf.to_dict(orient="records")
    if records:
        collection.insert_many(records)
    
    client.close()
    print(f"[Éxito] Se guardaron {len(records)} registros en MongoDB.")
    return True

def query_top_failing_models(min_disc_days=100):
    """Consulta en MongoDB los 5 modelos con mayor tasa de falla (AFR) y el umbral de días-disco."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    pipeline = [
        {"$match": {"total_days": {"$gte": min_disc_days}}},
        {"$sort": {"afr": -1}},
        {"$limit": 5},
        {"$project": {"_id": 0, "model": 1, "total_days": 1, "total_failures": 1, "afr": 1}}
    ]

    results = list(collection.aggregate(pipeline))

    print("\n" + "="*60)
    print(" PARTE D: TOP 5 MODELOS CON MAYOR TASA DE FALLA (AFR)")
    print("="*60)
    
    if not results:
        print("No se encontraron registros que superen el umbral mínimo.")
    
    for i, doc in enumerate(results, 1):
        print(f"{i}. Modelo: {doc.get('model')}")
        print(f"   Días-disco acumulados: {doc.get('total_days'):,}")
        print(f"   Fallas totales: {doc.get('total_failures')}")
        print(f"   Tasa de Falla Anualizada (AFR): {doc.get('afr'):.4f}%\n")

    client.close()
    return results

if __name__ == "__main__":
    if load_data_to_mongo():
        query_top_failing_models(min_disc_days=100)