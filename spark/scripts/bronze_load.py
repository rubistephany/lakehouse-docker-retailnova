from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("bronze-load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = builder.getOrCreate()

# Leemos el CSV
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/opt/input/ventas_raw.csv")
)

print("=== Datos RAW (primeras filas) ===")
df_raw.show()

# Guardamos en formato Delta en la carpeta Bronze
(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .save("/opt/data/bronze/ventas_delta")
)

print("Tabla Bronze creada con exito en /opt/data/bronze/ventas_delta")
spark.stop()
