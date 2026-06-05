from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("gold-to-csv")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = builder.getOrCreate()

# 1. Leemos los datos finales desde la capa Gold
df_gold = spark.read.format("delta").load("/opt/data/gold/reporte_ventas")

# 2. Guardamos el reporte en un solo archivo CSV limpio
# Usamos .coalesce(1) para que junte todo en un unico archivo CSV
(
    df_gold.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv("/opt/data/gold/reporte_ventas_csv")
)

print("Reporte exportado a CSV correctamente en la capa Gold.")
spark.stop()
