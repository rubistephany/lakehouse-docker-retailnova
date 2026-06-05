from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder
    .appName("bronze-to-silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = builder.getOrCreate()

# 1. Leemos los datos que guardamos en la capa Bronze
df_bronze = spark.read.format("delta").load("/opt/data/bronze/ventas_delta")

# 2. Aplicamos la regla: Convertir fecha a tipo DATE
df_bronze = df_bronze.withColumn("fecha", F.to_date("fecha", "yyyy-MM-dd"))

# 3. Separamos los datos invalidos (Cuarentena)
df_invalid = df_bronze.filter(
    (F.col("unidades") <= 0) | (F.col("precio_total") <= 0)
).withColumn("quarantine_reason", F.lit("unidades <= 0 o precio_total <= 0"))

# 4. Separamos los datos validos y eliminamos duplicados por id_venta
df_valid = (
    df_bronze
    .filter((F.col("unidades") > 0) & (F.col("precio_total") > 0))
    .dropDuplicates(["id_venta"])
)

print("=== Silver valido (ventas_clean) ===")
df_valid.show()

print("=== Silver cuarentena (ventas_cuarentena) ===")
df_invalid.show()

# 5. Guardamos ambos resultados en sus carpetas Delta correspondientes
df_valid.write.format("delta").mode("overwrite").save("/opt/data/silver/ventas_clean")
df_invalid.write.format("delta").mode("overwrite").save("/opt/data/silver/ventas_cuarentena")

print("Capas Silver completadas con exito.")
spark.stop()
