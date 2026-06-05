from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder
    .appName("silver-to-gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = builder.getOrCreate()

# 1. Leemos los datos limpios de la capa Silver
df_silver = spark.read.format("delta").load("/opt/data/silver/ventas_clean")

# 2. Agrupamos por producto (corregido) y calculamos los totales agregados
df_gold = (
    df_silver
    .groupBy("producto")
    .agg(
        F.sum("unidades").alias("total_unidades"),
        F.round(F.sum("precio_total"), 2).alias("total_ingresos")
    )
    .orderBy(F.desc("total_ingresos"))
)

print("=== Capa Gold: Reporte de Ventas por Producto ===")
df_gold.show()

# 3. Guardamos el resultado de negocio en la capa Gold
df_gold.write.format("delta").mode("overwrite").save("/opt/data/gold/reporte_ventas")

print("Capa Gold completada con exito.")
spark.stop()
