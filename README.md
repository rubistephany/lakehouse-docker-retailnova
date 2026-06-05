@'
# Lakehouse Moderno con Arquitectura Medallion en Docker 🐳✨

Este repositorio contiene la implementación completa de un entorno de **Lakehouse** corporativo utilizando una **Arquitectura Medallion (Bronze, Silver y Gold)**. El objetivo del proyecto es procesar, limpiar y agregar datos de ventas simulando un pipeline de ingeniería de datos real de nivel empresarial.

---

## 🏗️ Arquitectura del Pipeline (Flujo Medallion)

El pipeline de datos procesa la información de manera incremental a través de tres capas lógicas montadas sobre un motor de almacenamiento **Delta Lake**:

1. **Capa Bronze (Raw Data) 🥉:**
   - Se realiza la ingesta directa del archivo fuente `ventas.csv` sin aplicar transformaciones ni alterar su estructura original.
   - Guardado en formato **Delta** con histórico completo en `/opt/data/bronze/ventas_delta`.

2. **Capa Silver (Cleansing & Quarantine) 🥈:**
   - **Limpieza:** Conversión y tipado de campos de fechas a formato estándar (`yyyy-MM-dd`) y eliminación de registros duplicados por la llave primaria `id_venta`.
   - **Estrategia de Cuarentena:** Filtrado y aislamiento automatizado de datos corruptos o inconsistentes (ventas con `unidades <= 0` o `precio_total <= 0`). 
   - Los datos limpios se guardan en `ventas_clean` y las anomalías de negocio se desvían a `ventas_cuarentena`.

3. **Capa Gold (Business Insights) 🏆:**
   - Consolidación y agregación de métricas de rendimiento para el equipo de negocio.
   - Cálculo del **total de ingresos monetarios** y **total de unidades vendidas** agrupado por cada tipo de `producto`, ordenado de mayor a menor beneficio.
   - Exportación final optimizada a formato tradicional **CSV unificado** para el consumo inmediato por analistas de BI (Power BI, Tableau) o herramientas de oficina.

---

## 🛠️ Stack Tecnológico Utilizado

- **Engine de Procesamiento:** Apache Spark (PySpark v3.x)
- **Formato de Almacenamiento:** Delta Lake (v3.1.0) con soporte ACID, Time Travel y optimización Parquet.
- **Orquestación e Infraestructura:** Docker & Docker Compose (Contenedorización aislada).
- **Entorno de Consola:** Windows PowerShell & Git Control de Versiones.

---

## 📂 Estructura del Repositorio

```text
├── docker-compose.yml          # Configuración del entorno Spark + Delta Lake
├── data/                       # Volumen local compartido para el almacenamiento de las capas
│   ├── bronze/                 # Datos brutos en formato Delta
│   ├── silver/                 # Datos limpios y registros en cuarentena
│   └── gold/                   # Reporte agregado y exportación CSV de negocio
└── spark/
    └── scripts/                # Scripts de ingeniería de datos (PySpark)
        ├── load_to_bronze.py   # Ingesta inicial CSV -> Delta Bronze
        ├── bronze_to_silver.py # Limpieza, deduplicación y aislamiento de errores
        ├── silver_to_gold.py   # Reporte de métricas de negocio agregadas
        └── gold_to_csv.py      # Exportación del datamart para consumo tradicional
