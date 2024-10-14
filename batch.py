from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

# 1. Inicializar la sesión de Spark con configuraciones
spark = SparkSession.builder \
    .appName("Procesamiento Batch") \
    .master("spark://10.10.20.100:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# 2. Cargar el conjunto de datos desde un archivo CSV (o archivo de texto)
log_data = spark.read.csv("hdfs://nodo1:9820/weblog.csv", header=False, inferSchema=True)

# Renombrar las columnas para que sean más claras
log_data = log_data.withColumnRenamed("_c0", "IP") \
                   .withColumnRenamed("_c1", "Time") \
                   .withColumnRenamed("_c2", "URL") \
                   .withColumnRenamed("_c3", "Status")

# 3. Operaciones de limpieza y transformación

# Eliminar filas con valores nulos
log_data_clean = log_data.na.drop()

# Extraer la fecha de la columna 'Time' usando expresiones regulares
log_data_clean = log_data_clean.withColumn("Date", regexp_extract(col("Time"), r'\[(.*?)\:', 1))

# Convertir el campo 'Status' a entero
log_data_clean = log_data_clean.withColumn("Status", col("Status").cast("int"))

# Filtrar solo las solicitudes con estado HTTP 200 (exitosas)
successful_logs = log_data_clean.filter(log_data_clean.Status == 200)

# Mostrar los primeros registros para verificar la limpieza
successful_logs.show(5)

# 4. Análisis exploratorio (EDA)

# Conteo de IPs únicas
unique_ips = log_data_clean.select("IP").distinct().count()
print(f"Total de IPs únicas: {unique_ips}")

# Frecuencia de las URLs solicitadas, ordenado por cantidad de solicitudes
url_frequencies = log_data_clean.groupBy("URL").count().orderBy("count", ascending=False)
url_frequencies.show(10)

# 5. Almacenar los resultados procesados

# Guardar los datos limpios en formato CSV
log_data_clean.write.csv("/tmp/logs_procesados.csv", header=True)

# Alternativamente, guardar en formato Parquet (más eficiente para grandes volúmenes de datos)
log_data_clean.write.parquet("/tmp/logs_procesados.parquet")

# Finalizar la sesión de Spark
spark.stop()

