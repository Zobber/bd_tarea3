# Big Data - Tarea 3 - UNAD

Se tiene un conjunto de datos en el cual se basa en los logs de un servidor web, por lo anterior se requiere analizar cuáles son las cantidades peticiones que se realizan desde “x” dirección IP para así tener monitoreado ya que puede representar un riesgo de seguridad. En este orden de ideas se ha realizado con Python, Apache Spark y Kafka para realizar las tareas de analisis como procesamiento batch tales como: cargar, limpiar, transformar, analizar y almacenar los resultados. Se utilizará DataFrames de Spark.


# Ejecución

Para trabajar con Spark usamos el script de python **batch.py** y ejecutamos en la terminal una vez que se haya subido el conjunto de datos correspondiente:

`/opt/spark/bin/spark-submit --master spark://10.10.20.100:7077 /opt/spark/batch.py`

![image](https://github.com/user-attachments/assets/1a2e7bd5-5c58-4362-ad59-5ca300dd3d6b)

Lo anterior, creará los directorios como archivos resultantes de acuerdo al procesamiento del analisis ejecutado en el script

