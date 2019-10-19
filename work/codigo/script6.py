from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType,DateType
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objs as go
import pandas as pd



schema = StructType([
    StructField("Region", StringType()),
    StructField("Pais", StringType()),
    StructField("Departamento", StringType()),
    StructField("Canal Venta", StringType()),
    StructField("Prioridad Orden", StringType()),
    StructField("Fecha Orden", StringType()),
    StructField("Id Orden", StringType()),
    StructField("Fecha Envio", StringType()),
    StructField("Unidades Vendidas", IntegerType()),
    StructField("Precio Unidad", DoubleType()),
    StructField("Costo Unidad", DoubleType()),
    StructField("Ingreso Total", DoubleType()),
    StructField("Costo Total", DoubleType()),
    StructField("Ganancia Total", DoubleType()),

])


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("VENTAS.csv",header=True,sep=",",schema=schema)
df.createOrReplaceTempView("table1")

#REPORTE 6-----------------------------------------------------------------------------------
df7 = spark.sql("SELECT YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP))) as Anio, COUNT(`Prioridad Orden`) as Cantidad  FROM table1 "
                "WHERE `Prioridad Orden` == 'M'"
                "Group By YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP)))"
                "Order By Cantidad DESC"
                " LIMIT 1")
df7.show()

data = [go.Pie(labels=df7.toPandas()['Anio'],values=df7.toPandas()['Cantidad'])]
plot(data, filename="/home/jovyan/work/graficas/Reporte6.html")

#REPORTE 61-----------------------------------------------------------------------------------
df8 = spark.sql("SELECT YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP))) as Anio, COUNT(`Prioridad Orden`) as Cantidad  FROM table1 "
                "WHERE `Prioridad Orden` == 'M'"
                "Group By YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP)))"
                "Order By Cantidad DESC")
df8.show()

data1 = [go.Pie(labels=df8.toPandas()['Anio'],values=df8.toPandas()['Cantidad'])]
plot(data1, filename="/home/jovyan/work/graficas/Reporte6.html")