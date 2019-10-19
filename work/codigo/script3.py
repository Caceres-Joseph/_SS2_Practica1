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

#REPORTE 3------------------------------------------------------------------------------------
df4 = spark.sql("SELECT Pais, ROUND(sum(`Ganancia Total`),3) as Ganancia FROM table1 "
                "WHERE (Pais == 'Guatemala' OR Pais == 'Costa Rica' OR Pais == 'El Salvador' OR Pais == 'Honduras' OR Pais == 'Nicaragua' OR Pais == 'Panama' ) AND Departamento == 'Clothes'"
          "Group By Pais")
df4.show()
#
data = [go.Pie(labels=df4.toPandas()['Pais'],values=df4.toPandas()['Ganancia'])]
plot(data, filename="/home/jovyan/work/graficas/Reporte3.html")