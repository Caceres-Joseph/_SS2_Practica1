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

#REPORTE 1-----------------------------------------------------------------------------------
df2 = spark.sql("SELECT Departamento, ROUND(sum(`Ingreso Total`),3) as Ingreso FROM table1 "
          "WHERE Departamento =='Cereal' OR Departamento == 'Fruits'"
          "OR Departamento == 'Meat' OR Departamento == 'Snacks'"
          "OR Departamento == 'Vegetables' "
          "Group By Departamento")
df2.show()

data = [go.Bar(x=df2.toPandas()['Departamento'],y=df2.toPandas()['Ingreso'])]
plot(data, filename="/home/jovyan/work/graficas/Reporte1.html")