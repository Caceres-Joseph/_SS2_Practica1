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

#REPORTE 7-----------------------------------------------------------------------------------
df8 = spark.sql("SELECT YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP))) as Anio, ROUND(sum(`Ganancia Total`),3) as Ganancia,ROUND(sum(`Ingreso Total`),3) as Ingreso, ROUND(sum(`Costo Total`),3) as Costo  FROM table1 "
                "WHERE YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP))) == '2010' "
                "Group By YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Fecha Orden`, 'MM/dd/yyy') AS TIMESTAMP)))")
df8.show()

#data = [go.Pie(labels=df8.toPandas()['Ganancia,Ingreso,Costo'],values=df8.toPandas()['Ganancia,Ingreso,Costo'])]
#plot(data, filename="/home/jovyan/work/grafica7")




coor_x = ["Valores"]
coor_y_1 = df8.select("Ganancia").rdd.flatMap(lambda x: x).collect()
coor_y_2 = df8.select("Ingreso").rdd.flatMap(lambda x: x).collect()
coor_y_3 = df8.select("Costo").rdd.flatMap(lambda x: x).collect()

ingresos = go.Bar(
    x=coor_x,
    y=coor_y_1,
    text='Ingresos',
    textposition='auto',
    marker=dict(
        color='rgb(244, 157, 65)',
        line=dict(
            color='rgb(8,48,107)',
            width=1.5
        ),
    ),
    opacity=0.6
)

costo = go.Bar(
    x=coor_x,
    y=coor_y_2,
    text='Costo',
    textposition='auto',
    marker=dict(
        color='rgb(65, 70, 244)',
        line=dict(
            color='rgb(8,48,107)',
            width=1.5
        ),
    ),
    opacity=0.6
)

ganancia = go.Bar(
    x=coor_x,
    y=coor_y_3,
    text='Ganancia',
    textposition='auto',
    marker=dict(
        color='rgb(206, 244, 66)',
        line=dict(
            color='rgb(8,48,107)',
            width=1.5
        ),
    ),
    opacity=0.6
)

data = [ingresos, costo, ganancia]

layout = go.Layout(title="Ingresos, Ganancias & Costos del Ano 2010")

fig = go.Figure(data=data, layout=layout)
 
plot(fig, filename="/home/jovyan/work/graficas/Reporte7.html")