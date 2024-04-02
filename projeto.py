#Bibliotecas
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import row_number
from pyspark.sql.functions import lead
from pyspark.sql.functions import min, max
from pyspark.sql.functions import unix_timestamp

#Configurando o ambiente Spark
sc = SparkContext(appName = "Projeto 2")
spark = SparkSession.builder.getOrCreate()

#Carregando os dados
src = 'dados/dataset.txt'
df = spark.read.csv(src, header = True)
print(df.show(5))

#Tabela Temporária
df.createOrReplaceTempView("tb_logistica")

#Consultas SQL
spark.sql("SHOW COLUMNS FROM tb_logistica").show()
spark.sql("SELECT * FROM tb_logistica LIMIT 5").show()
spark.sql("DESCRIBE tb_logistica").show()

#Utilizando Dot Notation (funções Spark, sem a necessidade de criar tabelas temporárias)
df.select(col('id_veiculo').alias('veiculo'), 'entrega').limit(5).show()

#Select
df.select('id_veiculo', 'entrega').show(5)
df.select(col('id_veiculo'), col('entrega')).show(5)
df.select(df.columns[:2]).show(5)

#Collect
df.collect()
df.collect()[0][2]

#Filter & Where
df.filter("entrega == 'Entrega 2'").show()

lista_id_veiculos = [298, 300, 400]
df.filter(df.id_veiculo.isin(lista_id_veiculos)).show()

df.where("id_veiculo > 400").show()

#Order by & Sort
df.sort('horario', 'entrega').show(10)
df.orderBy(df.horario.desc(), df.entrega.desc()).show(5)

#Agregações com funções do Spark
df.groupBy('id_veiculo').agg({'horario':'min'}).show(10)
df.groupBy('id_veiculo').agg({'horario':'min'}).withColumnRenamed('min(horario)', 'hora_primeira_entrega').show(10)

#Funções Window (Agregação por linhas)
query = """
SELECT id_veiculo, entrega, horario, ROW_NUMBER() OVER (PARTITION BY entrega ORDER BY horario) AS ranking FROM tb_logistica
"""
spark.sql(query).show(21)

query2 = """
SELECT id_veiculo, entrega, horario, LAG(horario, 1) OVER (PARTITION BY id_veiculo ORDER BY horario) AS entrega_anterior FROM tb_logistica
"""

spark.sql(query2).show(21)

query3 = """
SELECT id_veiculo, entrega, horario, LEAD(horario, 1) OVER (PARTITION BY id_veiculo ORDER BY horario) AS proxima_entrega FROM tb_logistica
"""

spark.sql(query3).show(21)

#Calculando o tempo para a próxima entrega de cada veículo
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
window = Window.partitionBy('id_veiculo').orderBy('horario')

dot_df = df.withColumn('tempo_proxima_entrega', (unix_timestamp(lead('horario', 1).over(window),'H:m') - unix_timestamp('horario', 'H:m'))/60).show(21)