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