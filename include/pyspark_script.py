from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.Builder.appName('Testing PySpark').getOrCreate()
sample_data = [{'name': 'John D.', 'age': 30}, {'name': 'Alice G.', 'age': 25},
                {'name': 'Bob T.', 'age': 35}, {'name': 'Eve A', 'age': 28}]
df = spark.createDataFrame(sample_data)
df_transformed = df.withColumn('name', regexp_replace(col('name'), '\\s+', ' '))
df_transformed.show()
print(df_transformed.show())
