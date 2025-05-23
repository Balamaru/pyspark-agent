from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Simple SQL Transform").getOrCreate()

# PostgreSQL Configuration
jdbc_url = "jdbc:postgresql://localhost:5432/dev"
properties = {
    "user": "postgresql",
    "password": "postgresql",
    "driver": "org.postgresql.Driver"
}
table_name = "public.hr_analytics"
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Apply transformations
df_transformed = df

# Transform 1: sql
df_transformed.createOrReplaceTempView("hr_analytics")
df_transformed = spark.sql("""SELECT COUNT(EmpID) AS EmployeeCount FROM hr_analytics;""")

df_final = df_transformed

# Target S3 Configuration
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUAUYAYSBA")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "JBEYBYDWBSWHDVYWDBWDGWCWH")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://s3-penyedia.layanan.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

output_path = "s3a://dummycsv/analytics/test-agent.parquet"
print(f"\nMenyimpan DataFrame ke {output_path}...")
df_final.write.parquet(output_path, mode="overwrite")
print("DataFrame berhasil disimpan ke S3.")

spark.stop()
