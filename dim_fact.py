from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("dim_fact") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("default.taxi_with_desc")

# drop ehail_fee because it contains only null value
df = df.drop("ehail_fee")

# create Surrogate key for Store_fwd_flag
df = df.withColumn("SK_Store_Fwd_Flag", when(col("store_and_fwd_flag_desc") == "Not a store and Forward Trip", 1) \
                                .when(col("store_and_fwd_flag_desc") == "Store and Forward Trip", 2) \
                                       .otherwise(3))
# drop null value in trip_type and trip_type_desc columns
df = df.na.drop(subset=["trip_type","trip_type_desc"])

# create dimension table vendor
dim_vendor = df.select("VendorID","VendorDesc").distinct().orderBy("VendorID")
# create dimension table trip
dim_trip = df.select("trip_type","trip_type_desc").distinct().orderBy("trip_type")
# create dimension table payment
dim_payment = df.select("payment_type","payment_type_desc").distinct().orderBy("payment_type")
# create dimension store_fwd_flag
dim_store_fwd_flag = df.select("SK_Store_Fwd_Flag","store_and_fwd_flag","store_and_fwd_flag_desc").distinct().orderBy("SK_Store_Fwd_Flag")
# create dimension ratecode
dim_ratecode = df.select("RatecodeID","RateCodeDesc").distinct().orderBy("RatecodeID")


############ Creating Fact Table
fact_table = df.selectExpr("VendorID","SK_Store_Fwd_Flag","RatecodeID","lpep_pickup_datetime","lpep_dropoff_datetime","PULocationID","DOLocationID","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","payment_type","trip_type","congestion_surcharge").distinct()

# Specify the new database name
database_name = "database_taxi"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

dim_vendor.write.mode("overwrite").saveAsTable("dim_vendor")
dim_ratecode.write.mode("overwrite").saveAsTable("dim_ratecode")
dim_trip.write.mode("overwrite").saveAsTable("dim_trip")
dim_payment.write.mode("overwrite").saveAsTable("dim_payment")
dim_store_fwd_flag.write.mode("overwrite").saveAsTable("dim_store_fwd_flag")
fact_table.write.mode("overwrite").saveAsTable("fact_table")

