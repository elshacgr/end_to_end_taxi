from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.functions import col, from_unixtime
spark = SparkSession.builder \
    .appName("HiveTableAccess") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# df = spark.table("default.green_taxi_data")

# buat unix untuk lpep pickup dan lpep dropoff
df = spark.table("default.green_taxi_data")
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))
df1 = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))

# add_vendor_desc
conditions1 = [
    (col("VendorID") == 1, "Create Mobile Technology, LLC"),
    (col("VendorID") == 2, "VeriFone Inc")
]
# Create the new column "VendorDesc" based on conditions
df2 = df1.withColumn("VendorDesc", when(conditions1[0][0], conditions1[0][1])
                                            .when(conditions1[1][0], conditions1[1][1]))


# buat ratecode desc
# Define conditions and corresponding values for the new column
conditions2 = [
    (col("RatecodeID") == 1.0, "Standard Rate"),
    (col("RatecodeID") == 2.0, "JFK"),
    (col("RatecodeID") == 3.0, "Newark"),
    (col("RatecodeID") == 4.0, "Nassau or Westchester"),
    (col("RatecodeID") == 5.0, "Negotiated Fare"),
    (col("RatecodeID") == 6.0, "Group Ride")
]
df3 = df2.withColumn("RateCodeDesc", when(conditions2[0][0], conditions2[0][1])
                                            .when(conditions2[1][0], conditions2[1][1])
                                            .when(conditions2[2][0], conditions2[2][1])
                                            .when(conditions2[3][0], conditions2[3][1])
                                            .when(conditions2[4][0], conditions2[4][1])
                                            .when(conditions2[5][0], conditions2[5][1]))


# buat store_fws_flag desc
# Define conditions and corresponding values for the new column
conditions3 = [
    (col("store_and_fwd_flag") == "N", "Not a store and Forward Trip"),
    (col("store_and_fwd_flag") == "Y", "Store and Forward Trip")
]
df4 = df3.withColumn("store_and_fwd_flag_desc", when(conditions3[0][0], conditions3[0][1])
                                            .when(conditions3[1][0], conditions3[1][1]))

# payment type desc
conditions4 = [
    (col("payment_type") == 1.0, "Credit Card"),
    (col("payment_type") == 2.0, "Cash"),
    (col("payment_type") == 3.0, "No Charge"),
    (col("payment_type") == 4.0, "Dispute"),
    (col("payment_type") == 5.0, "Unknown"),
    (col("payment_type") == 6.0, "Voided Trip")
]
df5 = df4.withColumn("payment_type_desc", when(conditions4[0][0], conditions4[0][1])
                                            .when(conditions4[1][0], conditions4[1][1])
                                            .when(conditions4[2][0], conditions4[2][1])
                                            .when(conditions4[3][0], conditions4[3][1])
                                            .when(conditions4[4][0], conditions4[4][1])
                                            .when(conditions4[5][0], conditions4[5][1]))

# trip type desc
conditions5 = [
    (col("trip_type") == 1.0, "Street-hail"),
    (col("trip_type") == 2.0, "Dispatch"),
]
df6 = df5.withColumn("trip_type_desc", when(conditions5[0][0], conditions5[0][1])
                                            .when(conditions5[1][0], conditions5[1][1]))
# save dataframe to hive table               
df6.write.mode("overwrite").saveAsTable("taxi_with_desc")

spark.sql("SELECT * FROM taxi_with_desc limit 3").show()
