from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

# Creating SparkSession
spark = SparkSession.builder.master("local[2]").appName("bookings_data_processing").getOrCreate()

date = "2024-07-25"

print("Spark Version:", spark.version)

# file paths
booking_data = f"../data/booking_data/bookings_{date}.csv"
customer_data = f"../data/booking_data/bookings_{date}.csv"

# Read Data Date Wise
bookings_raw_df = spark.read.format("csv") \
    .option("header", True) \
    .option("quote", "\"") \
    .option("multiline", True) \
    .option("inferSchema", True) \
    .load(booking_data)

customer_raw_df = spark.read.format("csv") \
    .option("header", True) \
    .option("quote", "\"") \
    .option("multiline", True) \
    .option("inferSchema", True) \
    .load(customer_data)

# Schema Details of both data frames
bookings_raw_df.printSchema()
customer_raw_df.printSchema()

# Sample Data View
bookings_raw_df.show(5)
customer_raw_df.show(5)

# Applying Data Quality checks
booking_data_check = Check(spark, CheckLevel.Error, "quality check for bookings data") \
    .hasSize(lambda x: x >= 1) \
    .isUnique("booking_id", hint="Booking ID is not unique throught") \
    .isComplete("customer_id") \
    .isComplete("amount") \
    .isNonNegative("amount") \
    .isNonNegative("quantity") \
    .isNonNegative("discount")

customer_data_check = Check(spark, CheckLevel.Error, "quality check for customers data") \
    .hasSize(lambda x: x > 0) \
    .isUnique("customer_id") \
    .isComplete("customer_name") \
    .isComplete("customer_address") \
    .isComplete("phone_number") \
    .isComplete("email")

# Run the verification suite
booking_dq_check = VerificationSuite(spark) \
    .onData(bookings_raw_df) \
    .addCheck(booking_data_check) \
    .run()

customer_dq_check = VerificationSuite(spark) \
    .onData(customer_raw_df) \
    .addCheck(customer_data_check) \
    .run()

# Get the results data frame
booking_dq_check.checkResultsAsDataFrame().show()
customer_dq_check.checkResultsAsDataFrame().show()

# Check if verification passed
if booking_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Booking Data")

if customer_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Customer Data")

# Add ingestion timestamp to booking data and join with customers df
joined_df = bookings_raw_df \
    .withColumn("ingestion_time", current_timestamp()) \
    .join(customer_raw_df, "customer_id")

# Apply business filtering condition
df_transformed = joined_df.withColumn("total_cost", col("amount") - col("discount")) \
    .filter(col("quantity") > 0)

# Group by and aggregate df_transformed
df_transformed_agg = df_transformed \
    .groupBy("booking_type", "customer_id") \
    .agg(sum("total_cost").alias("total_amount_sum"),
         sum("quantity").alias("total_quantity_sum")
         )

df_transformed_agg.show(5)
# Check if the Delta table exists
fact_table_path = "hive_metastore.default.booking_fact"
fact_table_exists = spark._jsparkSession.catalog().tableExists(fact_table_path)

if fact_table_exists:

    # Read the existing fact table
    df_existing_fact = spark.read.format("delta").table(fact_table_path)
    # Combine existing tables records with file data
    df_combined = df_existing_fact.unionByName(df_transformed_agg, allowMissingColumns=True)

    # Again perform aggregations
    df_final_agg = df_combined \
        .groupBy("booking_type", "customer_id") \
        .agg(sum("total_amount_sum").alias("total_amount_sum"),
             sum("total_quantity_sum").alias("total_quantity_sum")
             )

else:
    # If the fact table doesn't exist, use the aggregated transformed data directly
    df_final_agg = df_transformed_agg

# Write the final aggregated data back to the Delta table
df_final_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(fact_table_path)

scd_table_path = "hive_metastore.default.customer_scd"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

if scd_table_exists:
    # Load the existing SCD table
    scd_table = DeltaTable.forName(spark, scd_table_path)
    display(scd_table.toDF())
    # Apply Merge Condition
    scd_table.alias("scd") \
        .merge(source=customer_raw_df.alias("updates"),
               condition="scd.customer_id = updates.customer_id and scd.valid_to = '9999-12-31'"
               )\
        .whenMatchedUpdate(set={"valid_to": "updates.valid_from"}) \
        .execute()
    customer_raw_df.write.format("delta").mode("append").saveAsTable(scd_table_path)
else:
    # If the SCD table doesn't exist, write the customer data as a new Delta table
    customer_raw_df.write.format("delta").mode("overwrite").saveAsTable(scd_table_path)