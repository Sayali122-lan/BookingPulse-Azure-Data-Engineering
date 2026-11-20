# Databricks notebook source

# BookingPulse - Bronze Layer Ingestion

file_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/bookingpulse_hotel_bookings_100k_INR.csv"

# Read CSV using PySpark
hotel_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(file_path)
)

# Display first 10 records
display(hotel_df.limit(5))

total_records = hotel_df.count()

print("Total hotel booking records:", total_records)
print("Expected records: 100000")
print("Validation:", "PASSED" if total_records == 100000 else "CHECK REQUIRED")

hotel_df.printSchema()


# ==========================================
# BRONZE LAYER: CSV TO DELTA
# ==========================================

from pyspark.sql.functions import current_timestamp, col

# 1. Define Bronze Delta destination
bronze_delta_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"

# 2. Add ingestion metadata
bronze_df = (
    hotel_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# 3. Write data in Delta format
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_delta_path)

print("Bronze Delta ingestion completed successfully!")

# 4. Read saved Delta data
bronze_delta_df = spark.read \
    .format("delta") \
    .load(bronze_delta_path)

# 5. Verify record count
print("Total Bronze records:", bronze_delta_df.count())

# 6. Display sample records
display(bronze_delta_df.limit(10))


bronze_check = spark.read.format("delta").load(
    "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"
)

print("Total Bronze records:", bronze_check.count())


# ==========================================
# BOOKINGPULSE - SILVER LAYER
# STEP 1: Read Bronze Delta Data
# ==========================================

from pyspark.sql.functions import col

bronze_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"

# Read Bronze Delta table
bronze_df = spark.read.format("delta").load(bronze_path)

# Verify total records
print("Bronze record count:", bronze_df.count())

# Display sample records
display(bronze_df.limit(10))

# Display schema
bronze_df.printSchema()


from pyspark.sql.functions import col, count, when

# Read Bronze data
bronze_df = spark.read.format("delta").load(
    "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"
)

# Total records
print("Total Bronze records:", bronze_df.count())

# Check missing values in each column
null_check = bronze_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in bronze_df.columns
])

print("Null value analysis:")
display(null_check)

# Check duplicate booking IDs
duplicate_count = (
    bronze_df.count() -
    bronze_df.dropDuplicates(["booking_id"]).count()
)

print("Duplicate booking IDs:", duplicate_count)


from pyspark.sql.functions import col, count, when

# Complete null analysis
null_report = bronze_df.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in bronze_df.columns
])

display(null_report)


from pyspark.sql import functions as F

# Bronze Delta location
bronze_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"

# Read Bronze data
bronze_df = spark.read.format("delta").load(bronze_path)

print("Bronze data loaded successfully")
print("Total Bronze records:", bronze_df.count())

display(bronze_df.limit(10))


# Identify invalid records
invalid_condition = (
    F.col("customer_id").isNull() |
    (F.trim(F.col("customer_id")) == "")
)

# Send invalid records to quarantine
quarantine_df = bronze_df.filter(invalid_condition)

# Keep valid records for Silver
silver_df = bronze_df.filter(~invalid_condition)

# Handle missing discount values
silver_df = silver_df.fillna({
    "discount_pct": 0.0
})

# Handle missing payment methods
silver_df = silver_df.withColumn(
    "payment_method",
    F.when(
        F.col("payment_method").isNull() |
        (F.trim(F.col("payment_method")) == ""),
        "UNKNOWN"
    ).otherwise(F.col("payment_method"))
)

print("Data separation completed!")
print("Silver records:", silver_df.count())
print("Quarantine records:", quarantine_df.count())


# Define output locations
silver_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/silver/hotel_bookings_silver"

quarantine_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/quarantine/hotel_bookings_quarantine"

# Save cleaned Silver data
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

# Save invalid records separately
quarantine_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(quarantine_path)

print("Silver and Quarantine Delta data saved successfully!")


# Read Silver data
silver_verify_df = spark.read.format("delta").load(silver_path)

# Read Quarantine data
quarantine_verify_df = spark.read.format("delta").load(quarantine_path)

print("Silver records:", silver_verify_df.count())
print("Quarantine records:", quarantine_verify_df.count())

print("Silver sample records:")
display(silver_verify_df.limit(10))

print("Quarantine sample records:")
display(quarantine_verify_df.limit(10))
