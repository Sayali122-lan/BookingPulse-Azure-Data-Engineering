# Databricks notebook source

# ============================================================
# BookingPulse - Silver Layer Transformation
# Project: Hotel Inventory, Dynamic Pricing & Analytics
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Bronze source path
bronze_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/bronze/hotel_bookings_delta"

# Silver destination path
silver_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/silver/hotel_bookings_silver"

# Quarantine destination path
quarantine_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/quarantine"

print("BookingPulse Silver Transformation")
print("Bronze source:", bronze_path)
print("Silver destination:", silver_path)
print("Configuration completed successfully!")


# Read Bronze Delta table

bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

bronze_count = bronze_df.count()

print("Bronze records:", bronze_count)

display(bronze_df.limit(10))


# Inspect schema
print("Bronze Schema:")
bronze_df.printSchema()

# Check total records
print("Total records:", bronze_df.count())

# Check duplicate booking IDs
duplicate_count = (
    bronze_df.groupBy("booking_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate booking IDs:", duplicate_count)



# ============================================================
# STEP 4: NULL VALUE ANALYSIS
# ============================================================

from pyspark.sql import functions as F

null_counts = bronze_df.select([
    F.count(
        F.when(F.col(c).isNull(), c)
    ).alias(c)
    for c in bronze_df.columns
])

print("NULL VALUE ANALYSIS")
display(null_counts)


# ============================================================
# STEP 5: DATE QUALITY VALIDATION
# ============================================================

date_quality = bronze_df.select(
    F.count("*").alias("total_records"),

    F.sum(
        F.when(F.col("booking_date").isNull(), 1).otherwise(0)
    ).alias("missing_booking_date"),

    F.sum(
        F.when(F.col("check_in_date").isNull(), 1).otherwise(0)
    ).alias("missing_checkin_date"),

    F.sum(
        F.when(F.col("check_out_date").isNull(), 1).otherwise(0)
    ).alias("missing_checkout_date"),

    F.sum(
        F.when(
            F.col("check_in_date") < F.col("booking_date"), 1
        ).otherwise(0)
    ).alias("invalid_checkin"),

    F.sum(
        F.when(
            F.col("check_out_date") <= F.col("check_in_date"), 1
        ).otherwise(0)
    ).alias("invalid_checkout")
)

display(date_quality)


# STEP 6: DETAILED NULL ANALYSIS

null_summary = bronze_df.select(
    F.explode(
        F.array(*[
            F.struct(
                F.lit(c).alias("column_name"),
                F.count(
                    F.when(F.col(c).isNull(), 1)
                ).alias("null_count")
            )
            for c in bronze_df.columns
        ])
    ).alias("result")
)

display(
    null_summary.select(
        "result.column_name",
        "result.null_count"
    ).orderBy(F.desc("null_count"))
)


# STEP 7: INVESTIGATE INVALID CHECKOUT DATES

invalid_checkout_df = bronze_df.filter(
    F.col("check_out_date") <= F.col("check_in_date")
)

print("Invalid checkout records:", invalid_checkout_df.count())

display(
    invalid_checkout_df.select(
        "booking_id",
        "customer_id",
        "booking_date",
        "check_in_date",
        "check_out_date",
        "booking_status"
    ).limit(20)
)


# ============================================================
# STEP 9: DATA QUALITY RULES AND RECORD CLASSIFICATION
# ============================================================

from pyspark.sql import functions as F

# Rule 1: Missing customer ID
missing_customer = F.col("customer_id").isNull()

# Rule 2: Invalid checkout date
invalid_checkout = (
    F.col("check_out_date") <= F.col("check_in_date")
)

# Add quarantine reason
classified_df = bronze_df.withColumn(
    "quarantine_reason",
    F.concat_ws(
        ", ",
        F.when(
            missing_customer,
            F.lit("MISSING_CUSTOMER_ID")
        ),
        F.when(
            invalid_checkout,
            F.lit("INVALID_CHECKOUT_DATE")
        )
    )
)

# Separate valid records
silver_df = (
    classified_df
    .filter(F.col("quarantine_reason") == "")
    .drop("quarantine_reason")
)

# Separate rejected records
quarantine_df = classified_df.filter(
    F.col("quarantine_reason") != ""
)

# Record counts
total_count = bronze_df.count()
silver_count = silver_df.count()
quarantine_count = quarantine_df.count()

print("========== DATA QUALITY RESULTS ==========")
print("Bronze records:", total_count)
print("Silver records:", silver_count)
print("Quarantine records:", quarantine_count)
print("Reconciliation:", silver_count + quarantine_count)

# Display quarantine reasons
display(
    quarantine_df.groupBy("quarantine_reason")
    .count()
    .orderBy(F.desc("count"))
)


# ============================================================
# STEP 11: WRITE SILVER AND QUARANTINE DELTA DATA
# ============================================================

silver_path = (
    "/Volumes/workspace/default/bookingpulse_bronze_vol/"
    "silver/hotel_bookings_silver"
)

quarantine_path = (
    "/Volumes/workspace/default/bookingpulse_bronze_vol/"
    "quarantine/hotel_bookings_quarantine"
)

# Save Silver data
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

# Save Quarantine data
quarantine_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(quarantine_path)

print("Silver and Quarantine Delta data saved successfully!")
print("Silver location:", silver_path)
print("Quarantine location:", quarantine_path)


# ============================================================
# STEP 12: VERIFY SILVER AND QUARANTINE
# ============================================================

silver_verify = spark.read.format("delta").load(silver_path)

quarantine_verify = spark.read.format("delta").load(
    quarantine_path
)

print("Silver records:", silver_verify.count())
print("Quarantine records:", quarantine_verify.count())

print("Silver sample:")
display(silver_verify.limit(10))

print("Quarantine sample:")
display(
    quarantine_verify.select(
        "booking_id",
        "customer_id",
        "check_in_date",
        "check_out_date",
        "quarantine_reason"
    ).limit(10)
)


# ============================================================
# STEP 13: SILVER BUSINESS TRANSFORMATIONS
# ============================================================

from pyspark.sql import functions as F

# Read verified Silver Delta data
silver_source = spark.read.format("delta").load(silver_path)

# Standardize important text columns
silver_enriched_df = (
    silver_source

    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )

    .withColumn(
        "country",
        F.initcap(F.trim(F.col("country")))
    )

    .withColumn(
        "room_type",
        F.initcap(F.trim(F.col("room_type")))
    )

    .withColumn(
        "booking_status_std",
        F.upper(F.trim(F.col("booking_status")))
    )

    # Calculate hotel stay duration
    .withColumn(
        "stay_nights",
        F.datediff(
            F.col("check_out_date"),
            F.col("check_in_date")
        )
    )

    # Calculate total guests
    .withColumn(
        "guest_count",
        F.coalesce(F.col("adults"), F.lit(0)) +
        F.coalesce(F.col("children"), F.lit(0))
    )

    # Calculate booking lead time
    .withColumn(
        "booking_lead_days",
        F.datediff(
            F.col("check_in_date"),
            F.col("booking_date")
        )
    )

    # Booking date dimensions
    .withColumn(
        "booking_year",
        F.year("booking_date")
    )

    .withColumn(
        "booking_month",
        F.month("booking_date")
    )

    # Check-in date dimensions
    .withColumn(
        "checkin_year",
        F.year("check_in_date")
    )

    .withColumn(
        "checkin_month",
        F.month("check_in_date")
    )
)

# Display transformed data
print("Silver business transformation completed!")

print(
    "Total transformed records:",
    silver_enriched_df.count()
)

display(
    silver_enriched_df.select(
        "booking_id",
        "city",
        "country",
        "booking_status_std",
        "stay_nights",
        "guest_count",
        "booking_lead_days",
        "booking_year",
        "booking_month"
    ).limit(10)
)


# ============================================================
# STEP 14: SAVE ENRICHED SILVER DELTA DATA
# ============================================================

silver_enriched_path = (
    "/Volumes/workspace/default/bookingpulse_bronze_vol/"
    "silver/hotel_bookings_enriched"
)

silver_enriched_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_enriched_path)

print("Enriched Silver dataset saved successfully!")
print("Location:", silver_enriched_path)
print("Records:", silver_enriched_df.count())


# ============================================================
# STEP 15: VERIFY SAVED SILVER DATA
# ============================================================

silver_final_df = (
    spark.read.format("delta")
    .load(silver_enriched_path)
)

print("Silver final record count:", silver_final_df.count())

print("Silver final schema:")
silver_final_df.printSchema()

print("Silver final sample:")
display(silver_final_df.limit(10))

# Verify duplicate booking IDs
duplicate_count = (
    silver_final_df.groupBy("booking_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate booking IDs:", duplicate_count)
