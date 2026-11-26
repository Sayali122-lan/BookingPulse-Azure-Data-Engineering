
# =====================================================
# BOOKINGPULSE - GOLD ANALYTICS LAYER
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("BookingPulse Gold Analytics")
print("Initializing Gold Layer...")

# Source: Enriched Silver Delta table
silver_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/silver/hotel_bookings_enriched"

# Gold destination
gold_path = "/Volumes/workspace/default/bookingpulse_bronze_vol/gold"

# Read Silver data
silver_df = spark.read.format("delta").load(silver_path)

# Record count
silver_count = silver_df.count()

print("Silver source loaded successfully")
print("Total Silver records:", silver_count)

# Display sample records
display(silver_df.limit(10))

# Display schema
silver_df.printSchema()


# =====================================================
# GOLD DIMENSION 1: HOTEL DIMENSION
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Select unique hotel information
hotel_base = (
    silver_df
    .groupBy("hotel_id")
    .agg(
        F.first("city", ignorenulls=True).alias("city"),
        F.first("country", ignorenulls=True).alias("country")
    )
)

# Generate deterministic surrogate key
hotel_window = Window.orderBy("hotel_id")

dim_hotel = (
    hotel_base
    .withColumn(
        "hotel_key",
        F.row_number().over(hotel_window)
    )
    .select(
        "hotel_key",
        "hotel_id",
        "city",
        "country"
    )
)

# Save as Delta table
dim_hotel_path = gold_path + "/dim_hotel"

dim_hotel.write.format("delta").mode("overwrite").save(dim_hotel_path)

print("Hotel dimension created successfully")
print("Total hotels:", dim_hotel.count())

display(dim_hotel.limit(10))


# =====================================================
# GOLD DIMENSION 2: CUSTOMER DIMENSION
# =====================================================

# Extract unique customers
customer_base = (
    silver_df
    .select("customer_id")
    .filter(F.col("customer_id").isNotNull())
    .distinct()
)

# Generate surrogate key
customer_window = Window.orderBy("customer_id")

dim_customer = (
    customer_base
    .withColumn(
        "customer_key",
        F.row_number().over(customer_window)
    )
    .select(
        "customer_key",
        "customer_id"
    )
)

# Save Delta table
dim_customer_path = gold_path + "/dim_customer"

dim_customer.write.format("delta").mode("overwrite").save(dim_customer_path)

print("Customer dimension created successfully")
print("Total customers:", dim_customer.count())

display(dim_customer.limit(10))


# =====================================================
# GOLD DIMENSION 3: DATE DIMENSION without extention
# =====================================================

# Recreate original date dimension from booking_date only

original_date_range = silver_df.agg(
    F.min("booking_date").alias("min_date"),
    F.max("booking_date").alias("max_date")
).first()

original_min_date = original_date_range["min_date"]
original_max_date = original_date_range["max_date"]

print("Original minimum date:", original_min_date)
print("Original maximum date:", original_max_date)

# Generate original calendar dates

original_date_df = spark.sql(f"""
    SELECT explode(
        sequence(
            DATE('{original_min_date}'),
            DATE('{original_max_date}'),
            INTERVAL 1 DAY
        )
    ) AS calendar_date
""")

# Create original date dimension

dim_date_original = (
    original_date_df
    .withColumn(
        "date_key",
        F.date_format("calendar_date", "yyyyMMdd").cast("int")
    )
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.quarter("calendar_date"))
    .withColumn("month", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("day", F.dayofmonth("calendar_date"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("calendar_date"))
)

# Save original dimension separately

original_date_path = gold_path + "/dim_date_original"

dim_date_original.write \
    .format("delta") \
    .mode("overwrite") \
    .save(original_date_path)

print("Original Date Dimension saved successfully!")
print("Location:", original_date_path)
print("Total original records:", dim_date_original.count())

display(dim_date_original.limit(10))


# ============================================================
# BOOKINGPULSE - EXTENDED GOLD DATE DIMENSION
# Purpose: Support booking, check-in and check-out dates
# ============================================================

# Collect date ranges from all three date columns

all_dates = (
    silver_df.select(F.col("booking_date").alias("date"))
    .unionByName(
        silver_df.select(F.col("check_in_date").alias("date"))
    )
    .unionByName(
        silver_df.select(F.col("check_out_date").alias("date"))
    )
    .filter(F.col("date").isNotNull())
)

date_range = all_dates.agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
).first()

min_date = date_range["min_date"]
max_date = date_range["max_date"]

print("Minimum date across all date types:", min_date)
print("Maximum date across all date types:", max_date)

# Generate calendar dates

date_df = spark.sql(f"""
    SELECT explode(
        sequence(
            DATE('{min_date}'),
            DATE('{max_date}'),
            INTERVAL 1 DAY
        )
    ) AS calendar_date
""")

# Create extended date dimension

dim_date_extended = (
    date_df
    .withColumn("date_key", F.date_format(
        "calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.quarter("calendar_date"))
    .withColumn("month", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("day", F.dayofmonth("calendar_date"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("calendar_date"))
    .select(
        "date_key", "calendar_date", "year", "quarter",
        "month", "month_name", "day", "day_name", "week_of_year"
    )
)

# Validate record count

print("Extended Date Dimension Records:", dim_date_extended.count())

display(dim_date_extended.limit(15))

# Verify date range
dim_date_extended.select(
    F.min("calendar_date").alias("min_date"),
    F.max("calendar_date").alias("max_date")
).show()


# Save extended Date Dimension to Gold Delta

dim_date_path = gold_path + "/dim_date"

dim_date_extended.write \
    .format("delta") \
    .mode("overwrite") \
    .save(dim_date_path)

print("Extended Date Dimension saved successfully!")
print("Location:", dim_date_path)
print("Total records:", dim_date_extended.count())

# Verify saved Gold Date Dimension

saved_dim_date = spark.read.format("delta").load(dim_date_path)

print("Saved Date Dimension Records:", saved_dim_date.count())

display(saved_dim_date.orderBy("calendar_date").tail(5))

# Verify both Date Dimension versions

original_dim = spark.read.format("delta").load(
    gold_path + "/dim_date_original"
)

extended_dim = spark.read.format("delta").load(
    gold_path + "/dim_date"
)

print("Original Date Dimension:", original_dim.count())
print("Extended Date Dimension:", extended_dim.count())

date_version_summary = spark.createDataFrame([
    ("dim_date_original", original_dim.count()),
    ("dim_date", extended_dim.count())
], ["dimension_name", "total_records"])

display(date_version_summary)

# =====================================================
# GOLD DIMENSION SUMMARY
# =====================================================

dimension_summary = spark.createDataFrame([
    ("dim_hotel", dim_hotel.count()),
    ("dim_customer", dim_customer.count()),
    ("dim_date_original", dim_date_original.count()),
    ("dim_date", dim_date_extended.count())
], ["dimension_table", "total_records"])

display(dimension_summary)

# =====================================================
# VALIDATION OF DATES FOR FACT TABLE
# =====================================================

date_columns = [
    "booking_date",
    "check_in_date",
    "check_out_date"
]

for column_name in date_columns:
    result = silver_df.select(
        F.min(column_name).alias("min_date"),
        F.max(column_name).alias("max_date")
    )

    print(f"Date range for {column_name}:")
    display(result)

# =====================================================
# BOOKINGPULSE - GOLD FACT TABLE
# =====================================================

from pyspark.sql import functions as F

print("Initializing Gold Fact Table...")

# Read Gold dimension tables
dim_hotel_df = spark.read.format("delta").load(
    gold_path + "/dim_hotel"
)

dim_customer_df = spark.read.format("delta").load(
    gold_path + "/dim_customer"
)

dim_date_df = spark.read.format("delta").load(
    gold_path + "/dim_date"
)

# Alias dimensions
hotel = dim_hotel_df.alias("h")
customer = dim_customer_df.alias("c")

booking_date = dim_date_df.alias("bd")
checkin_date = dim_date_df.alias("ci")
checkout_date = dim_date_df.alias("co")

# Join Silver data with Gold dimensions
fact_bookings = (
    silver_df.alias("s")

    # Hotel surrogate key
    .join(
        hotel,
        F.col("s.hotel_id") == F.col("h.hotel_id"),
        "left"
    )

    # Customer surrogate key
    .join(
        customer,
        F.col("s.customer_id") == F.col("c.customer_id"),
        "left"
    )

    # Booking date key
    .join(
        booking_date,
        F.col("s.booking_date") == F.col("bd.calendar_date"),
        "left"
    )

    # Check-in date key
    .join(
        checkin_date,
        F.col("s.check_in_date") == F.col("ci.calendar_date"),
        "left"
    )

    # Check-out date key
    .join(
        checkout_date,
        F.col("s.check_out_date") == F.col("co.calendar_date"),
        "left"
    )

    # Select fact table columns
    .select(
        F.col("s.booking_id"),
        F.col("h.hotel_key"),
        F.col("c.customer_key"),

        F.col("bd.date_key").alias("booking_date_key"),
        F.col("ci.date_key").alias("check_in_date_key"),
        F.col("co.date_key").alias("check_out_date_key"),

        F.col("s.booking_status_std"),
        F.col("s.stay_nights"),
        F.col("s.guest_count"),
        F.col("s.booking_lead_days"),

        F.col("s.nightly_rate_inr"),
        F.col("s.total_amount_inr"),
        F.col("s.tax_amount_inr"),

        F.col("s.discount_pct"),
        F.col("s.currency"),
        F.col("s.payment_method"),
        F.col("s.booking_channel")
    )
)

# Save Gold Fact Delta table
fact_bookings_path = gold_path + "/fact_bookings"

fact_bookings.write \
    .format("delta") \
    .mode("overwrite") \
    .save(fact_bookings_path)

print("Gold Fact Table created successfully!")
print("Fact records:", fact_bookings.count())
print("Location:", fact_bookings_path)

# Display sample records
display(fact_bookings.limit(10))

# Display schema
fact_bookings.printSchema()



# =====================================================
# BOOKINGPULSE - GOLD FACT TABLE VALIDATION
# =====================================================

from pyspark.sql import functions as F

print("GOLD FACT TABLE VALIDATION")

# 1. Total records
fact_count = fact_bookings.count()

# 2. Duplicate booking IDs
duplicate_count = (
    fact_bookings
    .groupBy("booking_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# 3. Null surrogate key validation
key_columns = [
    "hotel_key",
    "customer_key",
    "booking_date_key",
    "check_in_date_key",
    "check_out_date_key"
]

null_checks = fact_bookings.select([
    F.count(
        F.when(F.col(c).isNull(), 1)
    ).alias(c)
    for c in key_columns
])

print("Total Fact Records:", fact_count)
print("Duplicate Booking IDs:", duplicate_count)

print("Surrogate Key Null Analysis:")
display(null_checks)

# 4. Compare Silver and Fact counts
print("Silver Records:", silver_df.count())
print("Fact Records:", fact_count)

# 5. Final validation summary
validation_summary = spark.createDataFrame([
    ("Silver vs Fact Count",
     "PASS" if silver_df.count() == fact_count else "FAIL"),

    ("Duplicate Booking IDs",
     "PASS" if duplicate_count == 0 else "FAIL"),

    ("Surrogate Key Nulls",
     "PASS" if null_checks.first() is not None
     and all(null_checks.first()[c] == 0 for c in key_columns)
     else "FAIL")
], ["validation_check", "status"])

display(validation_summary)


# =====================================================
# BOOKINGPULSE - GOLD BUSINESS AGGREGATIONS
# =====================================================

from pyspark.sql import functions as F

print("Initializing Gold Business Aggregations...")

# Read Gold Fact and Dimension tables
fact_df = spark.read.format("delta").load(
    gold_path + "/fact_bookings"
)

hotel_df = spark.read.format("delta").load(
    gold_path + "/dim_hotel"
)

date_df = spark.read.format("delta").load(
    gold_path + "/dim_date"
)

# =====================================================
# 1. REVENUE BY COUNTRY
# =====================================================

revenue_by_country = (
    fact_df
    .join(hotel_df, "hotel_key", "inner")
    .groupBy("country")
    .agg(
        F.countDistinct("booking_id").alias("total_bookings"),
        F.sum("total_amount_inr").alias("total_revenue_inr"),
        F.avg("total_amount_inr").alias("average_booking_value")
    )
    .orderBy(F.desc("total_revenue_inr"))
)

revenue_by_country.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path + "/agg_revenue_by_country")

print("Revenue by country created successfully")
display(revenue_by_country)


# =====================================================
# 2. MONTHLY BOOKING TRENDS
# =====================================================

monthly_bookings = (
    fact_df
    .join(
        date_df,
        fact_df.booking_date_key == date_df.date_key,
        "inner"
    )
    .groupBy(
        "year",
        "month",
        "month_name"
    )
    .agg(
        F.countDistinct("booking_id").alias("total_bookings"),
        F.sum("total_amount_inr").alias("total_revenue_inr")
    )
    .orderBy("year", "month")
)

monthly_bookings.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path + "/agg_monthly_bookings")

print("Monthly booking trends created successfully")
display(monthly_bookings)


# =====================================================
# 3. BOOKING STATUS ANALYSIS
# =====================================================

booking_status = (
    fact_df
    .groupBy("booking_status_std")
    .agg(
        F.countDistinct("booking_id").alias("total_bookings"),
        F.sum("total_amount_inr").alias("total_revenue_inr")
    )
    .orderBy(F.desc("total_bookings"))
)

booking_status.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path + "/agg_booking_status")

print("Booking status analysis created successfully")
display(booking_status)


# =====================================================
# 4. HOTEL PERFORMANCE
# =====================================================

hotel_performance = (
    fact_df
    .groupBy("hotel_key")
    .agg(
        F.countDistinct("booking_id").alias("total_bookings"),
        F.sum("total_amount_inr").alias("total_revenue_inr"),
        F.avg("total_amount_inr").alias("average_booking_value"),
        F.avg("stay_nights").alias("average_stay_nights"),
        F.sum("guest_count").alias("total_guests")
    )
    .join(hotel_df, "hotel_key", "left")
    .select(
        "hotel_key",
        "hotel_id",
        "city",
        "country",
        "total_bookings",
        "total_revenue_inr",
        "average_booking_value",
        "average_stay_nights",
        "total_guests"
    )
)

hotel_performance.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path + "/agg_hotel_performance")

print("Hotel performance created successfully")
display(hotel_performance.limit(20))


# =====================================================
# FINAL SUMMARY
# =====================================================

print("====================================")
print("GOLD BUSINESS AGGREGATIONS COMPLETED")
print("====================================")

print("Revenue by Country:", revenue_by_country.count())
print("Monthly Booking Trends:", monthly_bookings.count())
print("Booking Status:", booking_status.count())
print("Hotel Performance:", hotel_performance.count())


# =====================================================
# GOLD BUSINESS AGGREGATION VALIDATION
# =====================================================

aggregation_tables = {
    "agg_revenue_by_country": revenue_by_country,
    "agg_monthly_bookings": monthly_bookings,
    "agg_booking_status": booking_status,
    "agg_hotel_performance": hotel_performance
}

summary = []

for table_name, df in aggregation_tables.items():

    count = df.count()

    print(f"{table_name}: {count} records")

    summary.append((table_name, count))

aggregation_summary = spark.createDataFrame(
    summary,
    ["aggregation_table", "total_records"]
)

display(aggregation_summary)


# =====================================================
# BOOKINGPULSE - GOLD BUSINESS AGGREGATION RECONCILIATION
# =====================================================

from pyspark.sql import functions as F

print("Initializing Gold Reconciliation Validation...")

# -----------------------------------------------------
# 1. SOURCE FACT TABLE METRICS
# -----------------------------------------------------

fact_total_bookings = fact_df.select(
    F.countDistinct("booking_id").alias("total")
).first()["total"]

fact_total_revenue = fact_df.agg(
    F.sum("total_amount_inr").alias("revenue")
).first()["revenue"] or 0

print("Fact Table Bookings:", fact_total_bookings)
print("Fact Table Revenue:", fact_total_revenue)


# -----------------------------------------------------
# 2. AGGREGATION METRICS
# -----------------------------------------------------

country_bookings = revenue_by_country.agg(
    F.sum("total_bookings")
).first()[0] or 0

country_revenue = revenue_by_country.agg(
    F.sum("total_revenue_inr")
).first()[0] or 0

monthly_total_bookings = monthly_bookings.agg(
    F.sum("total_bookings")
).first()[0] or 0

monthly_total_revenue = monthly_bookings.agg(
    F.sum("total_revenue_inr")
).first()[0] or 0

status_total_bookings = booking_status.agg(
    F.sum("total_bookings")
).first()[0] or 0

status_total_revenue = booking_status.agg(
    F.sum("total_revenue_inr")
).first()[0] or 0

hotel_total_bookings = hotel_performance.agg(
    F.sum("total_bookings")
).first()[0] or 0

hotel_total_revenue = hotel_performance.agg(
    F.sum("total_revenue_inr")
).first()[0] or 0


# -----------------------------------------------------
# 3. DIMENSION KEY VALIDATION
# -----------------------------------------------------

duplicate_hotel_keys = (
    hotel_df.count() -
    hotel_df.select("hotel_key").distinct().count()
)

duplicate_date_keys = (
    date_df.count() -
    date_df.select("date_key").distinct().count()
)

# Check unmatched fact records

unmatched_hotels = (
    fact_df.alias("f")
    .join(
        hotel_df.alias("h"),
        F.col("f.hotel_key") == F.col("h.hotel_key"),
        "left"
    )
    .filter(F.col("h.hotel_key").isNull())
    .count()
)

unmatched_dates = (
    fact_df.alias("f")
    .join(
        date_df.alias("d"),
        F.col("f.booking_date_key") == F.col("d.date_key"),
        "left"
    )
    .filter(F.col("d.date_key").isNull())
    .count()
)


# -----------------------------------------------------
# 4. RECONCILIATION CHECKS
# -----------------------------------------------------

validation_results = []

def add_check(check_name, expected, actual, tolerance=0.01):

    expected_value = float(expected or 0)
    actual_value = float(actual or 0)

    status = (
        "PASS"
        if abs(expected_value - actual_value) <= tolerance
        else "FAIL"
    )

    validation_results.append((
        check_name,
        str(expected),
        str(actual),
        status
    ))


# Booking count reconciliation

add_check(
    "Revenue by Country - Booking Count",
    fact_total_bookings,
    country_bookings
)

add_check(
    "Monthly Bookings - Booking Count",
    fact_total_bookings,
    monthly_total_bookings
)

add_check(
    "Booking Status - Booking Count",
    fact_total_bookings,
    status_total_bookings
)

add_check(
    "Hotel Performance - Booking Count",
    fact_total_bookings,
    hotel_total_bookings
)


# Revenue reconciliation

add_check(
    "Revenue by Country - Total Revenue",
    fact_total_revenue,
    country_revenue
)

add_check(
    "Monthly Bookings - Total Revenue",
    fact_total_revenue,
    monthly_total_revenue
)

add_check(
    "Booking Status - Total Revenue",
    fact_total_revenue,
    status_total_revenue
)

add_check(
    "Hotel Performance - Total Revenue",
    fact_total_revenue,
    hotel_total_revenue
)


# Dimension integrity

add_check(
    "Duplicate Hotel Keys",
    0,
    duplicate_hotel_keys
)

add_check(
    "Duplicate Date Keys",
    0,
    duplicate_date_keys
)

add_check(
    "Unmatched Hotel Keys",
    0,
    unmatched_hotels
)

add_check(
    "Unmatched Booking Date Keys",
    0,
    unmatched_dates
)


# -----------------------------------------------------
# 5. FINAL VALIDATION REPORT
# -----------------------------------------------------

validation_df = spark.createDataFrame(
    validation_results,
    [
        "validation_check",
        "expected_value",
        "actual_value",
        "status"
    ]
)

display(validation_df)

failed_checks = sum(
    1 for row in validation_results
    if row[3] == "FAIL"
)

print("==========================================")
print("GOLD RECONCILIATION VALIDATION SUMMARY")
print("==========================================")
print("Total Validation Checks:", len(validation_results))
print("Passed Checks:", len(validation_results) - failed_checks)
print("Failed Checks:", failed_checks)

if failed_checks == 0:
    print("FINAL RESULT: ALL GOLD RECONCILIATION CHECKS PASSED")
else:
    print("FINAL RESULT: VALIDATION FAILED - INVESTIGATION REQUIRED")



# =====================================================
# BOOKINGPULSE - GOLD PERFORMANCE OPTIMIZATION
# STEP 1: PARTITIONING STRATEGY ANALYSIS
# SERVERLESS COMPATIBLE
# =====================================================

from pyspark.sql import functions as F
print("====================================")
print("GOLD PERFORMANCE OPTIMIZATION")
print("====================================")
print("Analyzing Fact Table for Partitioning...")

# Read existing Gold Fact table
fact_path = gold_path + "/fact_bookings"

fact_df = spark.read.format("delta").load(fact_path)

# 1. Total records
total_records = fact_df.count()

print("Total Fact Records:", total_records)

# 2. Delta table storage details
delta_details = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")

print("Delta Table Storage Details:")

display(
    delta_details.select(
        "format",
        "numFiles",
        "sizeInBytes",
        "partitionColumns"
    )
)

# 3. Analyze booking records by month
monthly_distribution = (
    fact_df
    .withColumn(
        "booking_year_month",
        F.substring(
            F.col("booking_date_key").cast("string"),
            1,
            6
        )
    )
    .groupBy("booking_year_month")
    .agg(
        F.count("*").alias("total_bookings")
    )
    .orderBy("booking_year_month")
)

print("Monthly Booking Distribution:")

display(monthly_distribution)

# 4. Partitioning analysis summary
print("====================================")
print("PARTITIONING ANALYSIS COMPLETED")
print("====================================")

print("Total Fact Records:", total_records)
print("Fact table analyzed successfully.")
print("Existing Gold Delta table remains unchanged.")



# =====================================================
# BOOKINGPULSE - PREDICATE AND COLUMN PRUNING
# =====================================================

from pyspark.sql import functions as F
print("====================================")
print("PREDICATE AND COLUMN PRUNING ANALYSIS")
print("====================================")

fact_path = gold_path + "/fact_bookings"

# Read Gold Fact table
fact_df = spark.read.format("delta").load(fact_path)

# -----------------------------------------------------
# 1. WITHOUT PRUNING - ALL COLUMNS AND RECORDS
# -----------------------------------------------------

print("Original Fact Table")

print("Total Records:", fact_df.count())

print("Total Columns:", len(fact_df.columns))

# -----------------------------------------------------
# 2. PREDICATE PRUNING - FILTER BY BOOKING DATE
# January 2026
# -----------------------------------------------------

filtered_df = fact_df.filter(
    (F.col("booking_date_key") >= 20260101) &
    (F.col("booking_date_key") <= 20260131)
)

print("After Predicate Pruning")

print("Filtered Records:", filtered_df.count())

# -----------------------------------------------------
# 3. COLUMN PRUNING - SELECT REQUIRED COLUMNS
# -----------------------------------------------------

optimized_df = (
    filtered_df
    .select(
        "booking_id",
        "booking_date_key",
        "booking_status_std",
        "total_amount_inr"
    )
)

print("After Column Pruning")

print("Selected Columns:", len(optimized_df.columns))

display(optimized_df.limit(10))

# -----------------------------------------------------
# 4. FINAL SUMMARY
# -----------------------------------------------------

print("====================================")
print("PRUNING ANALYSIS COMPLETED")
print("====================================")

print("Original Records:", fact_df.count())
print("Filtered Records:", filtered_df.count())
print("Original Columns:", len(fact_df.columns))
print("Selected Columns:", len(optimized_df.columns))

print("Predicate and Column Pruning demonstrated.")


# =====================================================
# BOOKINGPULSE - BROADCAST JOIN OPTIMIZATION
# =====================================================

from pyspark.sql import functions as F
print("====================================")
print("BROADCAST JOIN OPTIMIZATION")
print("====================================")

# Read Gold tables
fact_df = spark.read.format("delta").load(
    gold_path + "/fact_bookings"
)

hotel_df = spark.read.format("delta").load(
    gold_path + "/dim_hotel"
)

# 1. Normal join
normal_join = fact_df.join(
    hotel_df,
    "hotel_key",
    "inner"
)

print("Normal Join:")
display(normal_join.limit(10))

# 2. Broadcast join
broadcast_join = fact_df.join(
    F.broadcast(hotel_df),
    "hotel_key",
    "inner"
)

print("Broadcast Join:")
display(broadcast_join.limit(10))

# 3. Compare execution plans
print("NORMAL JOIN EXECUTION PLAN")
normal_join.explain(mode="formatted")

print("BROADCAST JOIN EXECUTION PLAN")
broadcast_join.explain(mode="formatted")

# 4. Validate record counts
print("Normal Join Records:", normal_join.count())
print("Broadcast Join Records:", broadcast_join.count())

print("BROADCAST JOIN ANALYSIS COMPLETED")

# BROADCAST JOIN EXECUTION PLAN VALIDATION

print("BROADCAST JOIN PHYSICAL PLAN")

broadcast_join.explain(mode="formatted")

# =====================================================
# GOLD PERFORMANCE - LOGICAL PLAN ANALYSIS
# =====================================================
print("======================")

print("LOGICAL PLAN ANALYSIS")
print("======================")

print("\n1. NORMAL JOIN LOGICAL PLAN")

normal_join.explain(mode="extended")

print("\n2. BROADCAST JOIN LOGICAL PLAN")

broadcast_join.explain(mode="extended")

print("\nLOGICAL PLAN ANALYSIS COMPLETED")

# =====================================================
# GOLD PERFORMANCE - PHYSICAL PLAN ANALYSIS
# =====================================================

print("NORMAL JOIN - PHYSICAL PLAN")
normal_join.explain(mode="formatted")

print("\nBROADCAST JOIN - PHYSICAL PLAN")
broadcast_join.explain(mode="formatted")

print("\nPHYSICAL PLAN ANALYSIS COMPLETED")

# =====================================================
# NORMAL JOIN VS BROADCAST JOIN
# EXECUTION COMPARISON
# =====================================================

normal_count = normal_join.count()
broadcast_count = broadcast_join.count()

comparison = spark.createDataFrame([
    ("Normal Join", normal_count),
    ("Broadcast Join", broadcast_count)
], ["join_strategy", "total_records"])

print("JOIN EXECUTION COMPARISON")

display(comparison)

print("Normal Join Records:", normal_count)
print("Broadcast Join Records:", broadcast_count)

print("Execution plan comparison completed")

# =====================================================
# GOLD PERFORMANCE - DELTA TABLE INSPECTION
# =====================================================

fact_path = gold_path + "/fact_bookings"
print("======================")
print("DELTA TABLE INSPECTION")
print("======================")

delta_details = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")

display(delta_details.select(
    "format",
    "numFiles",
    "sizeInBytes",
    "partitionColumns"
))

print("Delta table inspection completed")

# =====================================================
# DELTA OPTIMIZE - FILE COMPACTION
# =====================================================

fact_path = gold_path + "/fact_bookings"
print("======================")
print("BEFORE OPTIMIZATION")

before_optimize = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")

display(before_optimize.select(
    "numFiles",
    "sizeInBytes"
))

# Optimize Delta table
spark.sql(f"""
    OPTIMIZE delta.`{fact_path}`
""")

print("DELTA OPTIMIZATION COMPLETED")
print("======================")
print("AFTER OPTIMIZATION")

after_optimize = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")

display(after_optimize.select(
    "numFiles",
    "sizeInBytes"
))

# =====================================================
# DELTA OPTIMIZATION - Z-ORDERING
# =====================================================
fact_path = gold_path + "/fact_bookings"
print("BEFORE Z-ORDERING")

before_zorder = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")

display(before_zorder.select(
    "numFiles",
    "sizeInBytes"
))

# Apply Z-Ordering
spark.sql(f"""
    OPTIMIZE delta.`{fact_path}`
    ZORDER BY (booking_date_key)
""")
print("Z-ORDERING COMPLETED")

# Verify Delta table
after_zorder = spark.sql(f"""
    DESCRIBE DETAIL delta.`{fact_path}`
""")
display(after_zorder.select(
    "numFiles",
    "sizeInBytes"
))
print("Z-Ordering execution completed successfully")

# =====================================================
# DELTA LAKE - TABLE HISTORY
# =====================================================

fact_path = gold_path + "/fact_bookings"

print("DELTA TABLE HISTORY")
print("===================")

history_df = spark.sql(f"""
    DESCRIBE HISTORY delta.`{fact_path}`
""")

display(
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationParameters"
    )
)

print("Total Delta Versions:", history_df.count())
print("Delta history inspection completed")


# =====================================================
# DELTA LAKE - VACUUM DRY RUN
# =====================================================

fact_path = gold_path + "/fact_bookings"
print("====================")
print("DELTA VACUUM DRY RUN")
print("====================")

vacuum_preview = spark.sql(f"""
    VACUUM delta.`{fact_path}` DRY RUN
""")

display(vacuum_preview)

print("VACUUM DRY RUN COMPLETED")
print("No files have been deleted.")

gold_base = "/Volumes/workspace/default/bookingpulse_bronze_vol/gold"

export_base = "/Volumes/workspace/default/bookingpulse_bronze_vol/exports/gold_export"

gold_tables = [
    "dim_customer",
    "dim_date",
    "dim_date_original",
    "dim_hotel",
    "fact_bookings",
    "agg_booking_status",
    "agg_hotel_performance",
    "agg_monthly_bookings",
    "agg_revenue_by_country"
]

for table in gold_tables:
    df = spark.read.format("delta").load(f"{gold_base}/{table}")

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{export_base}/{table}")

    print(f"{table} exported successfully")

print("ALL GOLD TABLES EXPORTED SUCCESSFULLY")
