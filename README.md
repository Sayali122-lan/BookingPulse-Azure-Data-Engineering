# BookingPulse Azure – Hotel Data Engineering Platform

An end-to-end hotel booking data engineering project demonstrating batch ingestion, data quality engineering, PySpark transformations, Delta Lake, dimensional modelling, Azure Data Factory and analytics-ready datasets.

## 1. Project Overview

BookingPulse is a portfolio-grade data engineering platform designed to process hotel booking information and transform raw booking records into reliable, analytics-ready datasets.

The project demonstrates a Medallion Architecture consisting of Bronze, Silver and Gold layers, with data ingestion through Azure Data Factory and data transformation using PySpark in Databricks Free Edition.

**Dataset:** 100,000 hotel booking records  
**Currency:** INR  
**Cloud region:** Central India

## 2. Architecture

The implementation consists of two workflows.

### Azure Data Factory – Batch Ingestion

```text
Hotel Booking CSV
       |
       v
ADLS Gen2 - Landing
       |
       v
Azure Data Factory
       |
       v
Copy Data Activity
       |
       v
File Metadata Validation
       |
       v
ADLS Gen2 - bronze_adf
```

### Databricks – Data Transformation

```text
Hotel Booking Dataset
       |
       v
Databricks Free Edition
       |
       v
BRONZE LAYER
Raw Delta Data
       |
       v
DATA QUALITY VALIDATION
       |
       +----------------------+
       |                      |
       v                      v
SILVER LAYER             QUARANTINE
Cleaned and              Rejected Records
Enriched Data
       |
       v
GOLD LAYER
       |
       +---------------------------+
       |             |             |
       v             v             v
 Dimensions       Fact Tables   Aggregations
       |             |             |
       +-------------+-------------+
                     |
                     v
               CSV EXPORTS
                     |
                     v
           Manual upload to ADLS Gen2
                     |
                     v
              Future Power BI
```

**Architecture note:** Azure Data Factory and Databricks Free Edition are currently implemented as separate workflows. The Databricks transformations use Unity Catalog Managed Volumes. Exported Silver, Quarantine and Gold files are manually uploaded to ADLS Gen2. Direct ADF-to-Databricks orchestration is not yet implemented.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Azure Data Factory | Batch ingestion and pipeline execution |
| Azure Data Lake Storage Gen2 | Cloud-based data storage |
| Azure Databricks Free Edition | Distributed data processing |
| PySpark | Data ingestion and transformations |
| Delta Lake | Reliable transactional data storage |
| Databricks Unity Catalog Volumes | Managed storage for notebook data |
| Python | Data engineering logic |
| SQL | Data analysis and transformation queries |
| GitHub | Source control and project documentation |
| Power BI | Future analytics and reporting |

## 4. Azure Resources

| Resource | Name |
|---|---|
| Resource Group | `rg-bookingpulse-dev` |
| Storage Account | `stbookingpulsedev26` |
| Data Factory | `adf-bookingpulse-dev-26` |
| Azure Region | Central India |
| ADLS Gen2 Container | `bookingpulse` |

### ADLS Gen2 Folder Structure

```text
bookingpulse/
|
|-- landing/
|-- bronze/
|-- bronze_adf/
|-- silver/
|-- quarantine/
|-- gold/
|-- audit/
|-- checkpoints/
```

## 5. Project Implementation

### 5.1 Bronze Layer – Data Ingestion

- Ingested 100,000 hotel booking records using PySpark.
- Validated source record counts.
- Stored raw data in Delta format.
- Verified Bronze Delta storage.

**Execution results:**

| Metric | Result |
|---|---:|
| Total source records | 100,000 |
| Bronze records | 100,000 |
| Validation | PASSED |

### 5.2 Azure Data Factory – Batch Ingestion

Created the pipeline `PL_BookingPulse_Bronze_Ingestion`.

**Pipeline components:**

- Source dataset: `DS_BookingPulse_Landing_CSV`
- Destination dataset: `DS_BookingPulse_Bronze_CSV`
- Linked service: `LS_ADLS_BookingPulse`
- Copy Data activity: `Copy_BookingPulse_CSV`
- Get Metadata activity: `Get_Bronze_File_Metadata`
- File validation: `Validate_Bronze_File_Exists`
- Failure handling using Fail and variable activities.

**Execution evidence:**

- Pipeline validation successful.
- Multiple manual pipeline runs completed successfully.
- Four successful pipeline runs were visible in the Azure Monitor pipeline runs view.
- One CSV file was copied successfully to the ADLS Gen2 `bronze_adf` directory.

### 5.3 Silver Layer – Data Quality and Transformation

Implemented data quality checks and business transformations using PySpark.

**Data quality checks:**

- Null value analysis.
- Duplicate booking ID validation.
- Missing customer ID identification.
- Invalid checkout date identification.
- Record reconciliation.
- Rejected record classification.

**Quarantine handling:**

Invalid records are separated from valid records and stored independently for controlled data quality management.

| Metric | Records |
|---|---:|
| Bronze records | 100,000 |
| Silver records | 99,760 |
| Quarantined records | 240 |
| Missing customer ID | 200 |
| Invalid checkout date | 40 |
| Duplicate booking IDs in final Silver data | 0 |

**Reconciliation:**

`99,760 + 240 = 100,000`

**Business transformations:**

Created additional business columns:

- `stay_nights`
- `guest_count`
- `booking_lead_days`
- `booking_year`
- `booking_month`

Saved the enriched Silver dataset and rejected records in Delta format.

### 5.4 Gold Layer – Dimensional Modelling

Implemented the Gold layer using Delta Lake.

**Dimension tables:**

- `dim_hotel`
- `dim_customer`
- `dim_date`
- `dim_date_original`

**Fact table:**

- `fact_bookings`

**Business aggregation tables:**

- `agg_booking_status`
- `agg_hotel_performance`
- `agg_monthly_bookings`
- `agg_revenue_by_country`

The two date dimensions preserve the original booking-date range and the extended date range used for booking, check-in and check-out analysis.

### 5.5 Performance Optimization

Implemented and demonstrated Spark and Delta Lake optimization techniques:

- Broadcast joins.
- Partitioning strategy analysis.
- Predicate pushdown and column pruning.
- Logical and physical execution plan comparison.
- Delta Lake OPTIMIZE.
- Z-Ordering.
- Delta transaction history inspection.
- VACUUM DRY RUN and file-retention validation.

These techniques demonstrate practical approaches to improving query execution and managing Delta data.

### 5.6 Data Export

Exported the Silver, Quarantine and Gold datasets from Databricks as CSV files.

Exported files were manually uploaded to their corresponding ADLS Gen2 folders.

This provides cloud-storage copies of the transformed datasets, but does not yet represent automated Databricks-to-ADLS data movement.

## 6. Execution Evidence

The following screenshots demonstrate actual notebook execution, Azure pipeline runs and data validation.

### Bronze Layer

- [Bronze ingestion success](screenshots/bronze_ingestion_success.png)
- [Data quality validation](screenshots/data_quality_validation.png)

### Silver Layer

- [Silver quarantine results](screenshots/silver_quarantine_results.png)
- [Silver business transformations](screenshots/silver_business_transformations.png)
- [Silver Delta verification](screenshots/silver_delta_verification.png)

### Gold Layer

- [Gold dimension tables](screenshots/gold_dimension_tables.png)
- [Gold fact table validation](screenshots/gold_fact_table_validation.png)
- [Gold business aggregations](screenshots/gold_business_aggregations.png)
- [Gold aggregation reconciliation](screenshots/gold_aggregation_reconciliation.png)
- [Gold date dimension comparison](screenshots/gold_date_dimension_comparison.png)

### Performance Optimization

- [Broadcast join optimization](screenshots/gold_broadcast_join.png)
- [Join execution comparison](screenshots/gold_join_execution_comparison.png)
- [Partitioning analysis](screenshots/gold_partitioning_analysis.png)
- [Predicate and column pruning](screenshots/gold_predicate_column_pruning.png)
- [Delta OPTIMIZE](screenshots/gold_delta_optimize.png)
- [Delta Z-Ordering](screenshots/gold_delta_zordering.png)
- [Delta table history](screenshots/gold_delta_table_history.png)
- [VACUUM dry run](screenshots/gold_vacuum_dry_run.png)

Azure Data Factory pipeline execution screenshots and additional evidence are maintained in the project screenshots folder.

## 7. Repository Structure

```text
BookingPulse-Azure-Data-Engineering/
|
|-- notebooks/
|   |-- BookingPulse_Bronze_Ingestion.py
|   |-- BookingPulse_Silver_Transformation.py
|   |-- BookingPulse_Gold_Analytics.py
|
|-- screenshots/
|   |-- README.md
|   |-- bronze_ingestion_success.png
|   |-- data_quality_validation.png
|   |-- silver_quarantine_results.png
|   |-- silver_business_transformations.png
|   |-- silver_delta_verification.png
|   |-- gold_dimension_tables.png
|   |-- gold_fact_table_validation.png
|   |-- gold_business_aggregations.png
|   |-- gold_delta_optimize.png
|   |-- gold_delta_zordering.png
|   |-- gold_delta_table_history.png
|   |-- gold_vacuum_dry_run.png
|
|-- README.md
|-- LICENSE
|-- .gitignore
```

## 8. Client Requirement Coverage – 12 Evaluation Areas

| # | Client requirement | BookingPulse implementation | Status |
|---|---|---|---|
| 1 | Data ingestion and extraction (Batch and Streaming) | CSV batch ingestion through ADF and PySpark. Streaming remains a future enhancement. | Partial |
| 2 | ETL/ELT pipeline development | ADF Copy Data pipeline and PySpark Bronze, Silver and Gold transformations. | Implemented |
| 3 | Data cleaning, transformation and validation | Null analysis, invalid date checks, quarantine, reconciliation and enrichment. | Implemented |
| 4 | SQL and advanced database concepts | Delta tables, joins, data modelling and transaction history. Additional dedicated SQL demonstrations can be added. | Partial |
| 5 | Python-based data processing | Python and PySpark data processing. | Implemented |
| 6 | Data warehousing and data modelling | Fact, dimension and business aggregation tables. | Implemented |
| 7 | Cloud-based data engineering architecture | Azure Data Factory and ADLS Gen2, with Databricks Free Edition used for transformations. | Implemented |
| 8 | Workflow orchestration and scheduling | ADF pipeline execution and validation; scheduled and integrated Databricks execution remain future enhancements. | Partial |
| 9 | Data quality monitoring and error handling | Databricks quarantine, reconciliation and ADF failure-handling activities. Centralized monitoring remains an enhancement. | Partial |
| 10 | Pipeline performance optimization and scalability | Broadcast joins, partitioning, pruning, Delta OPTIMIZE and Z-Ordering. | Implemented |
| 11 | CI/CD, automation and deployment | GitHub source control is available; automated CI/CD deployment remains pending. | Pending |
| 12 | Documentation and end-to-end architecture design | Project README, architecture description and execution evidence. Final architecture documentation is being completed. | Partial |

## 9. Business Use Cases

The platform is designed to support:

- Hotel booking performance analysis.
- Revenue analysis.
- Monthly booking trends.
- Hotel and country-level comparisons.
- Booking cancellation analysis.
- Customer booking behaviour.
- Hotel performance reporting.

## 10. Future Enhancements

The following enhancements are planned to strengthen coverage of the client evaluation requirements:

1. Implement a streaming ingestion demonstration.
2. Add dedicated advanced SQL examples, including window functions and analytical queries.
3. Establish a supported Azure Databricks and ADLS Gen2 integration.
4. Implement end-to-end orchestration and scheduling.
5. Add centralized monitoring and operational logging.
6. Implement GitHub Actions for code validation and CI/CD.
7. Develop Power BI dashboards using the Gold datasets.

## 11. Project Status

| Component | Status |
|---|---|
| Azure Resource Group and Storage | Completed |
| ADLS Gen2 folder structure | Completed |
| ADF Linked Service | Completed |
| ADF Bronze ingestion pipeline | Completed |
| Databricks Bronze ingestion | Completed |
| Silver transformations | Completed |
| Quarantine handling | Completed |
| Gold dimensional modelling | Completed |
| Gold business aggregations | Completed |
| Delta Lake optimization | Completed |
| Silver, Quarantine and Gold exports | Completed |
| Automated ADF-to-Databricks integration | Pending |
| Streaming ingestion | Pending |
| Dedicated advanced SQL demonstrations | Pending |
| CI/CD deployment | Pending |
| Power BI dashboard | Planned |

## 12. Author

Developed as an independent portfolio project to demonstrate practical data engineering skills in Azure, Azure Data Factory, PySpark, Delta Lake, data quality engineering and dimensional modelling.


---

*This project documents the implemented workflows and their current integration boundaries. Future enhancements are explicitly identified rather than presented as completed functionality.*
