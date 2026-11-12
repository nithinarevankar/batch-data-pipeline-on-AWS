# AWS Data Engineering – Serverless Data Lake Pipeline

<p align="center">
  <b>An end-to-end AWS data engineering pipeline that transforms raw data into analytics-ready datasets using Amazon S3, AWS Glue, Apache Iceberg, and Amazon Athena.</b>
</p>

<p align="center">
  <a href="#-architecture">🏗️ Architecture</a> |
  <a href="#-features">✨ Features</a> |
  <a href="#-data-pipeline">⚙️ Data Pipeline</a> |
  <a href="#-getting-started">🚀 Getting Started</a> |
  <a href="#-roadmap">🗺️ Roadmap</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Status-Completed-brightgreen?style=flat-square" />
  <img src="https://img.shields.io/badge/Cloud-AWS-orange?style=flat-square&logo=amazonaws" />
  <img src="https://img.shields.io/badge/Architecture-Medallion-blue?style=flat-square" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Amazon%20S3-Storage-569A31?style=for-the-badge&logo=amazons3&logoColor=white" />
  <img src="https://img.shields.io/badge/AWS%20Glue-ETL-8C4FFF?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/Amazon%20Athena-SQL-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Iceberg-Table%20Format-00A1E0?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Python-ETL-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Step%20Functions-Orchestration-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
</p>
## Architecture

**Pipeline Flow:**

![AWS Data Pipeline Architecture](docs/diagram.png)

**Orchestration and Monitoring:**

![AWS Data Pipeline Architecture](docs/step_function_flow.png)


## AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Stores raw and processed datasets |
| AWS Glue | Serverless ETL processing |
| AWS Glue Data Catalog | Centralized metadata management |
| Apache Iceberg | ACID transactions and table management |
| Amazon Athena | SQL-based data querying |
| AWS Step Functions | ETL workflow orchestration |
| Amazon EventBridge | Scheduled pipeline execution |
| Amazon SNS | Failure notifications and email alerts |
| AWS IAM | Roles and permissions management |
| Amazon CloudWatch | Execution logs and monitoring |

## Data Architecture

The project follows the Medallion Architecture.

### 1. Bronze Layer
- Ingests source data from Amazon S3.
- Preserves the raw data structure.
- Maintains ingestion metadata.
- Supports incremental ingestion.

### 2. Silver Layer
- Performs data cleaning and standardization.
- Handles duplicate records.
- Applies data quality validations.
- Handles schema changes and data inconsistencies.
- Uses Apache Iceberg for table management and data updates.

### 3. Gold Layer
- Performs business-level transformations.
- Creates analytics-ready datasets.
- Supports downstream reporting and analytical queries.

## Key Engineering Features

- **Incremental Data Processing:** Uses watermark-based processing to identify new and updated records.
- **Medallion Architecture:** Separates raw, cleaned, and business-ready datasets.
- **Data Quality:** Implements validation checks to maintain data reliability.
- **Schema Management:** Supports controlled schema evolution and validation.
- **Apache Iceberg:** Provides ACID transactions, MERGE operations, and table-level management.
- **Workflow Orchestration:** Uses AWS Step Functions to coordinate ETL execution.
- **Automated Scheduling:** Uses Amazon EventBridge for scheduled pipeline runs.
- **Failure Notifications:** Integrates Amazon SNS to send email alerts when pipeline execution fails.

## Repository Structure

```text
aws-data-engineering-pipeline/
│
├── glue_jobs/
│   ├── bronze/
|   |----- ingestion.py
│   ├── silver/
|   |----- transformation.py
│   └── gold/
|   |----- load.py
│
├── step_functions/
│   └── etl_workflow .json
│
├── config/
│   └── job.config.json
│
├── sql/
│   └── athena_queries.sql
│
├── docs/
│   └── architecture.png
│
├── README.md
└── .gitignore
```

## Workflow Execution

1. Amazon EventBridge triggers the pipeline on a scheduled basis.
2. AWS Step Functions starts the ETL workflow.
3. AWS Glue executes the required data processing jobs.
4. Data moves through the Bronze, Silver, and Gold layers.
5. AWS Glue Data Catalog maintains table metadata.
6. Amazon Athena enables SQL-based analytical queries.
7. Step Functions handles execution failures and triggers SNS notifications.

## Technologies

- Python
- Apache Spark / PySpark
- AWS Glue
- Amazon S3
- Apache Iceberg
- AWS Step Functions
- Amazon EventBridge
- Amazon SNS
- Amazon Athena
- AWS Glue Data Catalog
- SQL
- Git & GitHub

## Project Objectives

The objective of this project is to demonstrate practical cloud data engineering concepts by building an automated, serverless ETL pipeline using AWS-native services.

It focuses on data ingestion, incremental processing, data quality, lakehouse architecture, orchestration, and reliable pipeline execution.

---

**Author:** Nithin A R

**Domain:** Data Engineering | AWS | Big Data | Lakehouse Architecture