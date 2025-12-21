# AWS Event-Driven Time-Series ETL Pipeline

## 📌 Project Overview
This project demonstrates a production-style, event-driven ETL pipeline built on AWS to ingest, process, and analyze time-series commodity price data.

The system polls gold prices at fixed intervals, stores immutable raw data, validates and cleans records, and generates analytical summaries such as open, close, high, low, average price, and trend.

This project focuses on **data engineering fundamentals**, not just code execution.

---

## 🏗️ Architecture Overview

EventBridge Scheduler  
→ Fetcher Lambda  
→ Amazon S3 (Raw Layer)  
→ Processor Lambda  
→ Amazon S3 (Cleaned Layer)  
→ Reporter Lambda  
→ Amazon S3 (Analytics / Reports)

All components are fully serverless.

---

## 🔁 Data Flow

1. **EventBridge Scheduler**
   - Triggers the Fetcher Lambda every minute for a fixed window.

2. **Fetcher Lambda**
   - Calls an external finance API (TwelveData).
   - Stores each observation as an immutable JSON object in S3.
   - Uses time-based partitioning (date/hour/minute).

3. **Raw Data Layer (S3)**
   - Immutable storage of provider-shaped data.
   - Enables reprocessing without re-fetching APIs.

4. **Processor Lambda**
   - Validates schema and normalizes data types.
   - Produces cleaned, analysis-ready JSON files.

5. **Cleaned Data Layer (S3)**
   - Guaranteed schema.
   - Decoupled from data source.

6. **Reporter Lambda**
   - Aggregates all cleaned records for a day.
   - Computes:
     - Open / Close price
     - High / Low
     - Average price
     - Percent change
     - Trend (UP / DOWN / FLAT)

7. **Reports Layer (S3)**
   - Stores business-ready analytical summaries.

---

## 🧠 Key Design Principles

- Stateless Lambda functions
- Immutable raw data
- Time-partitioned S3 keys
- Separation of ingestion, processing, and analytics
- Idempotent batch processing
- Least-privilege IAM roles

---

## 🧰 AWS Services Used

- AWS Lambda
- Amazon EventBridge Scheduler
- Amazon S3
- AWS IAM
- Amazon CloudWatch Logs

---

## 📊 Sample Analytical Output

```json
{
  "asset": "gold",
  "date": "2025-12-21",
  "observations": 61,
  "open_price": 12494.8234,
  "close_price": 12494.8234,
  "high_price": 12494.8234,
  "low_price": 12494.8234,
  "average_price": 12494.8234,
  "percent_change": 0.0,
  "trend": "FLAT"
}
