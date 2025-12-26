# Databricks Healthcare Lakehouse

End-to-end Databricks Lakehouse project implementing **Delta Lake medallion architecture (Bronze, Silver, Gold)** with data quality checks, fraud analytics, and executive dashboards.

---

## 📌 Project Overview

This project simulates a real-world **healthcare claims analytics platform** built using Databricks Community Edition.  
It demonstrates how raw claims data can be ingested, validated, transformed, and analyzed using modern lakehouse best practices.

---

## 🏗️ Architecture (Medallion Pattern)

- **Bronze Layer**
  - Raw claims ingestion
  - Delta Lake managed tables
  - Schema preservation and audit columns

- **Silver Layer**
  - Data cleansing and deduplication
  - Data quality validation (null checks, invalid amounts)
  - Reconciliation metrics

- **Gold Layer**
  - Business-ready analytics tables
  - Monthly claim trends
  - Rule-based fraud detection
  - KPI-ready datasets

---

## 📊 Executive Analytics Dashboard

![Healthcare Claims Dashboard](dashboards/healthcare_claims_dashboard.png)

This executive dashboard is built on **Gold-layer Delta tables** using **Databricks SQL** and provides a business-level view of healthcare claims data.

### Key insights provided
- **Monthly Claim Amount Trend** for cost monitoring
- **Fraud Flag Distribution** highlighting high-risk claims
- **Data Quality & Reconciliation Metrics** to ensure pipeline reliability
- **Executive KPIs**, including:
  - Total Claims
  - Total Claim Amount (USD)
  - Fraud Rate (%)

The dashboard enables fast, reliable analytics with full traceability back to the **Bronze and Silver layers** of the Databricks Lakehouse.

---

## 📈 Key KPIs

- Total Claims Processed
- Total Claim Amount
- Fraud Rate Percentage
- Data Quality Validation Metrics

---

## 🧰 Tech Stack

- Databricks Community Edition
- PySpark
- Delta Lake
- Spark SQL
- Databricks SQL Dashboards

---

## 📂 Repository Structure

