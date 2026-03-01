# Data Warehouse Project

Building and deploying a MySQL data warehouse with ETL pipelines, data modeling, and analytics capabilities.

# 🚀 Project Requirements

### 🎯 Objective
Build a modern MySQL-based data warehouse that consolidates sales data to enable reporting and data-driven decision-making.

### 📌 Specifications

- **Data Sources:** Two systems (ERP and CRM) provided as CSV files.
- **Data Quality:** Clean and resolve inconsistencies before analysis.
- **Integration:** Combine both systems into a unified analytical data model.
- **Scope:** Focus on the latest available dataset (no historization required).
- **Documentation:** Provide clear data model documentation for business and analytics users.

----------------------------------------------

## 📌 Overview

This project demonstrates an end-to-end data warehousing and analytics solution — from building a modern data warehouse to generating actionable business insights.

Designed as a portfolio project, it highlights industry best practices in:
- Data Engineering
- ETL Development
- Dimensional Data Modeling
- SQL-Based Analytics

----------------------------------------------

# 🏗️ Data Architecture

This solution follows the **Medallion Architecture** pattern with three structured layers:

## 🥉 Bronze Layer — Raw Data

- Stores raw data directly from source systems.
- Data is ingested from CSV files into MySQL.
- No transformations are applied at this stage.

## 🥈 Silver Layer — Cleaned & Transformed Data

- Performs data cleansing and validation.
- Standardizes and normalizes datasets.
- Prepares structured data for analytics.

## 🥇 Gold Layer — Business-Ready Data

- Contains curated, analytics-ready datasets.
- Structured using a **Star Schema**.
- Optimized for reporting and analytical queries.

----------------------------------------------

# 📖 Project Components

## 1️⃣ Data Architecture
Designing and implementing a modern data warehouse using the Bronze–Silver–Gold layered approach.

## 2️⃣ ETL Pipelines
Building workflows to:
- Extract data from ERP and CRM systems
- Transform and cleanse data
- Load it into structured warehouse layers

## 3️⃣ Data Modeling
Creating:
- Fact tables
- Dimension tables  
Optimized for performance and analytical workloads.

## 4️⃣ Analytics & Reporting
Developing SQL-based reports and dashboards to deliver actionable insights.

----------------------------------------------

## 📊 Business Intelligence — Analytics & Reporting

### 🎯 Objective
Develop SQL-driven analytics to provide insights into:

- Customer Behavior
- Product Performance
- Sales Trends

These insights support stakeholders with key performance indicators and strategic metrics.

----------------------------------------------

# 🧱 Technology Stack

- MySQL
- CSV Data Sources
- Star Schema Modeling
- Medallion Architecture

----------------------------------------------

# 🛡️ License

This project is licensed under the MIT License.

You are free to use, modify, and distribute this project with proper attribution.

