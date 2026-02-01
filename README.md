# 🏥 FHIR Healthcare Data Lakehouse

**HIPAA-compliant modern data platform for healthcare analytics. Ingests FHIR/HL7 medical data into Azure Data Lake with Bronze-Silver-Gold architecture.**

![Azure](https://img.shields.io/badge/Azure-Data%20Lake-blue.svg)
![dbt](https://img.shields.io/badge/dbt-Transformations-orange.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-cyan.svg)
![HIPAA](https://img.shields.io/badge/Compliance-HIPAA%20Ready-green.svg)
![Python](https://img.shields.io/badge/Python-3.9-blue.svg)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange.svg)

## 📋 Table of Contents
- [Architecture Overview](#architecture-overview)
- [Medallion Architecture](#medallion-architecture-bronzesilvergold)
- [HIPAA Compliance](#hipaa-compliance-features)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)

## 🏗️ Architecture Overview

```mermaid
graph TD
    A[Epic Clarity / HL7 / FHIR APIs] -->|Azure Data Factory| B[Azure Data Lake - Bronze]
    B -->|Spark Processing| C[Silver - Cleaned]
    C -->|dbt Models| D[Gold - Analytics]
    D -->|Snowflake| E[Power BI Dashboards]
    F[Data Governance] -->|Purview/Collibra| B
    F -->|Purview/Collibra| C
    F -->|Purview/Collibra| D
    
    style A fill:#ff9999
    style B fill:#ffcc99
    style C fill:#99ccff
    style D fill:#99ff99
    style E fill:#ffff99
    style F fill:#ff99ff
