Sales Data Lakehouse Architecture 
Designed and implemented a modern data lakehouse pipeline using Azure services and Databricks to process and analyze sales data efficiently.

🔧 Key Features
End-to-End Ingestion Pipeline:
Ingested sales data from on-prem SQL sources using Azure Data Factory (ADF). Implemented an incremental load strategy using watermarking and lookup techniques to avoid full loads and improve efficiency.

Bronze Layer (Raw Data):
Data was ingested into Azure Data Lake Gen2 in Parquet format and organized by source systems. This layer retained raw, incremental data for traceability and reprocessing.

Silver Layer (Cleansed Data):
Applied transformation logic in Databricks notebooks to perform data cleaning, handle null values and duplicates, and apply basic business rules and aggregations to prepare clean, usable data for modeling.

Gold Layer (Analytics-Ready):
Modeled the cleaned data into a star schema using Delta Lake for business-level analysis and reporting. Created dimension and fact tables optimized for BI consumption.

Medallion Architecture:
Adopted the Bronze → Silver → Gold architecture pattern:

🔹 Bronze: Raw incremental sales data (Parquet)

🔸 Silver: Cleaned and business-transformed sales data

⭐ Gold: Star-schema Delta tables for analytics

Reporting & Visualization:
Connected Power BI to Gold layer Delta tables for real-time sales analytics dashboards.

Security & Governance:

Used Azure Key Vault to manage secrets and credentials securely.

Enforced RBAC at storage and Databricks levels for role-based access control.

Applied Unity Catalog for fine-grained data governance, schema management, and secure sharing across workspaces.
