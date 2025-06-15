 End-to-End Data Engineering Project – Learning Analytics Platform (Udemy Clone)
🚀 Project Overview
This project demonstrates an end-to-end data engineering solution built to analyze learning platform data (similar to Udemy). It uses a modern data stack including Azure, Databricks, Snowflake, and Streamlit. The project follows the Medallion Architecture (Bronze → Silver → Gold) and integrates AI-based analytics using Snowflake Cortex Analyst.

🧱 Architecture

🛠️ Tech Stack
Layer	Tools & Technologies
Ingestion	Azure Data Factory (ADF), Azure Blob Storage
Storage	Azure Data Lake Gen2 (Bronze Layer)
Processing	Azure Databricks (PySpark)
Data Warehouse	Snowflake (Silver & Gold Layers)
Analytics	Streamlit Dashboard, Snowflake Cortex Analyst
Monitoring	Azure Monitor Alerts

🔍 Key Features
ADF pipelines extract raw CSV files from Blob Storage and load them into Data Lake Gen2.

Bronze Layer holds raw data as-is.

Databricks (PySpark) performs:

Schema enforcement

Deduplication

Incremental processing

Slowly Changing Dimensions (SCD Type 2)

Silver Layer: Cleaned & conformed data.

Gold Layer: Fact & dimension tables stored in Snowflake.

Streamlit Dashboard provides:

Student-wise engagement & progress insights

Instructor performance analytics

Leaderboard for top students & instructors

Cortex Analyst AI on top of Snowflake adds semantic layer & natural language analytics.

Azure Monitor sends alerts for pipeline failures.

📊 Dashboards Preview
Coming soon: Screenshots or demo link of Streamlit dashboard.

📦 Requirements
Python 3.9+

Snowflake Account

Azure Subscription

Databricks Workspace

Streamlit

🚧 Future Enhancements
CI/CD integration using GitHub Actions

Row-level security in Streamlit dashboards

Real-time data ingestion

Chatbot integration with Cortex Analyst

🤝 Let's Connect
If you liked this project or have feedback, feel free to connect on LinkedIn or raise a GitHub issue!-  https://www.linkedin.com/in/venkatesh-mannepati-764841272/
