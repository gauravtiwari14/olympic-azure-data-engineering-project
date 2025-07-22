# 🏅 Olympic Data Engineering Project on Azure

## 📌 Objective
Build an end-to-end data engineering pipeline using Azure to ingest, store, transform, and analyze the Tokyo Olympic dataset.

## 🔧 Tech Stack
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks (PySpark)
- Azure Synapse Analytics (SQL)
- GitHub (data source)

## 🚀 Architecture

<img width="1536" height="1024" alt="Architecture of Project" src="https://github.com/user-attachments/assets/10ad0af7-a4bf-4d05-a6a2-c152ff79526d" />

**Flow:**
1. **Kaggle** ➝ Dataset hosted on GitHub
2. **Azure Data Factory** ➝ Fetch data from GitHub → Store in **ADLS (raw)**
3. **Databricks** ➝ Clean and transform data → Store in **ADLS (transformed)**
4. **Synapse Analytics** ➝ Run SQL queries and analyze
