# 🚀 **CarDataTransform: Data Engineering Pipeline**

<p align="center">
 <img src="https://img.shields.io/badge/Azure-Data-blue?style=for-the-badge&logo=microsoft-azure"/>
 <img src="https://img.shields.io/badge/Databricks-ETL-orange?style=for-the-badge&logo=databricks"/>
 <img src="https://img.shields.io/badge/PowerBI-Analytics-yellow?style=for-the-badge&logo=power-bi"/>
 <img src="https://img.shields.io/badge/Delta_Lake-Storage-green?style=for-the-badge&logo=databricks"/>
</p>

## 📌 **Project Overview**
This project is a robust **Data Engineering Pipeline** designed using **Azure Data Factory, Databricks, Delta Lake, and Power BI**. CarDataTransform is a scalable data pipeline using Medallion Architecture (Bronze, Silver, Gold) with Azure Data Factory for ingestion and Databricks (PySpark) for transformation. It implements Star Schema with fact and dimension tables, uses Unity Catalog for governance, and stores data in Parquet & Delta Lake, with insights in Power BI.

## 🎯 **Key Features**
✅ **Data Ingestion:** Automated data ingestion from **CSV, SQL DB** using **Azure Data Factory**  
✅ **Data Processing:** ETL transformations using **Databricks, PySpark**  
✅ **Data Modeling:** Star Schema with **Fact & Dimension Tables**  
✅ **Storage & Format:** Stored as **Parquet & Delta Lake** in **Azure Data Lake Gen2**  
✅ **Incremental Data Processing** for real-time updates  
✅ **Data Security & Governance:** **Unity Catalog, Role-based Access Control (RBAC)**  
✅ **BI & Analytics:** Power BI dashboards for visualization  

---


## 🏗️ **Architecture Diagram**
### **Medallion Architecture (Bronze, Silver, Gold)**

![Medallion Architecture](architecture_diagrams/medallion_architecture.png)


**📌 Medallion Architecture (Bronze | Silver | Gold)**<br>
✔ **Bronze Layer**: Raw data ingestion via **Azure Data Factory**<br>
✔ **Silver Layer**: Cleansed & transformed data with **Databricks (PySpark)**<br>
✔ **Gold Layer**: Aggregated & analytics-ready data stored in **Delta Lake**<br>

### **Star Schema Model**

![Star Schema](https://github.com/Dilip-Anand/CarDataTransform/blob/main/DataBricks_WorkFlow.png)

**📌 Star Schema Implementation**<br>
✔ **Fact & Dimension tables** optimized for analytics<br>
✔ **Governance with Unity Catalog**<br>
✔ **BI & Reporting with Power BI**<br>


## 🚀 **Technologies & Tools Used**
| Category  | Tools/Technologies |
|-----------|--------------------|
| **Cloud** | Azure (Data Lake, Data Factory, Functions, Logic Apps) |
| **Processing** | Databricks, PySpark, SQL, Delta Lake |
| **Data Storage** | Azure Data Lake Gen2, Parquet, Delta |
| **ETL & Orchestration** | Azure Data Factory|
| **DevOps** | Azure DevOps (CI/CD), GitHub Actions |
| **Security** | Unity Catalog, RBAC, Azure Security |
| **Visualization** | Power BI |

---
