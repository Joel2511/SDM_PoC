# TrendSync SDM Join Project

## Overview
This repository contains the Semantic Data Management (SDM) component of the BDMA Joint Project for TrendSync, a simulated fashion retail start-up focused on inventory optimization and demand forecasting. The Proof of Concept (PoC) uses property graphs to model customer-article purchase relationships, leveraging graph analytics to identify trending articles and customer segments. It processes a sampled dataset of 100,000 records from a 28M-record transaction dataset using PySpark and GraphFrames in Azure Databricks.

## Repository Structure
- `SDM_POC.ipynb`: Main notebook with PoC code, explanations, and embedded results.
- `TrendSyncMountAzureStorage.ipynb`: Script to mount Azure Blob Storage in Databricks.
- `README.md`: This file.
- `SDM_POC.pdf`: PoC documentation.

## Setup Instructions
The PoC was developed in Azure Databricks and requires specific configurations to run. Results are embedded in `SDM_POC.ipynb` for review without execution.

### Prerequisites
- **Databricks Runtime**: Use **Databricks Runtime 14.3 LTS** (includes Apache Spark 3.2.0, Scala 2.12) to avoid version conflicts with GraphFrames and other libraries.
- **Azure Blob Storage**: Configure access using SAS tokens provided in the notebooks.
- **Dependencies**:
  - **PySpark**: Included in Databricks Runtime 14.3 LTS.
  - **GraphFrames**: Install via Maven with coordinates `graphframes:graphframes:0.8.2-spark3.2-s_2.12`.
  - **Python Libraries**: Uses `time` (Python standard library).

### Cluster Setup
1. Create or select a Databricks cluster with Runtime 14.3 LTS.
2. Install GraphFrames:
   - Go to the **Libraries** tab in the cluster configuration.
   - Select **Maven** as the library source.
   - Enter coordinates: `graphframes:graphframes:0.8.2-spark3.2-s_2.12`.
   - Click **Install** and restart the cluster if prompted.

### Mounting Storage
To access the dataset:
- Ensure access to an Azure Blob Storage account with the `enriched_transactions` Delta table in `/mnt/gold-zone-bdm`.
- Run `TrendSyncMountAzureStorage.ipynb` to mount the storage container.

### Running the Code
1. Attach `SDM_POC.ipynb` to the cluster.
2. Ensure dependencies and storage are configured.
3. Execute all cells in `SDM_POC.ipynb`. The notebook samples 100,000 records, builds a property graph, runs PageRank and Connected Components, and stores metadata. Note: Execution requires the original dataset; otherwise, review embedded results.

## Results
Embedded in `SDM_POC.ipynb` (Step 5: Verifying Results):
- Top 10 trending articles by PageRank score (e.g., article 706016001, score 72.90, "Jade HW Skinny Denim", "Black Dark").
- Top 10 customer segments by community ID (e.g., community 7.0).
These insights support TrendSync’s inventory optimization and demand forecasting goals.

## Additional Notes
- The PoC uses a 100,000-record sample for feasibility, with a runtime of 4.64 minutes.
- The code relies on Databricks-specific dependencies (GraphFrames, Spark), limiting external execution.

## Contact
For questions, contact Joel Anil Jose at joel.anil.jose@estudiantat.upc.edu.
