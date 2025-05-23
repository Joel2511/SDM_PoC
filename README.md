# TrendSync SDM Join Project
Overview

This repository contains the Semantic Data Management (SDM) component of the BDMA Joint Project for TrendSync, a simulated start-up focused on inventory optimization and demand forecasting in the fashion industry. The Proof of Concept (PoC) uses property graphs to model customer-article purchase relationships, leveraging graph analytics to identify trending articles and customer segments. This supports TrendSync’s goals of optimizing inventory and improving forecasting accuracy.

The implementation is built using PySpark and GraphFrames in Azure Databricks, processing a sampled dataset of 100,000 records from a 28M-record transaction dataset.
Repository Structure

    1.SDM_POC.ipynb: The main Databricks notebook containing the PoC code, including Markdown explanations and embedded results.
    2.TrendSyncMountAzureStorage.ipynb: A script to mount the Azure Blob Storage container in Databricks.
    3.README.md
    4.PoC documentation (SDM_POC.pdf)

Setup Instructions

This PoC was developed in Azure Databricks. While the code is not directly executable outside Databricks, the notebook includes all steps, explanations, and results for review.
Environment

Platform: Azure Databricks (Standard_D3_v2 cluster, 4 vCPUs, 14 GB RAM).
## Prerequisites
To run this project successfully, ensure the following:
- **Databricks Runtime Version**: Use **Databricks Runtime 14.3 LTS** (Long Term Support) to avoid compatibility issues with dependencies such as GraphFrames (`graphframes:graphframes:0.8.2-spark3.2-s_2.12`) and other libraries used in the notebooks.
- **Azure Blob Storage**: Configure access with appropriate SAS tokens as specified in the notebooks.
- **Dependencies**: Install the following library on your Databricks cluster:
- **GraphFrames**: Add via Maven with coordinates `graphframes:graphframes:0.8.2-spark3.2-s_2.12` (see installation instructions below).
    Dependencies:
        1.PySpark (included in Databricks).
        2.GraphFrames: Add to the Databricks cluster via Maven (graphframes:graphframes:0.8.2-spark3.2-s_2.12).
        3.Python libraries: time (standard library).

Mounting Storage

The code accesses data from an Azure Blob Storage container (gold-zone-bdm). To replicate the setup:

    Ensure access to an Azure Blob Storage account with the enriched_transactions Delta table.
    Run the TrendSyncMountAzureStorage.ipynb script in Databricks:


Running the Code

The code is not executable outside Databricks due to its dependency on GraphFrames and Spark. However, the results are embedded in the notebook for review.

    Open the Notebook:
        Import SDM_POC.ipynb into a Databricks workspace.
    Install Dependencies:
        Ensure GraphFrames is installed on the cluster (see Dependencies above).
    Mount Storage:
        Run the mounting script as described above (or ensure /mnt/gold-zone-bdm/enriched_transactions is accessible).
    Execute the Notebook:
        Run all cells in the notebook. It samples 100,000 records, builds a property graph, runs PageRank and Connected Components, stores metadata, and verifies results.
        Note: Without access to the original dataset, execution will fail, but results are visible in the notebook.

Results

The results of the PoC are embedded in SDM_POC.ipynb under Step 5 (Verifying Results). The output includes:

    Top 10 trending articles by PageRank score (e.g., article 706016001, score 72.90, "Jade HW Skinny Denim", "Black Dark").
    Top 10 customer segments by community ID (e.g., customer in community 7.0). These insights support FashionFlow’s inventory optimization and demand forecasting goals.

Additional Notes

    The PoC was executed on a sampled dataset (100,000 records), ensuring feasibility within time and resource constraints.
    The total runtime on the sampled dataset was 4.64 minutes.

Contact
For questions, please contact Joel Anil Jose via email (joel.anil.jose@estudiantat.upc.edu).
