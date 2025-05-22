# Databricks notebook source
#sparkDF = spark.read.format("delta").load("/mnt/gold-zone-bdm/enriched_transactions").limit(1000)
#sparkDF.write.format("parquet").mode("overwrite").save("/mnt/gold-zone-bdm/subset_enriched_transactions")

# COMMAND ----------

from pyspark.sql.functions import col, when, lit
from graphframes import GraphFrame
import time

# Start time for logging
start_time = time.time()

# Step 2: Load and Sample Data, Build Graph (M4)
print("Starting Step 2: Loading and sampling data...")
sparkDF = spark.read.format("delta").load("/mnt/gold-zone-bdm/enriched_transactions")
# Sample 100,000 records (~0.35% of 28M)
sparkDF = sparkDF.sample(fraction=0.0035, seed=42).limit(100000)
print(f"Sampled {sparkDF.count()} records")

# Derive age_group
sparkDF = sparkDF.withColumn(
    "age_group",
    when(col("age") < 30, "Youth")
    .when((col("age") >= 30) & (col("age") <= 50), "Adult")
    .otherwise("Senior")
)

# COMMAND ----------

# Create vertices with aligned schemas
print("Creating vertices...")
customer_vertices = sparkDF.select(
    col("customer_id").alias("id"),
    lit("Customer").alias("type"),
    col("age_group").alias("property1"),
    lit(None).cast("string").alias("property2")
).distinct()

article_vertices = sparkDF.select(
    col("article_id").alias("id"),
    lit("Article").alias("type"),
    col("prod_name").alias("property1"),
    col("color_category").alias("property2")
).distinct()

vertices = customer_vertices.union(article_vertices)

# Create edges
print("Creating edges...")
edges = sparkDF.select(
    col("customer_id").alias("src"),
    col("article_id").alias("dst"),
    col("t_dat"),
    col("price"),
    col("transaction_year"),
    col("transaction_month")
).withColumn("relationship", lit("PURCHASE"))

# Optimize with smaller partitions for the sample
vertices = vertices.repartition(20).persist()
edges = edges.repartition(20).persist()
print(f"Vertices: {vertices.count()}, Edges: {edges.count()}")

# Create GraphFrame
graph = GraphFrame(vertices, edges)
print(f"Step 2 completed in {(time.time() - start_time) / 60:.2f} minutes")

# COMMAND ----------

# Step 3: Run Analytics (M5)
print("Starting Step 3: Running graph analytics...")
step3_start = time.time()

print("Running PageRank...")
pagerank_start = time.time()
pagerank_result = graph.pageRank(resetProbability=0.15, maxIter=3)
trending_articles = pagerank_result.vertices.filter(col("type") == "Article").select(
    col("id").alias("article_id"),
    col("property1").alias("prod_name"),
    col("property2").alias("color_category"),
    col("pagerank")
).orderBy(col("pagerank").desc()).limit(10)
print(f"PageRank completed in {(time.time() - pagerank_start) / 60:.2f} minutes")

print("Setting checkpoint directory...")
spark.sparkContext.setCheckpointDir("dbfs:/tmp/checkpoints")

print("Running Connected Components...")
cc_start = time.time()
cc_result = graph.connectedComponents()
customer_segments = cc_result.filter(col("type") == "Customer").select(
    col("id").alias("customer_id"),
    col("component").alias("community")
).limit(10)
print(f"Connected Components completed in {(time.time() - cc_start) / 60:.2f} minutes")

print("PageRank Results:")
trending_articles.show()
print("Connected Components Results:")
customer_segments.show()
print(f"Step 3 completed in {(time.time() - step3_start) / 60:.2f} minutes")

# COMMAND ----------

# Step 4: Store Metadata (M6)
print("Starting Step 4: Storing metadata...")
pagerank_metadata = trending_articles.select(
    col("article_id").alias("entity_id"),
    lit("Article").alias("entity_type"),
    lit("PageRank").alias("metric"),
    col("pagerank").cast("double").alias("value")
)

community_metadata = customer_segments.select(
    col("customer_id").alias("entity_id"),
    lit("Customer").alias("entity_type"),
    lit("Community").alias("metric"),
    col("community").cast("double").alias("value")
)

metadata_df = pagerank_metadata.union(community_metadata)
metadata_df.write.format("delta").mode("overwrite").save("/mnt/gold-zone-bdm/graph_metadata")
print("Metadata saved to Delta")

# COMMAND ----------

# Step 5: Verify (M7)
print("Starting Step 5: Verifying results...")
metadata_df = spark.read.format("delta").load("/mnt/gold-zone-bdm/graph_metadata")

# Join with trending_articles to include prod_name and color_category for articles
articles_with_details = metadata_df.filter(col("entity_type") == "Article")\
    .join(trending_articles, metadata_df.entity_id == trending_articles.article_id, "left")\
    .select(
        metadata_df.entity_id,
        metadata_df.entity_type,
        metadata_df.metric,
        metadata_df.value,
        trending_articles.prod_name,
        trending_articles.color_category
    )

# Customers remain as-is
customers = metadata_df.filter(col("entity_type") == "Customer")

# Combine and display
final_output = articles_with_details.union(customers.select("entity_id", "entity_type", "metric", "value", lit(None).alias("prod_name"), lit(None).alias("color_category")))
final_output.show()
print(f"Total runtime: {(time.time() - start_time) / 60:.2f} minutes")


# COMMAND ----------

vertices.unpersist()
edges.unpersist()

# COMMAND ----------

