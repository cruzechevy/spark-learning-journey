# 📅 Day 02 --- Vehicle Health Analytics Pipeline (Feature Engineering, Aggregation, Resource Management)

## 🎯 Objective

Build a real-world Spark data pipeline on telemetry data to detect risky
vehicle behavior, generate aggregated insights, and understand
distributed resource management.

## ⚡ Spark Topics Covered

-   DataFrame Transformations (filter, join, withColumn, groupBy)
-   Feature Engineering using conditional logic
-   Aggregations and shuffle behavior
-   Fact + Dimension joins
-   Lazy Evaluation and Action triggers
-   Spark Execution (Driver → DAG → Stages → Tasks)
-   Resource Management in Spark Cluster
-   Spark UI for monitoring and debugging

## 💼 Business Problem

A fleet management system needs to monitor vehicle health and
operational risks in near real-time.

Key questions: - Which vehicles are risky? - Which models or plants are
underperforming? - How can we prioritize operational interventions?

## 🛠️ Solution Approach

### Step 1: Data Enrichment

Joined telemetry with vehicle metadata.

### Step 2: Feature Engineering

-   is_over_speed → speed \> 90\
-   is_low_fuel → fuel_level \< 20\
-   is_overheat → engine_temp \> 110

### Step 3: Aggregation

Grouped by vehicle_id, model, plant and calculated: - avg_speed\
- avg_engine_temp\
- low_fuel_events\
- overheat_events\
- overspeed_events

### Step 4: Risk Scoring (Normalized)

Normalized metrics and applied weighted scoring:

df_poormodels = df_poormodels.withColumn( "riskscore", 100 - (
((df_poormodels.avg_engine_temp / max_avg_engine_temp) \* 100 \* 0.5) +
((df_poormodels.overheated_vehicles / max_overheat) \* 100 \* 0.5) ) )

### Step 5: Output

Generated: - vehicle_health dataset\
- plant_summary dataset

## 🔄 Notebook to Production

Converted using: jupyter nbconvert --to script Day2.ipynb

## ⚙️ How to Run

docker exec -it spark-submit /opt/spark/bin/spark-submit\
--master spark://spark-master:7077\
--total-executor-cores 4\
/opt/spark-apps/day2_job.py

## 🚨 Real-World Issue

Job stuck due to resource contention.

## 🧠 Key Learnings

-   Feature engineering → business signals\
-   Aggregation → shuffle\
-   Normalization → fair scoring\
-   Resource management is critical\
-   Spark UI helps debugging

## 📌 Summary

Built a production-style Spark pipeline with real-world constraints.
