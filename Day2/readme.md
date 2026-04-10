# 🚀 Spark Learning Journey — From Fundamentals to Real-World Pipelines

This repository documents my hands-on journey in learning **Apache Spark** with a strong focus on:

- Distributed system fundamentals  
- Data engineering pipelines  
- Real-world problem solving using PySpark  
- Transition from notebooks → production-ready jobs  

---

# 🏗️ Environment Setup

- Spark Standalone Cluster using Docker  
  - 1 Master  
  - 1 Worker  
- Jupyter Notebook for exploration  
- PySpark jobs executed via `spark-submit`  

📂 Data Path:/opt/spark-data/
📂 Jobs Path:/opt/spark-apps/


---

# 📊 Dataset Overview

### 1. telematics.csv (Fact Table)

Time-series vehicle telemetry data:

- timestamp  
- vehicle_id  
- speed  
- engine_temp  
- fuel_level  
- lat, lon  

---

### 2. vehicles.csv (Dimension Table)

Static vehicle metadata:

- vehicle_id  
- model  
- plant  
- year  

---

# 📅 Day 01 — Apache Spark Architecture

## 🎯 Objective

Understand how Spark executes jobs internally.

---

## 🔍 Concepts Covered

- Driver & Executor roles  
- DAG (Directed Acyclic Graph)  
- Lazy Evaluation  
- Stage & Task execution  

---

## 🧪 Workflow

1. Created SparkSession (Driver)  
2. Loaded datasets  
3. Applied transformations:
   - Filter → speed > 60  
   - Join → telematics + vehicles  
   - Aggregation → avg(speed) by model  
4. Triggered execution using `.show()`  
5. Analyzed execution plan using:
   - `explain(True)`  
   - Spark UI  

---

## ⚙️ Execution Flow

1. Driver initializes job  
2. Logical Plan created  
3. Catalyst Optimizer optimizes query  
4. Physical Plan generated  
5. DAG split into stages  
6. Tasks executed by workers  

---

## 🔍 Key Observations

- Lazy Evaluation → No execution until action  
- Column Pruning → Only required columns processed  
- Filter Pushdown → Applied at source  
- Broadcast Join → Small dataset optimized  
- Shuffle → Triggered by `groupBy`  
- Adaptive Query Execution (AQE) enabled  

---

## 📉 Stage Breakdown

- Stage 1 → Read + Filter + Join  
- Stage 2 → Shuffle + Aggregation  

---

## 💻 Production Conversion

Notebook converted into script: common/jobs/day01_job.py


Executed using:

```bash
docker exec -it spark-submit /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/day01_job.py
