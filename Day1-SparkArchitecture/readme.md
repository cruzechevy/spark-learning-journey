# Day 01 — Apache Spark Architecture (Driver, Executor, DAG, Lazy Evaluation)

## 🎯 Objective

Understand how Apache Spark executes a job internally using:

* Driver & Executor roles
* DAG (Directed Acyclic Graph)
* Lazy Evaluation
* Stage & Task execution

---

## 🏗️ Setup (Local Distributed Environment)

* Spark Standalone Cluster using Docker

  * 1 Master
  * 1 Worker
* Jupyter Notebook for exploration
* PySpark job executed via `spark-submit`

📂 Data Location:

```
/opt/spark-data/
```

---

## 📊 Dataset Used (Telematics Simulation)

### 1. telematics.csv (Fact Table)

* Time-series vehicle data (30 days)
* Columns:

  * timestamp, vehicle_id, speed, engine_temp, fuel_level, lat, lon

### 2. vehicles.csv (Dimension Table)

* Static vehicle metadata
* Columns:

  * vehicle_id, model, plant, year

---

## 🧪 Notebook Workflow (Exploration Phase)

### Steps Performed

1. Created SparkSession (Driver)
2. Loaded both datasets
3. Applied transformations:

   * Filter → speed > 60
   * Join → telematics + vehicles
   * Aggregation → avg(speed) by model
4. Triggered action using `.show()`
5. Analyzed execution using:

   * `result.explain(True)`
   * Spark UI (Stages & DAG)

---

## ⚙️ Execution Flow (What Actually Happens)

1. **spark-submit / notebook starts Driver**
2. Spark builds a **Logical Plan**
3. Catalyst Optimizer creates an **Optimized Plan**
4. Spark converts it into a **Physical Plan**
5. DAG is split into **Stages**
6. Tasks are sent to **Executors**
7. Executors process partitions
8. Result returned to Driver

---

## 🔍 Key Observations from Execution Plan

### ✅ Lazy Evaluation

* Transformations were not executed until `.show()` was called

---

### ✅ Column Pruning

* Only required columns (`speed`, `model`) were processed

---

### ✅ Filter Pushdown

* Filter (`speed > 60`) applied at data read level

---

### ✅ Broadcast Join

* Small dataset (`vehicles.csv`) broadcasted
* Avoided shuffle during join

---

### ✅ Shuffle Occurred

* `groupBy(model)` caused:

  * Data redistribution
  * Stage boundary

---

### ✅ Adaptive Query Execution (AQE)

* Spark dynamically optimized execution during runtime

---

## 📉 Stage Breakdown (From Spark UI)

* **Stage 1** → Read + Filter + Join
* **Stage 2** → Shuffle + Aggregation

---

## 🧠 Key Learnings

* Spark does **not execute immediately** → Lazy evaluation
* Execution is driven by **actions**, not transformations
* DAG defines execution flow
* **Wide transformations (groupBy)** cause shuffle
* Spark automatically optimizes:

  * Join strategy
  * Column selection
  * Filter placement

---

## 💻 Production Conversion

Notebook logic was converted into a structured PySpark job:

```
common/jobs/day01_job.py
```

Executed using:

```bash
docker exec -it spark-submit /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/day01_job.py
```

---

## ⚠️ Important Real-World Insight

* Data must be accessible to **executors**, not just driver
* In production:

  * Data is stored in S3 / HDFS
  * Not local volumes

---

## 📌 Summary

This exercise demonstrated:

* End-to-end Spark execution
* Distributed processing behavior
* Internal optimization mechanisms

---
