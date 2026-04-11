# 📅 Day 3 – Spark DataFrame API, Joins & Partitioning

---

## 🚀 Overview

On Day 3, I worked on building a **real-world PySpark data pipeline** using vehicle telematics data.
The focus was on **data transformation, join optimization, and partition tuning** — core concepts required for a Data Engineer role.

Unlike Day 2 (Notebook execution), this pipeline was executed using:

👉 `spark-submit` (production-style execution)

---

## 🧱 Problem Statement

Analyze vehicle telemetry data (43K+ records) and enrich it with vehicle metadata to generate insights on:

* Vehicle performance
* Fuel efficiency
* Engine stress patterns

---

## ⚙️ Tech Stack

* PySpark (DataFrame API)
* Docker (Spark Cluster – Master + Worker)
* Jupyter Notebook (Development)
* spark-submit (Execution)
* GitHub (Version Control)

---

## 🏗️ Infrastructure Setup

* Spark running in Docker containers (1 Master + 1 Worker)
* Job submitted using:

```bash
spark-submit day3_telematics.py
```

* Spark UI confirms successful execution with:

  * 1 Worker (16 cores)
  * Job duration ~32 seconds
  * Application: `Day3 Telematics Project`

````

---

## 📊 Dataset Description

### 🔹 Telematics Data (Fact Table)
- ~43,000 records
- Contains:
  - timestamp  
  - vehicle_id  
  - speed  
  - engine_temp  
  - fuel_level  
  - location (lat, lon)

### 🔹 Vehicle Data (Dimension Table)
- 5 records
- Contains:
  - vehicle_id  
  - model  
  - plant  
  - year  

---

## 🔄 Data Processing Steps

### 1️⃣ Data Loading
- Loaded CSV files into Spark DataFrames
- Inferred schema for structured processing

---

### 2️⃣ Data Cleaning
- Converted `timestamp` to proper timestamp type
- Ensured numeric fields are correctly typed

---

### 3️⃣ Feature Engineering

Created new derived columns:

- `speed_category` → low / medium / high  
- `engine_stress` → speed × engine_temp  

---

### 4️⃣ Aggregations

- Average speed per vehicle  
- Average fuel level per vehicle  

---

## 🔗 Join Strategy (Critical Learning)

### ❌ Default (Shuffle Join)
- Causes data movement across partitions
- Expensive for large datasets

### ✅ Optimized (Broadcast Join)

Used broadcast join for small dimension table:

```python
from pyspark.sql.functions import broadcast

df_joined = df_tele.join(
    broadcast(df_vehicle),
    on="vehicle_id",
    how="inner"
)
````

### 💡 Insight

* Since vehicle dataset is very small (5 rows), broadcasting avoids shuffle and improves performance
So we again increased the dataset size from 43200 rows 907200 rows

---

## ⚙️ Partitioning & Optimization

### 🔹 Repartition

```python
df_repart = df_tele.repartition(8, "vehicle_id")
```

* Increases parallelism
* Causes shuffle
* Useful for large transformations

---

### 🔹 Coalesce

```python
df_coalesce = df_repart.coalesce(2)
```

* Reduces partitions
* Avoids shuffle
* Useful before writing output

---

## 🔬 Performance Experiments

### 🔹 Join Comparison

* Compared:

  * Shuffle Join vs Broadcast Join
* Observed:

  * Broadcast join executed faster

---

### 🔹 Partitioning Impact

* Compared:

  * Default partitions vs Repartitioned dataset
* Observed:

  * Better parallelism improved execution time

---

### 🔹 Skew Impact

* Compared:

  * Default partitions vs Repartitioned dataset after importing a 70% skew on one vehicle_id
* Observed:

  * Used: df_repart.rdd.glom().collect() To inspect how data is distributed across partitions.

---

## 📈 Execution Details

* Application Name: **Day3 Telematics Project**
* Execution Mode: `spark-submit`
* Cluster:

  * 1 Worker
  * 16 cores
  * 6.6 GB memory
* Execution Time: ~32 seconds

---

## 🧠 Key Learnings

* Deep understanding of **Spark DataFrame API**
* Practical usage of **broadcast joins**
* Difference between **shuffle vs broadcast joins**
* When to use **repartition vs coalesce**
* Running Spark jobs using **spark-submit**
* Observing execution via **Spark UI**
* Hash Partitioning Behavior : Spark does not strictly distribute data evenly by key
        Instead, it uses hash partitioning:
        partition = hash(key) % num_partitions

        👉 This means:

        Same keys go to same partition ✔️
        But distribution may still be uneven ❗

---

## 🎯 What This Demonstrates

This project demonstrates:

* Ability to build scalable data pipelines
* Understanding of distributed data processing
* Performance optimization techniques in Spark
* Transition from notebook-based work to production-style execution
* Experiments and impact on imputing data skewness and controlling the partition by data values
---
