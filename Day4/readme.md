# Spark Performance Debugging Playbook

This project demonstrates practical Spark optimization techniques using production-like scenarios. The focus is on understanding *why* performance issues occur and *how* to fix them using data-driven approaches.

---

## 🔍 Key Areas Covered

- Caching & recomputation
- Execution plan analysis
- Join strategy optimization
- Data skew handling
- File format optimization (CSV vs Parquet)

---

## 📊 Dataset

- Simulated telematics dataset (~5M–10M rows)
- Vehicle dimension (~100 entities)
- Includes realistic features:
  - speed
  - engine temperature
  - fuel consumption
- Designed with controlled skew and distribution

---

## ⚙️ Tech Stack

- Apache Spark (PySpark)
- Docker (local setup)
- Jupyter Notebook

---

# 🚀 Case Studies

---

## 📌  Caching Optimization

### Problem
Repeated transformations caused recomputation due to Spark's lazy evaluation.

### Solution
Applied caching on intermediate aggregated DataFrame.

### Result
- Runtime reduced from **2.73s → 0.95s (~65% improvement)**

### Insight
Caching is effective when intermediate results are reused multiple times.

---

## 📌 Execution Plan Optimization

### Problem
Join strategy impacting performance.

### Solution
- Analyzed execution plan using `explain()`
- Compared Broadcast vs SortMerge joins

### Result
- Broadcast was slower in local setup
- Shuffle join performed better

### Insight
Join strategy effectiveness depends on environment and data size.

---

## 📌 Data Skew Handling

### Problem
Uneven data distribution caused inefficient parallelism.

### Solution
- Identified skew using aggregation
- Applied salting technique

### Challenge
- Initial salting caused memory issues and disk spill

### Final Fix
- Reduced skew intensity and salt buckets

### Insight
Skew handling introduces trade-offs between parallelism and memory usage.

---

## 📌 File Format Optimization

### Problem
CSV-based storage caused inefficient reads and large storage usage.

### Solution
Compared CSV vs Parquet formats.

### Result
- Parquet reduced storage by **~3x**
- Improved query performance

### Insight
File format optimization can significantly improve performance without changing business logic.

---

# 🧠 Key Learnings

- Performance issues are often data-driven, not code-driven
- Understanding execution plans is critical for optimization
- Trade-offs exist in every optimization (memory vs compute vs storage)
- File format choice has a major impact on system efficiency

---

# 🎯 Conclusion

This project focuses on practical debugging and optimization techniques used in real-world Spark pipelines, emphasizing a systems-thinking approach over theoretical knowledge.