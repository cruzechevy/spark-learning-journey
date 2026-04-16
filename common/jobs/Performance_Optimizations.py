#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("Day4 Caching and Performance") \
        .getOrCreate()


# In[2]:


spark.conf.set("spark.sql.adaptive.enabled","false")


# In[21]:


from pyspark.sql.functions import col, concat, lit, explode, array, rand, when, monotonically_increasing_id,explode, sequence, to_timestamp, expr,avg,max,min
import time


# ## Vehicles Table

# In[ ]:


from pyspark.sql import Row

data = [
    Row(vehicle_id="V1", brand="Toyota", vehicle_type="Car", fuel_type="Petrol", purchase_year=2018),
    Row(vehicle_id="V2", brand="Honda", vehicle_type="Car", fuel_type="Diesel", purchase_year=2019),
    Row(vehicle_id="V3", brand="Hyundai", vehicle_type="Car", fuel_type="Petrol", purchase_year=2020),
    Row(vehicle_id="V4", brand="Tata", vehicle_type="Truck", fuel_type="Diesel", purchase_year=2017),
    Row(vehicle_id="V5", brand="Mahindra", vehicle_type="SUV", fuel_type="Diesel", purchase_year=2021),
]

vehicles = spark.createDataFrame(data)
vehicles.show()


# In[ ]:


# Step 1: Create suffixes to expand 5 → 100
multipliers = [f"_{i}" for i in range(1, 21)]  # 5 * 20 = 100

vehicles_expanded = vehicles.withColumn(
    "suffix", explode(array([lit(x) for x in multipliers]))
)

# Step 2: Create new vehicle_id
vehicles_expanded = vehicles_expanded.withColumn(
    "vehicle_id", concat(col("vehicle_id"), col("suffix"))
).drop("suffix")

# Step 3: Add realistic variation

# City distribution (important for Spark partitions)
vehicles_expanded = vehicles_expanded.withColumn(
    "city",
    when(rand() < 0.3, "Bangalore")
    .when(rand() < 0.6, "Mumbai")
    .otherwise("Delhi")
)

# Purchase year variation (+/- 2–3 years)
vehicles_expanded = vehicles_expanded.withColumn(
    "purchase_year",
    col("purchase_year") + (rand()*3).cast("int")
)

# Unique driver_id (avoid unrealistic joins)
vehicles_expanded = vehicles_expanded.withColumn(
    "driver_id",
    monotonically_increasing_id()
)

# Optional: Add region (useful for skew later)
vehicles_expanded = vehicles_expanded.withColumn(
    "region",
    when(rand() < 0.7, "South").otherwise("North")
)

# Step 4: Validate
print("Final Vehicle Count:", vehicles_expanded.count())
vehicles_expanded.show(5, truncate=False)


# ## Telematics

# In[ ]:


from pyspark.sql.functions import explode, sequence, to_timestamp, expr

# Generate timestamps (e.g., 1000 events per vehicle)
telematics = vehicles_expanded.select("vehicle_id").withColumn(
    "event_time",
    explode(
        sequence(
            to_timestamp(lit("2024-01-01 00:00:00")),
            to_timestamp(lit("2024-01-10 00:00:00")),
            expr("interval 5 minutes")
        )
    )
)


# In[ ]:


from pyspark.sql.functions import rand

telematics = telematics.withColumn("speed", (rand()*100).cast("int")) \
                       .withColumn("distance", (rand()*5).cast("double")) \
                       .withColumn("fuel_consumed", (rand()*2).cast("double")) 

telematics = telematics.withColumn("engine_temp",(col("speed") * 0.7 + rand()*30 + 60).cast("double"))


# In[ ]:


telematics_large = telematics

for _ in range(5):  # adjust (5–7)
    telematics_large = telematics_large.unionByName(telematics_large)

telematics_large = telematics_large.withColumn("speed",when(rand() < 0.05, rand()*120)  # occasional spikes
    .otherwise(col("speed"))
)

print("Row count:", telematics_large.count())


# In[ ]:


telematics_large.write.mode("overwrite").parquet("/opt/spark-data/telematics_bigdata")
vehicles_expanded.write.mode("overwrite").parquet("/opt/spark-data/vehicles_bigdata")


# In[ ]:


telematics_large.show(1)


# ## Caching

# In[4]:


telematics_large = spark.read.parquet("/opt/spark-data/telematics_bigdata")
vehicles_expanded = spark.read.parquet("/opt/spark-data/vehicles_bigdata")


# In[5]:


vehicle_telematics = telematics_large.join(vehicles_expanded,"vehicle_id")
vehicle_telematics.show(1)


# In[6]:


vehicle_telematics.select("vehicle_type").distinct().show()


# In[7]:


vehicle_telematics = vehicle_telematics.withColumn(
    "speed",
    when(col("vehicle_type") == "Truck", rand()*(40-20) + 20)
    .when(col("vehicle_type") == "SUV",  rand()*(80-40) + 40)
    .otherwise( rand()*(120-80) + 80)
)


# In[9]:


vehicle_telematics.count()


# In[10]:


vehicle_telematics.select("speed").distinct().show()


# In[11]:


# Apply transformations
filtered_df = vehicle_telematics.filter(col("speed") > 10)

agg_df = filtered_df.groupBy("vehicle_id", "city").agg(
    avg("speed").alias("avg_speed"),
    avg("engine_temp").alias("avg_temp"),
    max("speed").alias("max_speed"),
    min("fuel_consumed").alias("min_fuel")
)


# In[12]:


quantiles = agg_df.approxQuantile("avg_speed", [0.33, 0.66], 0.01)

low_th = quantiles[0]
high_th = quantiles[1]

print(low_th, high_th)


# In[13]:


import time

start = time.time()

high_speed = agg_df.filter(col("avg_speed") > high_th).count()
medium_speed = agg_df.filter((col("avg_speed") > low_th) & (col("avg_speed") <= high_th)).count()
low_speed = agg_df.filter(col("avg_speed") <= low_th).count()

end = time.time()

print(high_speed, medium_speed, low_speed)
print("Time WITHOUT cache:", end - start)


# In[14]:


agg_df.cache()
agg_df.count()


# In[15]:


start = time.time()

high_speed = agg_df.filter(col("avg_speed") > 80).count()
medium_speed = agg_df.filter((col("avg_speed") > 40) & (col("avg_speed") <= 80)).count()
low_speed = agg_df.filter(col("avg_speed") <= 40).count()

end = time.time()

print("Time WITH cache:", end - start)


# ## Identify Inefficiencies using Execution Plans

# In[16]:


query_df = telematics_large.join(vehicles_expanded, "vehicle_id") \
    .filter(col("speed") > 30) \
    .groupBy("city", "vehicle_type") \
    .agg(avg("engine_temp").alias("avg_temp"))


# In[ ]:


query_df.explain(True)


# In[ ]:


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


# In[ ]:


query_df.explain(True)


# In[ ]:


from pyspark.sql.functions import broadcast

optimized_df = telematics_large.join(
    broadcast(vehicles_expanded), "vehicle_id"
).filter(col("speed") > 30) \
 .groupBy("city", "vehicle_type") \
 .agg(avg("engine_temp").alias("avg_temp"))


# In[ ]:


optimized_df.explain(True)


# In[ ]:


start = time.time()
query_df.count()
print("Before optimization:", time.time() - start)

start = time.time()
optimized_df.count()
print("After optimization:", time.time() - start)


# ## handling Data Skew

# In[8]:


from pyspark.sql.functions import when, rand, col

skewed_df = telematics_large.withColumn(
    "vehicle_id",
    when(rand() < 0.3, "V1_1").otherwise(col("vehicle_id"))
)


# In[9]:


skewed_df.show(10)


# In[19]:


skewed_df = skewed_df.sample(0.5)
skewed_df.count()


# In[20]:


skew_query = skewed_df.join(vehicles_expanded, "vehicle_id") \
    .groupBy("vehicle_id") \
    .count()

skew_query.explain(True)


# In[21]:


skewed_df.groupBy("vehicle_id").count().orderBy(col("count").desc()).show()


# ### Adding Salt Technique

# In[10]:


from pyspark.sql.functions import floor

skewed_df = skewed_df.withColumn(
    "salt", floor(rand()*2)  # 5 buckets
)


# In[11]:


vehicles_salted = vehicles_expanded.withColumn(
    "salt", explode(array([lit(i) for i in range(2)]))
)


# In[14]:


vehicles_salted.show(100)


# In[13]:


skewed_df.show(10)


# In[24]:


salted_join = skewed_df.join(
    vehicles_salted,
    ["vehicle_id", "salt"]
)


# In[25]:


start = time.time()
skew_query.count()
print("Before skew fix:", time.time() - start)


# In[26]:


# After fix
start = time.time()
salted_join.groupBy("vehicle_id").count().count()
print("After skew fix:", time.time() - start)


# ## Experimenting with Different File Formats

# In[16]:


telematics_large.write.mode("overwrite").csv("tmp/telematics_csv",header=True)


# In[18]:


telematics_large.write.mode("overwrite").parquet("tmp/telematics_parquet")


# In[19]:


telematics_large.write.mode("overwrite").format("delta").save("tmp/telematics_delta")


# In[26]:


start = time.time()

df_csv = spark.read.option("header",True).option("inferSchema",True).csv("tmp/telematics_csv/")

df_csv.filter("speed > 50").groupBy("vehicle_id").agg(avg("engine_temp")).count()

print("CSV Time:", time.time() - start)


# In[27]:


start = time.time()

df_parquet = spark.read.parquet("tmp/telematics_parquet/")

df_parquet.filter("speed > 50").groupBy("vehicle_id").agg(avg("engine_temp")).count()

print("Parquet Time:", time.time() - start)


# In[32]:


df_parquet.explain(True)


# In[31]:


import os

def get_size(path):
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / (1024)  # MB

print("CSV Size:", get_size("tmp/telematics_csv"), "MB")
print("Parquet Size:", get_size("tmp/telematics_parquet"), "MB")

