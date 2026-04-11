#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, to_timestamp


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("Day3 Telematics Project") \
        .getOrCreate()


# In[3]:


spark.conf.set("spark.sql.adaptive.enabled","false")


# In[4]:


df_tele = spark.read.csv("/opt/spark-data/telematics.csv",header=True,inferSchema=True)
df_vehicle = spark.read.csv("/opt/spark-data/vehicles.csv",header=True,inferSchema=True)


# In[5]:


df_tele.printSchema()
df_vehicle.show()
df_tele.show(5)


# In[6]:


from pyspark.sql.functions import lit

df_big = df_tele
df_big = df_big.withColumn("dup_id",lit(0))
for i in range(20):   # increase multiplier gradually
    df_big = df_big.union(df_tele.withColumn("dup_id", lit(i)))

df_big.count()


# In[7]:


df_tele = df_big


# In[8]:


df_tele = df_tele.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)


# In[9]:


from pyspark.sql.functions import when

df_tele = df_tele.withColumn(
    "speed_category",
    when(col("speed") < 40, "low")
    .when(col("speed") < 80, "medium")
    .otherwise("high")
)

df_tele = df_tele.withColumn(
    "engine_stress",
    col("engine_temp") * col("speed")
)


# In[10]:


df_agg = df_tele.groupBy("vehicle_id").agg(
    {"speed": "avg", "fuel_level": "avg"}
)

df_agg.show()


# In[11]:


df_joined = df_tele.join(df_vehicle, on="vehicle_id", how="inner")
df_joined.explain()


# In[12]:


from pyspark.sql.functions import broadcast

df_joined_broadcast = df_tele.join(
    broadcast(df_vehicle),
    on="vehicle_id",
    how="inner"
)

df_joined_broadcast.explain()


# In[15]:


import time

start = time.time()
df_tele.join(df_vehicle, "vehicle_id").count()
print("Shuffle Join Time:", time.time() - start)

start = time.time()
df_tele.join(broadcast(df_vehicle), "vehicle_id").count()
print("Broadcast Join Time:", time.time() - start)


# ### Imputing disturbances/skewness in the dataset

# In[16]:


df_big.groupBy("vehicle_id").count().show()


# In[17]:


from pyspark.sql.functions import when, rand

df_skewed = df_big.withColumn(
    "vehicle_id",
    when(rand() < 0.7, "V1").otherwise(df_big.vehicle_id)
)


# In[28]:


df_skewed_part = df_skewed.repartition(5, "vehicle_id")


# In[29]:


df_skewed_part.groupBy("vehicle_id").count().show()


# In[45]:


df_skewed_part.rdd.glom().map(len).collect()


# In[35]:


spark.conf.get("spark.sql.adaptive.enabled")


# In[36]:


import time

start = time.time()
df_skewed_part.join(df_vehicle, "vehicle_id").count()
print("Shuffle Join Time:", time.time() - start)

start = time.time()
df_skewed_part.join(broadcast(df_vehicle), "vehicle_id").count()
print("Broadcast Join Time:", time.time() - start)

