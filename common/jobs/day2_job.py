#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Day2_Telematics") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


# In[30]:


from pyspark.sql.functions import when,avg,sum,max


# In[2]:


df_tele = spark.read.csv("/opt/spark-data/telematics.csv", header=True, inferSchema=True)
df_vehicle = spark.read.csv("/opt/spark-data/vehicles.csv", header=True, inferSchema=True)


# In[3]:


df_tele.show(5)


# In[4]:


df_vehicle.show()


# ## 🎯 Business Scenario
# 
# You are working for:
# 
# Fleet / IoT / Logistics company (Uber / Bosch / Caterpillar type)
# 
# They want a daily monitoring dataset to answer:
# 
# 👉 Which vehicles are risky?
# 👉 Which plants/models are underperforming?
# 👉 Where should operations teams take action?

# In[5]:


df = df_tele.join(df_vehicle,on='vehicle_id',how='inner')


# In[14]:


df.show()


# ### Identify Risky Vehicles and Score the level of Risk (Vehicles with Engine Temp above 100 or Fuel Level below 20)

# In[22]:


df = df.withColumn("is_overspeed",when(df.speed > 90,1).otherwise(0)) \
    .withColumn("is_low_fuel",when(df.fuel_level<20,1).otherwise(0)) \
    .withColumn("is_overheat",when(df.engine_temp>110,1).otherwise(0))
        


# In[42]:


df_risky_vehicles = df.groupBy("vehicle_id","model","plant").agg(avg("speed").alias("avg_Speed")
                                                                 ,avg("engine_temp").alias("avg_engine_temp")
                                                                 ,avg("fuel_level").alias("avg_fuel_level")
                                                                 ,sum("is_overspeed").alias("speeding_vehicles")
                                                                 ,sum("is_low_fuel").alias("fuel_low_events")
                                                                 ,sum("is_overheat").alias("overheated_vehicles")
                                                                )


# In[43]:


df_risky_vehicles = df_risky_vehicles.withColumn("riskscore",(df_risky_vehicles.speeding_vehicles * 3) 
                                                             + (df_risky_vehicles.fuel_low_events * 2)
                                                             + (df_risky_vehicles.overheated_vehicles * 3)
                                                )
max_val = df_risky_vehicles.agg(max(df_risky_vehicles.riskscore)).collect()[0][0]

df_risky_vehicles = df_risky_vehicles.withColumn("riskscore",(df_risky_vehicles.riskscore/max_val)*10)


# In[45]:


df_risky = df_risky_vehicles.filter(df_risky_vehicles.riskscore > 5)


# In[46]:


df_risky.show(10)


# ### Identify Underperforming Models and Plants

# In[59]:


df_poormodels = df.groupBy("plant","model").agg(avg("engine_temp").alias("avg_engine_temp")
                                                    ,sum("is_overheat").alias("overheated_vehicles"))
max_avg_engine_temp = df_poormodels.agg(max(df_poormodels.avg_engine_temp)).collect()[0][0]
max_overheat = df_poormodels.agg(max(df_poormodels.overheated_vehicles)).collect()[0][0]


# In[58]:


max_avg_engine_temp


# In[57]:


max_overheat


# In[60]:


df_poormodels = df_poormodels.withColumn("riskscore",100 - (((df_poormodels.avg_engine_temp/max_avg_engine_temp)*100*0.5) + ((df_poormodels.overheated_vehicles/max_overheat)*100*0.5)))


# In[61]:


#df_poormodels = df_poormodels.filter(df_poormodels.riskscore > 5)
df_poormodels.show()

