# Databricks notebook source
# DBTITLE 1,Mounting Blob Storage
dbutils.fs.mount(
source ="wasbs://animalcrossingraw@animalcrossingraw.blob.core.windows.net",
mount_point ="/mnt/animalcrossingraw",
extra_configs={"fs.azure.account.key.animalcrossingraw.blob.core.windows.net":"O0+4BBTJXspM0QX7NKzQTKWUDHA47GzC/s318/NrBI4bhYu5/Maec/n/oC3Blxb1GyJ/k7aXZKVZ+AStlsMxsQ=="} 
)

# COMMAND ----------

# MAGIC %fs ls /mnt/animalcrossingraw

# COMMAND ----------

# DBTITLE 1,Dataframe
df=spark.read.format('csv').option("header","True").option("inferSchema", "True").load('dbfs:/mnt/animalcrossingraw/Copy of Data Spreadsheet for Animal Crossing New Horizons.xlsx.txt')
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# DBTITLE 1,Dropping Columns
df=df.drop('Closet Image','Storage Image')

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns


# COMMAND ----------

df=df.drop('DIY' 'Color 1','Color 2','Size','Exchange Price','Exchange Currency', 'Source Notes','Season/Event','Season/Event Exclusive','Seasonal Availability', 'Mannequin Season','Gender','Villager Gender','Style 1','Style 2','Sort Order','Label Themes''Villager Equippable','Catalog','Version Added','Unlocked?','Filename','ClothGroup ID')

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns


# COMMAND ----------

df=df.drop('DIY','HHA Base Points', 'Color 1', 'Label Themes', 'Villager Equippable')

# COMMAND ----------

display(df)

# COMMAND ----------

df.write

# COMMAND ----------

df.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("/mnt/animalcrossingraw/Data.csv")

# COMMAND ----------


