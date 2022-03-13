from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql.functions import first, col

## Start Spark Session
spark = SparkSession.builder \
    .master("local") \
    .appName("GamePlayAnalysis") \
    .getOrCreate()

# Create the activity dataframe 
activity = [( 1, 2, datetime(2016, 3, 1), 5),
(1, 2, datetime(2016, 3, 2), 6),
( 2, 3, datetime(2017, 6, 5), 1),
( 3, 1, datetime(2016, 3, 1), 0),
( 3, 4, datetime(2018, 7, 3), 5)
]

activity_schema = StructType([ \
    StructField("player_id", IntegerType(), True), \
    StructField("device_id", IntegerType(), True), \
    StructField("event_date", DateType(), True), \
    StructField("games_played", IntegerType(),True), 
  ])

activity_df = spark.createDataFrame(data = activity, schema = activity_schema )

# Debugging
# activity_df.show()
# activity_df.printSchema()

# Challenge 1
activity_df1 = activity_df.groupBy('player_id').agg(first(col('event_date'))).orderBy('event_date').show()