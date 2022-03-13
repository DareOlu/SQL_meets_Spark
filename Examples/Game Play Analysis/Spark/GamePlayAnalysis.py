from pyspark.sql import SparkSession, Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql.functions import first, col, min, sum, when, datediff, count, round


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
activity_df1 = activity_df.groupBy('player_id')\
  .agg(min('event_date').alias('first_login'))\
  .orderBy('player_id')


# Challenge 2
activity_df2 = activity_df.withColumn('first_login', min('event_date').over(Window.partitionBy('player_id')))\
  .filter(col('event_date') == col('first_login'))\
  .select('player_id', 'device_id')


# Challenge 3
windowspec = (Window.partitionBy('player_id').orderBy('event_date').rangeBetween(Window.unboundedPreceding, 0))
activity_df3 = activity_df.withColumn('games_played_so_far', sum('games_played').over(windowspec))\
  .select('player_id', 'device_id', 'games_played_so_far')

