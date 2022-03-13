from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import avg, when, col, date_format, month


## Start Spark Session
spark = SparkSession.builder \
    .master("local") \
    .appName("CountryWeather") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Creating the dataframes (similar to the database tables)

# Create the countries dataframe 
countries = [ (2, 'USA'),
    (3, 'Australia'),
    (7, 'Peru'),
    (5, 'China'),
    (8, 'Morocco'),
    (9, 'Spain')
]

countries_schema = StructType([ \
    StructField("country_id", IntegerType(),True), \
    StructField("country_name", StringType(),True), \
  ])
 
countries_df = spark.createDataFrame(data = countries, schema = countries_schema )

# Create the weather dataframe 
weather = [(2, 15, datetime(2019, 11, 1)),
(2, 12, datetime(2019, 10, 28)),
(2, 12, datetime(2019, 10, 27)),
(3, -2, datetime(2019, 11, 10)),
(3, 0, datetime(2019, 11, 1)),
(3, 3, datetime(2019, 11, 12)),
(5, 16, datetime(2019, 11, 7)),
(5, 18, datetime(2019, 11, 9)),
(5, 21, datetime(2019, 11, 23)),
(7, 25, datetime(2019, 11, 28)),
(7, 22, datetime(2019, 12, 1)),
(7, 20, datetime(2019, 12, 2)),
(8, 25, datetime(2019, 11, 5)),
(8, 27, datetime(2019, 11, 15)),
(8, 31, datetime(2019, 11, 25)),
(9, 7, datetime(2019, 11, 23)),
(9, 3, datetime(2019, 12, 23)) 
]

weather_schema = StructType([ \
    StructField("country_id", IntegerType(),True), \
    StructField("weather_state", IntegerType(),True), \
    StructField("day", DateType(),True), \
  ])
 
weather_df = spark.createDataFrame(data = weather, schema = weather_schema )

#Debugging
# countries_df.show()
# countries_df.printSchema()
# weather_df.show()
# weather_df.printSchema()

# Solution - QUESTION 1
# +++++++++++++++++++++++

# Compute a temp DV of Average, group by country id
# Create lookup for the cases and create a DV weather_type
#       Cold : average <= 15, 
#       Hot: average >= 25 
#       Warm: otherwise.
# Join country dataframe on country_id

weather_type_df = weather_df.groupBy('country_id').agg(avg('weather_state').alias('average'))\
    .withColumn('weather_type', when(col('average') <= 15, 'Cold')
    .when(col('average') >= 25, 'Hot')
    .otherwise('Warm')
    )\
    .join(countries_df, ['country_id'], 'left')\
    .select('country_name', 'weather_type')


# Solution - QUESTION 2
# +++++++++++++++++++++++
# The solution expands the solution above by creating a DV month_of_the_year and group
# Asusumption:
#       The year does not matter, Each month is assumed to have the same weather

weather_type_df2 = weather_df.withColumn('month_of_the_year', date_format('day', 'MMMM'))\
    .groupBy(['country_id', 'month_of_the_year']).agg(avg('weather_state').alias('average'))\
    .withColumn('weather_type', when(col('average') <= 15, 'Cold')
    .when(col('average') >= 25, 'Hot')
    .otherwise('Warm')
    )\
    .join(countries_df, ['country_id'], 'left')\
    .select('country_name', 'weather_type', 'month_of_the_year')\
    #.orderBy('country_name')
