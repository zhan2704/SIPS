# HELPER FUNCTIONS FOR TRANSFERRING DATASETS FROM S3 TO POSTGRESQL

from pyspark.sql import SparkSession 
import sys
#from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Transferring S3 Tables to PostgreSQL") \
    .master("spark://10.0.0.9:7077") \
    .config('spark.executor.memory', '20g') \
    .config('spark.network.timeout', '300s') \
    .config('spark.executor.heartbeatInterval', '30s') \
    .getOrCreate()

# function to read a table from s3 
def readTable(source, numPartitions):
    table0 = spark.read \
        .format("csv") \
        .option("header", 'True')\
        .option("inferschema",'True') \
        .option("lowerBound", 0).option("upperBound", 320000000) \
        .option("numPartitions", numPartitions) \
        .load(source)  \
        .cache()
    return table0

# function to pre-process parking data frames in spark 
def transformTable(dfName):
    # drop extra columns and rename selected columns
    df = dfName
    drop_name = ['PaidParkingArea', 'PaidParkingSubArea',\
        'ParkingTimeLimitCategory', 'PaidParkingRate', 'ParkingCategory']
    mapping = {'SourceElementKey':'ID', 'OccupancyDateTime': 'DateTime'}
    new_names = [mapping.get(col,col) for col in df.columns]
    df = df.toDF(*new_names).drop(*drop_name).na.drop(subset=['DateTime'])

    #cast schema
    integer_type = ['PaidOccupancy', 'ID', 'ParkingSpaceCount']
    double_type = ['Latitude', 'Longitude']
    for c in integer_type:
        df = df.withColumn(c, df[c].cast("INT"))
    for c in double_type:
        df = df.withColumn(c, df[c].cast('DOUBLE'))
    df = df.withColumn('DateTime', unix_timestamp(df.DateTime, 'MM/dd/yy HH:mm')\
        .cast('timestamp'))
    df.printSchema()

    # seperate the date and time columns and calc the occupancy rate
    hourly_info = ['ID', 'Latitude', 'Longitude', 'PaidOccupancy', 'ParkingSpaceCount', \
        'BlockfaceName', 'SideOfStreet']
    hourly = df.select(date_trunc("hour", df.DateTime).alias('DateTime'), *hourly_info)
    hourly = hourly.withColumn("Date", to_date(hourly.DateTime))\
        .withColumn("Time", hour(hourly.DateTime)).drop("DateTime").dropna()\
        .withColumn("OccupancyRate", round(hourly['PaidOccupancy']/hourly['ParkingSpaceCount'], 4))
 
    # groupby hourly occupancy rate with hourly-avg numbers
    hourly_df = hourly.groupBy("Date", "Time", "ID")\
        .agg(round(avg("OccupancyRate"), 4).alias('OccupancyRate'),\
            first('Latitude').alias('Latitude'),\
            first('Longitude').alias('Longitude'),\
            first('BlockfaceName').alias('BlockfaceName'),\
            first('SideOfStreet').alias('SideOfStreet'))
    
    hourly_df.show(20)
    print(hourly_df.count())
    
    return hourly_df

# function to transform collision data
def collisionTable(dfName):
    df = dfName

    # drop extra columns and rename selected columns
    drop_name = ['OBJECTID', 'INCKEY', 'COLDETKEY', 'REPORTNO',\
        'STATUS', 'INTKEY', 'EXCEPTRSNCODE', 'EXCEPTRSNDESC', \
        'INATTENTIONIND', 'SDOT_COLCODE', 'ST_COLDESC', 'INCDTTM',\
        'UNDERINFL', 'PEDROWNOTGRNT','SDOTCOLNUM','SPEEDING',\
        'SEGLANEKEY','CROSSWALKKEY','PEDCOUNT','PEDCYLCOUNT',\
        'JUNCTIONTYPE','ST_COLCODE', 'SEVERITYCODE']
    mapping = {'SEVERITYDESC':'Severity','COLLISIONTYPE':'CollisionType',\
        'INCDATE': 'DateTime', 'ADDRTYPE':'AddressType', 'LOCATION': 'Location',\
        'PERSONCOUNT':'PersonCount', 'VEHCOUNT':'VehCount',\
        'INJURIES':'Injuries', 'SERIOUSINJURIES': 'SeriousInjur',\
        'FATALITIES': 'Fatal', 'SDOT_COLDESC':'CollisionDesc',
        'X':'Longitude', 'Y':'Latitude', 'WEATHER': 'Weather',
        'ROADCOND':'RoadCondition', 'LIGHTCOND':'LightCondition',\
        'HITPARKEDCAR':'HitParkedCar'}
    new_names = [mapping.get(col,col) for col in df.columns]
    df = df.toDF(*new_names).drop(*drop_name).na.drop(subset=['Latitude','Longitude'])

    #uniform the lat and long format with the ones in the parking data frame
    df = df.withColumn('Longitude', round(df['Longitude'],8))\
        .withColumn('Latitude', round(df['Latitude'],8))\
        .withColumn("Date", to_date(df.DateTime))\
        .withColumn("Time", hour(df.DateTime)).drop("DateTime")

    # select collision records in the same scope with parking occupancy data
    df = df.filter(year(df.Date).isin([2017, 2018, 2019]))

    df.printSchema()
    df.show(10)
    return df

# function to write a table to postgresql    
def writeTable(df, dfName, saveMode="error"):
    cluster   = 'jdbc:postgresql://10.0.0.9:26257/test'    
    df.write\
    .format("jdbc")\
    .option("driver", "org.postgresql.Driver")\
    .option("url", cluster)\
    .option("dbtable", dfName)\
    .option("user", "anqi")\
    .option("password", "1234")\
    .save(mode=saveMode)
