# RUN THIS .PY FILE TO TRANSFER DATA FROM S3 TO POSTGRESQL
# FILES:
# - PARKING OCCUPANCY FILES
# - COLLISIONS FILE
# DATASETS ARE CLEANED BEFORE SENDING TO POSTGRESQL

from trans_helper import *

# 20 cores * 3 partitions per core + sauce
numPartitions = 65 

parking17 = "s3a://anqi-zhang-parking-first-backup-bucket/2017_parking.csv"
parking18 = "s3a://anqi-zhang-parking-first-backup-bucket/2018_parking.csv"
parking19 = "s3a://anqi-zhang-parking-first-backup-bucket/2019_parking.csv"
collision = "s3a://anqi-zhang-parking-first-backup-bucket/Collisions.csv"

source = [parking17, parking18, parking19]

for table in range(len(source)):
	tabNow = source[table]
	pre = readTable(tabNow, numPartitions)
	print("Read in table " + tabNow + " successfully!")

	parking = transformTable(pre)
	print("Transformed table " + tabNow + " successfully!")

	writeTable(parking, "parking", saveMode="append")
	print("Wrote table " + tabNow + " to postgreSQL successfully!")

	spark.catalog.clearCache()

pre_collision = readTable(collision, numPartitions)
print("Read in collision table successfully!")

collisions = collisionTable(pre_collision)
print("Transformed collision table successfully!")

writeTable(collisions, "collisions", saveMode="overwrite")
spark.catalog.clearCache()

