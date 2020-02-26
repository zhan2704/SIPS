# SIPS
Safety Information near Parking Surroundings

![image](https://user-images.githubusercontent.com/57073578/75394690-8b693f00-58a5-11ea-807f-141ad80b819f.png)

When finding parkings, the least thing a car owner want is his or her car gets hitted or scraped. However, more than half of car collisions are actually associated with parked cars. The SIPS platform aims at to provide the historical information about collision events and parking occupancy rates near a selected destination from users, so that the user can be aware of how's the parking surrounding situation look like. 

## Pipeline

![image](https://user-images.githubusercontent.com/57073578/74552670-923d9c80-4f0a-11ea-94a1-4f349b360265.png)

## Challenges

### Challenge 1: How to transfer the data

![image](https://user-images.githubusercontent.com/57073578/75393914-f0bc3080-58a3-11ea-9a08-d755bdbe1b8f.png)

Due to the large amount of data from separate tables from 2017 to 2019, it will take extremely long time to transfer data directly from S3 to PostgreSQL. Thus, Spark is being used to complete the data transferring process.

Some more reasons for choosing Spark in this project:

![image](https://user-images.githubusercontent.com/57073578/75394168-6cb67880-58a4-11ea-8538-e7fc02487f2f.png)

### Challenge 2: Different schema 

The schema of the geo-location data in the parking datasets are different. Thus, a pre-processing function has been defined in the transfer_helper.py to unify the information before further processing the data.

### Challenge 3: Tradeoff between saving storage and computation speed

![image](https://user-images.githubusercontent.com/57073578/75394892-02063c80-58a6-11ea-879e-c3fb4a9ccec3.png)

After testing out several different techniques, groupby was selected to conduct the pre-processing work considering the storage it needed for store the information and the speed it can achieve the computation.

## Dataset

* Seattle 2019 Paid Parking Occupancy (Year-to-date): https://data.seattle.gov/Transportation/2019-Paid-Parking-Occupancy-Year-to-date-/qktt-2bsy
* Seattle 2018 Paid Parking Occupancy (Year-to-date): https://data.seattle.gov/Transportation/2018-Paid-Parking-Occupancy-Year-to-date-/6yaw-2m8q
* Paid Parking Occupancy 2017 (Archived): https://data.seattle.gov/Transportation/Paid-Parking-Occupancy-2017-Archived-/t96b-taja
* Seattle Collision Data: http://data-seattlecitygis.opendata.arcgis.com/datasets/collisions/data

## Caveat

Make sure spark env is set up with **Java 8** for compatibility.

