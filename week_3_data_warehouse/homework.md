<!-- yellow

CREATE OR REPLACE EXTERNAL TABLE `de-zoom-camp-339323.trips_data_all.yellow_trip_data`
OPTIONS(
    format = 'parquet',
    uris=[
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/yellow_tripdata/2019/*',
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/yellow_tripdata/2020/*',
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/yellow_tripdata/2021/*'
    ]
)
 -->

 <!-- 
 green 

 CREATE OR REPLACE EXTERNAL TABLE `de-zoom-camp-339323.trips_data_all.green_trip_data`
OPTIONS(
    format = 'parquet',
    uris=[
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/green_tripdata/2019/*',
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/green_tripdata/2020/*',
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/green_tripdata/2021/*'
    ]
)
  -->


<!-- 
fhv

CREATE OR REPLACE EXTERNAL TABLE `de-zoom-camp-339323.trips_data_all.fhv_trip_data`
OPTIONS(
    format = 'parquet',
    uris=[
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/fhv_tripdata/2019/*'
    ]
)
 -->

 <!-- 
 zones

 CREATE OR REPLACE EXTERNAL TABLE `de-zoom-camp-339323.trips_data_all.taxi_zone_lookup`
OPTIONS(
    format = 'parquet',
    uris=[
        'gs://dtc_data_lake_de-zoom-camp-339323/raw/taxi_zone/taxi_zone_lookup.parquet'
    ]
)
  -->

## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)

SELECT count(*)
FROM `de-zoom-camp-339323.trips_data_all.fhv_trip_data` 

ans = 42084899

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

SELECT count(distinct dispatching_base_num)
FROM `de-zoom-camp-339323.trips_data_all.fhv_trip_data` 

ans = 792


### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

Ans= Partition the table 'dropoff_datetime' and cluster by dispatching_base_num

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

**Create a partitioned table from external table
CREATE OR REPLACE TABLE `de-zoom-camp-339323.trips_data_all.fhv_trip_data_partitoned_clustered`
PARTITION BY DATE (pickup_datetime)
  CLUSTER  BY dispatching_base_num AS
SELECT * FROM `de-zoom-camp-339323.trips_data_all.fhv_trip_data`;

SELECT count(*) 
FROM `de-zoom-camp-339323.trips_data_all.fhv_trip_data_partitoned_clustered` 
WHERE DATE(pickup_datetime) between "2019-01-01" and "2019-03-31"
and dispatching_base_num in ('B00987', 'B02060', 'B02279')
LIMIT 10

 estimated = 400.1 MB
 actual    =164.2 MB

 count = 26647

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag** 
Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

Ans = Clustering

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

Ans = The query improvements are not very noticeable when partitioning and clustering Tables with DataSize less than 1GB
and it might also increase costs as the partitions have to be stored seperately which adds to storage costs

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

Ans = columnar format known as Capacitor