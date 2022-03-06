## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

What's the output?

- command = spark.version
- Ans = '3.0.3'


## Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv
```

Read it with Spark using the same schema as we did 
in the lessons. 

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

- command = du -sh fhvhv/2021/02
- Ans = 220MB    ./data/pq/fhvhv/2021

## Question 3. Count records 

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

- code 
```
spark.sql("""
SELECT 
    service_type,
    pickup_date,
    count(1) as count
FROM 
    fhvhv_trips_data
WHERE 
    pickup_date = '2021-02-15'
GROUP BY 
    service_type, pickup_date
""").show()
```

- ans = 367170


## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest? 

- code 

```
spark.sql("""
SELECT 
     service_type
    ,hvfhs_license_num
    ,dispatching_base_num
    ,pickup_date
   -- ,pickup_datetime
    ,dropoff_date
    --,dropoff_datetime
    --,PULocationID
    --,DOLocationID
    --,SR_Flag
    ,round(((bigint(dropoff_datetime))-(bigint(pickup_datetime)))/(60),2) trip_duration_min_sec
FROM 
    fhvhv_trips_data
WHERE 
    pickup_date between '2021-02-01' AND '2021-02-28'
ORDER BY 
    trip_duration_min_sec DESC
LIMIT 
    1
""").show()
```

- Ans = 2021-02-11


## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?

> Note: the answer may depend on how you write the query,
> so there are multiple correct answers. 
> Select the one you have.

- code 
```
spark.sql("""
SELECT 
     service_type
    ,dispatching_base_num
    ,count(1) count
FROM 
    fhvhv_trips_data
WHERE 
    pickup_date between '2021-02-01' AND '2021-02-28'
GROUP BY 
    service_type, dispatching_base_num
ORDER BY 
    count DESC
LIMIT 
    1
""").show()
```

- ans = [Stage 38:=============================================>            (7 + 2) / 9]
- ans = B02510


## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 

- code 
```
wk_5_q6_ans = spark.sql("""
--select * from (
SELECT 
     service_type
    ,NVL(b.Zone,'Unknown') ||' / ' || NVL(c.Zone,'Unknown') as pickup_dropoff_pair
    ,count(1)count
FROM 
    fhvhv_trips_data a 
    INNER JOIN   zones b
    ON a.PULocationID = b.LocationID
    INNER JOIN   zones c
    ON a.DOLocationID = c.LocationID
WHERE 
    pickup_date between '2021-02-01' AND '2021-02-28'
   -- and (b.LocationID is null or c.LocationID is null)
GROUP BY 
    service_type
    ,NVL(b.Zone,'Unknown') ||' / ' || NVL(c.Zone,'Unknown')
ORDER BY 
    count DESC
LIMIT
    100
--)
-- LOWER(pickup_dropoff_pair) like '%unknown%'
""")
# .show()
```
- ans = East New York / East New York	45041

## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it?
- ans = INNER JOIN (TWICE)

And how many stages your spark job has?
- ans = [Stage 125:============================================>            (7 + 2) / 9]


## Submitting the solutions

* Form for submitting: TBA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 02 March (Wednesday), 22:00 CET
