## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL

## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

--solution

Google Cloud SDK 369.0.0
bq 2.0.72
core 2022.01.14
gsutil 5.6

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form

## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15?

53024

select 
cast(tpep_pickup_datetime as date) as date,
count(*) countt
FROM public.yellow_taxi_trips
where cast(tpep_pickup_datetime as date) = '2021-01-15'
group by cast(tpep_pickup_datetime as date)
order by cast(tpep_pickup_datetime as date);

## Question 4. Average

Find the largest tip for each day. 

select date, tip_amount
from (
select 
cast(tpep_pickup_datetime as date) as date,
tip_amount,
rank() over (partition by cast(tpep_pickup_datetime as date) order by tip_amount desc) as rnk
FROM public.yellow_taxi_trips
where tip_amount > 0
order by cast(tpep_pickup_datetime as date)  asc, tip_amount desc
)a
where rnk = 1


On which day it was the largest tip in January?

"2021-01-20"

select date
from 
(
select date, tip_amount,year_month,
rank() over (order by tip_amount desc) as rnk_2
from (
select 
cast(tpep_pickup_datetime as date) as date, to_char(tpep_pickup_datetime, 'YYYYMM')year_month,
tip_amount,
rank() over (partition by cast(tpep_pickup_datetime as date) order by tip_amount desc) as rnk
FROM public.yellow_taxi_trips
where tip_amount > 0
and to_char(tpep_pickup_datetime, 'YYYYMM') = '202101'
order by cast(tpep_pickup_datetime as date)  asc, tip_amount desc	
)a
where rnk = '1'
	)b
where rnk_2 = 1


(note: it's not a typo, it's "tip", not "trip")

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

select 
cast(tpep_pickup_datetime as date) as date,
a."PULocationID", 
p."Zone" as pick_up_zone,
a."DOLocationID",
d."Zone" as drop_off_zone, 
count(*) countt
FROM public.yellow_taxi_trips a
left join public.zones p
on a."PULocationID" =  p."LocationID"
left join public.zones d
on a."DOLocationID" = d."LocationID"
where cast(tpep_pickup_datetime as date) = '2021-01-14'
and a."PULocationID" = '43'
group by 
cast(tpep_pickup_datetime as date),
a."PULocationID", 
p."Zone" ,
a."DOLocationID",
d."Zone" 
order by countt desc

Enter the zone name (not id)
Upper East Side South

## Question 6. 

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

select 
p."Zone" ||' / '||d."Zone" as pick_up_drop_off_zone, 
avg(total_amount) avg_total_amount
FROM public.yellow_taxi_trips a
inner join public.zones p
on a."PULocationID" =  p."LocationID"
inner join public.zones d
on a."DOLocationID" = d."LocationID"
group by 
p."Zone" ||' / '||d."Zone"
order by avg_total_amount desc

"Jamaica Bay / Clinton East"

Union Sq / Canarsie
## Submitting the solutions

Form for sumitting (TBA)

Deadline: 24 January, 17:00 CET