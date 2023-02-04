## 1. Knowing docker tags:
```console
docker build --help
```
## 2. Understanding docker first run:
```console
docker run -d python:3.9
pip list
```
## 3.  Count records:

### 1. Run container:
```console
docker run -d \
	--name leo \
	-e POSTGRES_PASSWORD=Sw@tCH_1993 \
	-e PGDATA=/var/lib/postgresql/data/pgdata \
	-v /Users/{user}/Desktop/work/data:/var/lib/postgresql/data \
	postgres
### 2. Create tables 'taxi' and 'tripdata' with correct types
```
```sql
CREATE TABLE taxi(
LocationID serial,
Borough VARCHAR(50),
Zone VARCHAR(50),
service_zone VARCHAR(50)
);

CREATE TABLE tripdata(
VendorID Serial,
lpep_pickup_datetime TIMESTAMP,
lpep_dropoff_datetime TIMESTAMP,
store_and_fwd_flag VARCHAR(50),
RatecodeID Integer,
PULocationID Integer,
DOLocationID Integer,
passenger_count Integer,
trip_distance float,
fare_amount float,
extra float,
mta_tax float,
tip_amount float,
tolls_amount float,
ehail_fee float null,
improvement_surcharge float,
total_amount float,
payment_type float,
trip_type float,
congestion_surcharge float null);
```

### 2. Export data from files:
```sql
COPY taxi FROM 'taxi+_zone_lookup.csv' ( FORMAT CSV );
```
Second file unzip and then COPY too.

### 3. Execute query
```sql
SELECT COUNT(*) 
FROM tripdata
WHERE lpep_pickup_datetime='2019-01-15' AND lpep_dropoff_datetime='2019-01-15' 
```
## 4. Largest trip for each day
```sql
SELECT lpep_pickup_datetime, sum(tip_amount)
FROM tripdata
GROUP BY lpep_pickup_datetime
ORDER BY sum(tip_amount) DESC
LIMIT 1;
```
## 5. The number of passengers
```sql
SELECT passenger_count, COUNT(*) AS total_trips
FROM tripdata
WHERE passenger_count IN (2,3) AND lpep_pickup_datetime = '2019-01-01'
GROUP BY passenger_count;
```

## 6. Largest tip
```sql
SELECT t.Zone, SUM(td.tip_amount) AS total_tips
FROM tripdata td LEFT JOIN taxi t 
ON td.PULocationID = t.LocationID
WHERE td.Zone = 'Astoria'
GROUP BY td.PULocationID
ORDER BY total_tips DESC
LIMIT 1;
```
Then check id of result (sorry, no so much time to great answer)
