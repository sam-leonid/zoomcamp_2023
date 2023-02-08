
```sql
CREATE OR REPLACE EXTERNAL TABLE `zoom-camp-hw3.hw3.fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp-hw3/fhv/fhv_tripdata_2019-*.csv.gz']
);
```

# 1. What is the count for fhv vehicle records for year 2019?

```sql
SELECT COUNT(*) FROM zoom-camp-hw3.hw3.fhv;
```
 Answer: `43244696`

# 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
 -- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoom-camp-hw3.hw3.fhv_non_partitoned AS
SELECT * FROM zoom-camp-hw3.hw3.fhv_external;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM zoom-camp-hw3.hw3.fhv_non_partitoned;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM zoom-camp-hw3.hw3.fhv_external;
```

Answer: 0 MB for the External Table and 317.94MB for the BQ Table

# 3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
SELECT COUNT(*)
FROM zoom-camp-hw3.hw3.fhv_external
WHERE PUlocationID IS NULL and DOlocationID IS NULL;
```

Answer : 717,748

# 4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE zoom-camp-hw3.hw3.fhv_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM zoom-camp-hw3.hw3.fhv_external;
```

Answer: Partition by pickup_datetime Cluster on affiliated_base_number

# 5. Implement the optimized solution you chose for question 4.  What are these values? 

```sql
SELECT DISTINCT(Affiliated_base_number)
FROM zoom-camp-hw3.hw3.fhv_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(Affiliated_base_number)
FROM zoom-camp-hw3.hw3.fhv_partitoned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

# 6. Where is the data stored in the External Table you created?

An external data source is a data source that you can query directly from BigQuery, even though the data is not stored in BigQuery storage. 

Answer: GCP Bucket

# 7. It is best practice in Big Query to always cluster your data:

Clustering of data is great when clustered column is always used for filtering, distinct, ordering. But it can be bad for inserting time in table.

Answer: False