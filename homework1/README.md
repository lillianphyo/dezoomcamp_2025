# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run

```
docker run -it python:3.12.8 /bin/bash -c "pip --version"
```

pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)

## Question 2. Understanding Docker networking and docker-compose

```
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

## Question 3. Trip Segmentation Count

```
SELECT 
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS "Up to 1 mile",
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS "Between 1 and 3 miles",
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS "Between 3 and 7 miles",
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS "Between 7 and 10 miles",
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS "Over 10 miles"
FROM 
    green_tripdata
WHERE 
    lpep_pickup_datetime >= '2019-10-01' 
    AND lpep_pickup_datetime < '2019-11-01';
```

## Question 4. Longest trip for each day

```
WITH daily_max AS (
    SELECT
        DATE(lpep_pickup_datetime) AS pickup_day,
        MAX(trip_distance) AS max_distance
    FROM green_tripdata
    WHERE lpep_pickup_datetime >= '2019-10-01'
      AND lpep_pickup_datetime < '2019-11-01'
    GROUP BY DATE(lpep_pickup_datetime)
)
SELECT pickup_day
FROM daily_max
ORDER BY max_distance DESC
LIMIT 1;
```

## Question 5. Three biggest pickup zones

```
SELECT
    z."Zone" AS pickup_zone,
    SUM(g.total_amount) AS total_amount_sum
FROM
    green_tripdata g
JOIN
    zones z ON g."PULocationID" = z."LocationID"
WHERE
    g.lpep_pickup_datetime >= '2019-10-18' AND g.lpep_pickup_datetime < '2019-10-19'
GROUP BY
    z."Zone"
HAVING
    SUM(g.total_amount) > 13000
ORDER BY
    total_amount_sum DESC
LIMIT 3;
```

## Question 6. Largest tip

```
SELECT
    z_dropoff."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS max_tip
FROM
    green_tripdata g
JOIN
    zones z_pickup ON g."PULocationID" = z_pickup."LocationID"
JOIN
    zones z_dropoff ON g."DOLocationID" = z_dropoff."LocationID"
WHERE
    z_pickup."Zone" = 'East Harlem North' AND
    g.lpep_pickup_datetime >= '2019-10-01' AND g.lpep_pickup_datetime < '2019-11-01'
GROUP BY
    z_dropoff."Zone"
ORDER BY
    max_tip DESC
LIMIT 1;
```
