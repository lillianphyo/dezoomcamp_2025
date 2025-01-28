import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')
query = """
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
"""

# Execute the query
result = pd.read_sql(query, engine)

# Display the result
print(result)
