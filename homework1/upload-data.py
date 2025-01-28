#!/usr/bin/env python
# coding: utf-8

# In[98]:


import pandas as pd 
import pyarrow.parquet as pq
from time import time


# In[99]:


df=pd.read_csv('./data/green_tripdata_2019-10.csv', nrows=100)


# In[100]:


df.head()


# In[101]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[102]:


from sqlalchemy import create_engine


# In[103]:


engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')


# In[104]:


print(pd.io.sql.get_schema(df, name='green_tripdata', con=engine))


# In[116]:


df_iter = pd.read_csv('./data/green_tripdata_2019-10.csv', iterator=True, chunksize=100000, low_memory=False)


# In[117]:


df = next(df_iter)


# In[118]:


len(df)


# In[119]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[120]:


df


# In[121]:


df.head(n=0).to_sql(name='green_tripdata', con=engine, if_exists='replace')


# In[122]:


get_ipython().run_line_magic('time', "df.to_sql(name='green_tripdata', con=engine, if_exists='append')")


# In[123]:


from time import time


# In[125]:


while True:
    try:
        t_start = time()

        # Get the next chunk of data
        df = next(df_iter)

        # Convert datetime columns
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # Insert the chunk into the database
        df.to_sql(name='green_tripdata', con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))

    except StopIteration:
        # No more chunks to process
        print("Finished processing all chunks.")
        break

    except Exception as e:
        # Handle any other exceptions
        print(f"An error occurred: {e}")
        break


# In[46]:


df_zones = pd.read_csv('./data/taxi_zone_lookup.csv')


# In[126]:


df_zones.head()


# In[127]:


df_zones.to_sql(name='zones', con=engine, if_exists='replace')


# In[ ]:




