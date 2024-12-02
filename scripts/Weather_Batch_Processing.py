#!/usr/bin/env python
# coding: utf-8

# ## Weather Data Batch Processing
# 
# New notebook

# ### Weather Details(JSON) Batch processing using **Py-Spark**

# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql import *

src_df=spark.read.\
option('multiline','true').\
format('json').\
load("Files/WeatherDetails_Stream/*.json")


# In[ ]:


#read Country Details Table
country_df = spark.sql("SELECT * FROM LHDEV.countrydetails")
Joined_df=src_df.join(country_df,col('location.country')==col('Country'),'inner')\
                .filter(col('location.region')!='Anhui')\
                .filter(col('location.region')!='Ghazni')\
                .withColumn('WeatherID',col('current.weather_code').cast('int'))\
                .withColumn('AtmosphereID',concat(col('CountryCode'),substring(col('location.country'),-3,3)))
                



# In[ ]:


#ID Details -- FactTable
#SCD Type - 1
fact_df=Joined_df.withColumn('Load_TS',current_timestamp()).select(['CountryCode','WeatherID','AtmosphereID','Load_TS'])


# In[ ]:


#countryDetails --DimentionTable
#SCD Type - 1
country_df_final=Joined_df.withColumn('City',split(col('request.query'),',').getItem(0)).\
                            withColumn('Latitude',col('location.lat').cast('decimal(8,6)')).\
                            withColumn('Longitude',col('location.lon').cast('decimal(9,6)')).\
                            select(['CountryCode','City','Country','Region','Latitude','Longitude']).distinct().dropDuplicates(['CountryCode'])


# In[ ]:


#AtmosphereDetails --DimentionTable
#SCD Type - 2
atmosphere_df=Joined_df.withColumn('CloundCover',concat(col('current.cloudcover').cast('int'),lit('%')))\
                        .withColumn('Humidity',concat(col('current.humidity').cast('int'),lit('%')))\
                        .withColumn('is_day',col('current.is_day'))\
                        .withColumn('temperature',concat(col('current.temperature').cast('int'),lit('°C')))\
                        .withColumn('wind_degree',concat(col('current.wind_degree').cast('int'),lit('%')))\
                        .withColumn('wind_direction',col('current.wind_dir'))\
                        .withColumn('wind_speed',concat(col('current.wind_speed').cast('int'),lit(' Km/hr')))\
                        .withColumn('observation_time',to_timestamp((concat(substring(col('location.localtime'),1,10),col('current.observation_time'))),'yyyy-MM-ddhh:mm a'))\
                        .withColumn('is_latest',lit(1))\
                        .select(['AtmosphereID','observation_time','temperature','Humidity','CloundCover','wind_direction','wind_degree','wind_speed','is_day','is_latest','WeatherID'])


# In[ ]:


#WeatherDetails --DimentionTable
#SCD Type - 1
weather_df=Joined_df.withColumn('weather_descriptions',col('current.weather_descriptions')[0])\
                    .select(['WeatherID','weather_descriptions']).distinct().dropDuplicates(['WeatherID'])


# In[ ]:


#Creating Dict of ID and TableNames for future uses
tble_dict={'CountryCode':'CountryDetFinal','WeatherID':'WeatherDet','AtmosphereID':'AtmosphereDet'}


# In[ ]:


def process_batch(df):
    '''
    A function which takes inputs as dataframe(df) and perform the MERGE opertion
    Based on the certain conditions.
    '''
    id_col=[x for x in df.columns if x in ['CountryCode','WeatherID','AtmosphereID']]
    if len(id_col) == 3:
        df.createOrReplaceTempView("incoming_df")
        spark.sql(f'''MERGE INTO countrymstr T USING incoming_df A on T.CountryCode=A.CountryCode
                    WHEN MATCHED THEN UPDATE SET
                    T.WeatherID=A.WeatherID,
                    T.AtmosphereID=A.AtmosphereID,
                    T.Load_TS=CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                    INSERT
                    * ''')
    elif len(id_col) == 2:
        df.createOrReplaceTempView("incoming_df")
        spark.sql(f'''MERGE INTO atmospheredet T USING incoming_df A on T.is_latest=A.is_latest
                    AND T.AtmosphereID=A.AtmosphereID
                    AND T.observation_time <= A.observation_time
                    WHEN MATCHED THEN UPDATE SET T.is_latest=0
                    ''')
        spark.sql(f'''MERGE INTO atmospheredet T USING incoming_df A on T.is_latest=A.is_latest
                    AND T.AtmosphereID=A.AtmosphereID
                    AND T.observation_time <= A.observation_time
                    WHEN NOT MATCHED THEN
                    INSERT
                    * ''')
    else:    
        tble=tble_dict[id_col[0]]
        df.createOrReplaceTempView("incoming_df")
        spark.sql(f'''MERGE INTO {tble} T USING incoming_df A on T.{id_col[0]}=A.{id_col[0]}
                    WHEN NOT MATCHED THEN
                    INSERT
                    * ''')

    


# In[ ]:


#Loading CountryMaster Table
process_batch(fact_df)

#Loading Atmosphere Details Table
process_batch(atmosphere_df)

#Loading Country Details Final Table
process_batch(country_df_final)

#Loading Weather Details Table
process_batch(weather_df)

