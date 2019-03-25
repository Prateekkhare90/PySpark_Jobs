import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def input_data():
    data_prep_schema = StructType([StructField("VIN_ID",StringType(),True)\
                    ,StructField("OWNERSHIP_CYCLE_NUM",LongType(),True)\
                    ,StructField("CONSUMER_ID",LongType(),True)\
                    ,StructField("COUNTRY_CODE",StringType(),True)\
                    ,StructField("VEHICLE_MAKE",StringType(),True)\
                    ,StructField("VEHICLE_MODEL_YEAR",StringType(),True)\
                    ,StructField("ACQUISITION_DATE",DateType(),True)\
                    ,StructField("DISPOSAL_DATE",DateType(),True)\
                    ,StructField("WARRANTY_START_DATE",DateType(),True)\
                    ,StructField("DATE",DateType(),True)\
                    ,StructField("ESP_IND",LongType(),True)\
                    ,StructField("OAR_ACTIVE_IND",LongType(),True)\
                    ,StructField("RO_COUNT_365",LongType(),True)\
                    ,StructField("RO_COUNT_365_729",LongType(),True)\
                    ,StructField("RO_COUNT_RC05_365",LongType(),True)\
                    ,StructField("RO_MONTHS_SINCE_LAST_REPAIR",LongType(),True)])
    
#     with open("data_prep.hql", "r") as query:
#         hivequery = query.read()
    
#     data_pull_query = hivequery
#     df = spark.sql(data_pull_query)          

    spark = (SparkSession
                .builder
                .appName('main')
                .enableHiveSupport()
                .getOrCreate())
    #pd_df = pd.read_excel("Service_segmentation_data_set.xlsx", infer_datetime_format= True)
    
    df= spark.sql("select Vin_id ,"
          +"ownership_cycle_num , "
          +"Consumer_id ,"    
          +"Country_code ,"
          +"Vehicle_make ,"
          +"Vehicle_model_year ,"
          +"Acquisition_date ,"
          +"Disposal_date ,"
          +"Warranty_start_date ,"
          +"Look_at_date ,"
          +"Esp_ind ,"
          +"Oar_active_ind ,"
          +"count_365 ,"
          +"count_365_729 ,"
          +"count_rc05_365 ,"
          +"months_since_last_repair " 
          +"from Database1.table1 " )
   
    df = df[(df['COUNTRY_CODE']== "USA" ) & (df['ACQUISITION_DATE'] < df['DATE'])]
    
    df = df.dropDuplicates(['CONS_ID']).sort('ID','OWNERSHIP_CYCLE_NUM')

    return df