import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
import prepare_data as dp
import pyspark.sql.functions as f

def get_features(DataFrame):
    data_df = DataFrame
    #Getting the featuring Engineering Schema
    sls_cond = """CASE 
                    WHEN COUNT_365_729 >=3 AND COUNT_365 >=3 THEN 'CONSISTENT'
                    WHEN COUNT_365_729 <1 AND  COUNT_365 <1 THEN 'NO VISIT'
                    WHEN COUNT_365 >=2 THEN 'FREQUENT'
                    ELSE 'INFREQUENT'
                  END"""

    feature_df = data_df.withColumn('SLOYALTY_STATUS',expr(sls_cond))


    vehicleage_cond = """CASE
                            WHEN OWNERSHIP_CYCLE_NUM = 1 THEN 
                            CASE 
                                WHEN abs(ceil(months_between(ACQUISITION_DATE,DATE)/12)) IS NOT NULL 
                                    THEN abs(ceil(months_between(ACQUISITION_DATE,DATE)/12))
                                ELSE abs(year(LOOK_AT_DATE)- cast(VEHICLE_MODEL_YEAR as Int) +1)
                            END
                            WHEN OWNERSHIP_CYCLE_NUM <> 1 THEN 
                            CASE 
                                WHEN abs(ceil(months_between(START_DATE,LOOK_AT_DATE)/12)) IS NOT NULL 
                                    THEN abs(ceil(months_between(START_DATE,LOOK_AT_DATE)/12))
                                ELSE abs(year(DATE)- cast(MODEL_YEAR as Int) +1)
                            END
                         END"""


    feature_df=feature_df.withColumn('AGE',expr(vehicleage_cond))


    earlyMid_cond =""" CASE
                            WHEN AGE<=3 THEN 1
                            ELSE 0
                       END"""

    feature_df = feature_df.withColumn('L_EARLY_MID', expr(earlyMid_cond))

    return feature_df