import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#This is the data preparation python file
import prepare_data as dp

#This is the feature engineering python file
import feature_engineering as fe

#This is the scoring python file
import scoring as sr

spark = (SparkSession
                .builder
                .appName('main')
                .enableHiveSupport()
                .getOrCreate())
def wrapper():
    print("Modelling started.....!!!!")
    df1 = dp.input_data()
    print("Data prep is done")
    df2  = fe.get_features(df1)
    print("Feature engineering is done")
    df3 = sr.get_scores(df2)
    print("Scoring is done")    
    df3.coalesce(1).write.csv("Scored_Output.csv",header = 'true')
    print("Modelling completed..!!!!")
    

wrapper()
    
    