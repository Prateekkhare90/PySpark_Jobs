import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
import prepare_data as dp
import feature_engineering as fe

# featured_df = fe.get_features(dp.input_data())

def get_scores(DataFrame):
    featured_df = DataFrame
    segmentation_cond = (If then else statements)
    scored_df = featured_df.withColumn("SEGMENT_16SEG" , expr(segmentation_cond))
    
    return scored_df