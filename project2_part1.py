#Lanette Tyler
#ST554 Project 2, Part 1

#read in modules and functions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    df = None
    
    def __init__(self, df: DataFrame):
        self.df = df
        
    @classmethod
    def from_csv(cls, spark_session, filepath: str):
        df = spark_session.read.load(filepath, 
                             format = "csv", 
                             sep = ",", 
                             header = "true", 
                             inferSchema = "true")
        return cls(df)
        
    @classmethod
    def from_pandas_df(cls, spark_session, pandas_df: pd.DataFrame):
        df = spark_session.createDataFrame(pandas_df)
        return cls(df)
        
        
    