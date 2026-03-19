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
    
    def numColVal(self, column:str, upper_bound = None, lower_bound = None):
        #check that column exists in df
        if column not in self.df.columns:
            print("Please enter a valid column name as a string.")
            return
        
        #check that column is numeric
        data_type_list = ["int", "integer", "bigint", "longint", "decimal", "float", "double"]
        if dict(self.df.dtypes)[column] not in data_type_list:
            print("Please enter a column with numeric data.")
            return self.df
        
        #check that upper and/or lower bound has been entered
        if (lower_bound == None) & (upper_bound == None):
            print("Please enter a lower and/or upper bound.")
            return self.df
        
        #compute new column for lower_bound only
        elif upper_bound == None:
            self.df = self.df.withColumn(column + " >= " + str(lower_bound),
                                         F.when(self.df[column] >= lower_bound, "true") \
                                         .when(self.df[column] < lower_bound, "false"))
                           
        #compute new column for upper_bound only
        elif lower_bound == None:
            self.df = self.df.withColumn(column + " <= " + str(upper_bound), 
                                         F.when(self.df[column] <= upper_bound, "true") \
                                         .when(self.df[column] > upper_bound, "false"))
           
        else:
            self.df = self.df.select("*", self.df[column] \
                                                        .between(lower_bound, upper_bound))
        
        return self.df
        
        
    