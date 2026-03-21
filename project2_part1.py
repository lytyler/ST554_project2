#Lanette Tyler
#ST554 Project 2, Part 1

#read in modules and functions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    """
    Class which takes an input of spark sql-style data frame (__init__), spark 
    session and csv file (class method .from_csv), or spark session and pandas 
    data frame (class method .from_pandas_df) to create an instance. The class objects 
    have attributes of a sql-style data frame of the inputted data (df) and pandas 
    data frames as data summaries (numSum, strSum). The class contains methods 
    .numColSummarizer and .strColSummarizer) for generating the summaries from the 
    data. There are alse methods for validating data columns (.numColVal, .strColVal) 
    which append columns of boolean values to the df attribute.
    """
    df = None
    numSum = None
    strSum = None
    
    def __init__(self, df: DataFrame):
        "create an instance of the class from a spark sql-style data frame"""
        self.df = df
        
    @classmethod
    def from_csv(cls, spark_session, filepath: str):
        """create an instance of the class from a spark session and a csv file"""
        df = spark_session.read.load(filepath, 
                             format = "csv", 
                             sep = ",", 
                             header = "true", 
                             inferSchema = "true")
        return cls(df)
        
    @classmethod
    def from_pandas_df(cls, spark_session, pandas_df: pd.DataFrame):
        """create an instance of the class from a spark session and a pandas data frame"""
        df = spark_session.createDataFrame(pandas_df)
        return cls(df)
    
    def numColVal(self, column:str, upper_bound = None, lower_bound = None):
        """validate a numeric column"""
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
    
    def strColVal(self, column: str, levels: list):
        """validate a string column"""
        #check that column exists in df
        if column not in self.df.columns:
            print("Please enter a valid column name as a string.")
            return
        
        #check that column is string data type
        if dict(self.df.dtypes)[column] != "string":
            print("Please enter a column with string data.")
            return self.df
        
        #check that values in string column correspond to user-entered values
        self.df = self.df.withColumn(column + "Check", F.col(column).isin(levels))
        
        return self.df
    
    def nullCheck(self, column: str):
        """check column for nulls"""
        self.df = self.df.withColumn(column + "IsNull", F.col(column).isNull())
        return self.df
    
    def numColSummarizer(self, column: str = None, gr_var: str = None):
        """summarize a numeric column"""
        if column != None:
            #check for valid column name
            if column not in self.df.columns:
                print("This is not a valid column name.")
                return
    
            #check for numeric data type
            data_type_list = ["int", "integer", "bigint", "longint", "decimal", "float", "double"]
            if dict(self.df.dtypes)[column] not in data_type_list:
                print("Please enter a column with numeric data.")
                return
        
        #create summary for user selected column without grouping variable
        if (column != None) & (gr_var == None):
            self.numSum = self.df.select(column).agg(F.min(column), F.max(column)).toPandas()
            #numSum = self.df.describe() \
             #   .select("summary", column) \
              #  .filter((F.col("summary") == "min") | (F.col("summary") == "max")) \
               # .toPandas()
            return self.numSum
        
        #create summary for user selected column with grouping variable
        if (column != None) & (gr_var != None):
            self.numSum = self.df.select(column, gr_var).groupBy(gr_var) \
                .agg(F.min(column), F.max(column)).toPandas()
            return self.numSum
        
        #create summary for all numeric variables (no column or grouping variable selected)
        if (column == None) & (gr_var == None):
            num_cols = [x for x, i in self.df.dtypes \
                        if (i.startswith("str") == False) \
                        & (i.startswith("bool") == False) \
                        & (i.startswith("time") == False)]
            num_cols.insert(0, "summary")
            self.numSum = self.df.describe().select(num_cols) \
                .filter((F.col("summary") == "min") | (F.col("summary") == "max")).toPandas()
            return self.numSum
        
        #create summary for all numeric variables with user-selected grouping variable
        if (column == None) & (gr_var != None): 
            num_cols2 = [x for x, i in self.df.dtypes \
                        if (i.startswith("str") == False) \
                        & (i.startswith("bool") == False) \
                        & (i.startswith("time") == False)]
            min_functions = [F.min(F.col(cols)) for cols in num_cols2]
            max_functions = [F.max(F.col(cols)) for cols in num_cols2]
            numSumMin = self.df.groupBy(F.col(gr_var)).agg(*min_functions).toPandas()
            numSumMax = self.df.groupBy(F.col(gr_var)).agg(*max_functions).toPandas()
            self.numSum = pd.merge(numSumMin, numSumMax, on = gr_var)
            return self.numSum
    
    
    def strColSummarizer(self, columns: list):
        """summarize a string column"""
        if len(columns) == 1:
            #check that column name is valid
            if columns[0] not in self.df.columns:
                print("Please enter a valid column name.")
                return
            
            #check that column is string data type
            if dict(self.df.dtypes)[columns[0]] != "string":
                print("This column is not string type data. Please enter a column of string data type.")
                return
            
            #create and print summary
            self.strSum = self.df.groupBy(columns[0]).count().toPandas()
            return self.strSum
        
        if len(columns) == 2:
            #check that column names are valid
            if columns[0] not in self.df.columns:
                if columns[1] not in self.df.columns:
                    print("Neither column specified is a valid column name.")
                    return
                    
            #check that columns are string data type and create and print summaries
            if dict(self.df.dtypes)[columns[0]] != "string":
                if dict(self.df.dtypes)[columns[1]] != "string":
                    print("Neither column specified is of string data type.")
                    return
                else:
                    self.strSum = self.df.groupBy(columns[1]).count().toPandas()
                    return self.strSum
            else:
                if dict(self.df.dtypes)[columns[1]] != "string":
                    self.strSum = self.df.groupBy(columns[0]).count().toPandas()
                    return self.strSum
                else:
                    self.strSum = self.df.groupBy(columns[0], columns[1]).count().toPandas()
                    return self.strSum