from pyspark.sql.functions import  col

class sparkReader:
    def readCsvFromSpark(self, spark, csv_path):
        return spark.read.csv(csv_path, header=True, inferSchema=True, nullValue='?' ,timestampFormat="dd-MM-yyyy HH:mm")
    
    def dataframe_join(self,df1, df2, df1_col, df2_col, col_rename={}, drop_columns =[] ):
        
        if df1_col!= df2_col:
            df = df1.join(df2, df1[df1_col] == df2[df2_col])
        else:
            df = df1.join(df2, [df1_col])
        for old_column, new_column in col_rename.items():
            df= df.withColumnRenamed(old_column, new_column)
            
        for column in drop_columns:
            df = df.drop(col(column))
            
        return df
    

