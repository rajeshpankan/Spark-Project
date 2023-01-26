from pyspark.sql.functions import row_number, col

class SparkSql:
    def __init__(self, spark ,db):
        spark.sql(f"create database if not exists {db}")
        self.db_name = db
        self.spark = spark 
        
    def show_all_database_in_hive(self):
        return self.spark.sql(f"show databases")
    
    def WriteTotable(self, dataFrame, table_name):
        # =f"hdfs:///user/{self.db_name}/{path}
        dataFrame.write.saveAsTable(f"{table_name}",format="hive",mode="overwrite" ,path=f"hdfs:///user/{self.db_name}/{table_name}")
        
    def display_table(self):
        return self.spark.sql(f"show tables from {self.db_name}")
    
    def read_data(self, table_name):
        return self.spark.sql(f"select * from {table_name}")

    def create_view(self,dataframe,tab_name):
        return dataframe.createOrReplaceTempView(tab_name)   
    
    def drop_view(self, tab_name):
        self.spark.catalog.dropTempView(tab_name)
    
    def createDim(self,view_name,dimCol,alias_name):
        return self.spark.sql(f"select ROW_NUMBER() OVER (ORDER BY {dimCol}) as id, {dimCol} as {alias_name} from {view_name} group BY {dimCol}")