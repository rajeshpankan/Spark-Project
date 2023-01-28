from dependencies.spark_init import sparkLoader
from dependencies.spark_sql import SparkSql
from dependencies.extract_data import sparkReader


def demo_function():
    spark_object = sparkLoader('local', 'test')
    spark = spark_object.spark
    
    spark_sql_object = SparkSql(spark, 'inc')
    databases = spark_sql_object.show_all_database_in_hive()
    # databases.show()
    
    spark_reader = sparkReader()
    df = spark_reader.readCsvFromSpark(spark,'hdfs:///user/inc/main_data/')
    
    
    spark_sql_object.create_view(df,"incident_data")
    df = spark_sql_object.read_data("incident_data")
    # df.show(3)
    
    # ## spark_sql_object.WriteTotable()

    df_status=spark_sql_object.createDim("incident_data","active", "active_status")
    # df_status.show()

    spark_sql_object.WriteTotable(df_status, "inc.Dim_Active")
    
    df_inc_state=spark_sql_object.createDim("incident_data","incident_state", "incident_state")

    spark_sql_object.WriteTotable(df_inc_state, "inc.Dim_Inc_state")
    # df_status.show()

    df_callerDim=spark_sql_object.createDim("incident_data","caller_id", "caller_id")
    spark_sql_object.WriteTotable(df_callerDim, "inc.Dim_Caller")
    
    
    fact_df = spark_reader.dataframe_join(df, df_status, "active", "active_status", {"id":"active_id"},["active_status"])
    
    fact_df = spark_reader.dataframe_join(fact_df, df_inc_state, "incident_state","incident_state", {"id":"state_id"})
    # fact_df.show()

    fact_df = spark_reader.dataframe_join(fact_df, df_callerDim, "caller_id",'caller_id', {'id':"call_id"})

    fact_df.printSchema()
    fact_df.select("call_id").show()
    spark_object.stop_spark()
    
    
    