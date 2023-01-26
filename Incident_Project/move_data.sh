
hdfs dfsadmin -safemode leave
hdfs dfs -mkdir -p /user/inc/main_data/
hdfs dfs -put data/Source_data/inc_dataset.csv /user/inc/main_data/