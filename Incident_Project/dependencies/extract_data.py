

class sparkReader:
    def readCsvFromSpark(self, spark, csv_path):
        return spark.read.csv(csv_path, header=True, inferSchema=True, nullValue='?' ,timestampFormat="dd-MM-yyyy HH:mm")

    

