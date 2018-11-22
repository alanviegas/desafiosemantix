from pyspark import SparkContext
from pyspark.sql import SQLContext

def main(sc):

    sqlc = SQLContext(sc)
    data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'

    catalog = ''.join("""{
        "table":{"namespace":"dev", "name":"serverlogs"},
        "rowkey":"host",
        "columns":{
            "host":{"cf":"rowkey", "col":"host", "type":"string"},
            "timestamp":{"cf":"logs", "col":"timestamp", "type":"string"},
            "request":{"cf":"logs", "col":"request", "type":"string"},
            "http":{"cf":"logs", "col":"http", "type":"string"},
            "bytes":{"cf":"logs", "col":"bytes", "type":"string"}
            }
         }""".split())

    df = sc.parallelize([('199.72.81.55', '01/Jul/1995:00:00:01 -0400', 'GET /history/apollo/ HTTP/1.0', '200', '6245')]).\
        toDF(schema=['host', 'timestamp','request','http', 'bytes'])

    df.write.options(catalog=catalog, newtable='5', zkUrl='localhost:2181').format(data_source_format).mode("overwrite").save()

    df_read = sqlc.read.options(catalog=catalog).format(data_source_format).load()
    df_read.show()


if __name__ == "__main__":

    sc = SparkContext(appName="StreamCatsFromFlumeToHBase")

    main(sc)

    sc.stop()
