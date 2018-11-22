import sys
import logging
from pyspark import sql
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

#Gera logs
def getlogger(name, level=logging.DEBUG):

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        # or else, as I found out, we keep adding handlers and duplicate messages
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

# Essa funcao separa cada linha em colunas, cria uma tupla
def get_row(row):
    host = row[0:row.find(' - -')]
    timestamp = row[row.find('[') + 1:row.find(']')]
    request = row[row.find('"') + 1:row.rfind('"')]
    http = row[row.rfind('"') + 2:row.rfind('"') + 5]
    bytes = row[row.rfind('"') + 6:]

    return (host, timestamp, request, http, bytes)

# Essa funcao armazena cada linha no Hbase
def SaveRecord(rdd):

    logger = getlogger('pipes.SaveRecord')

    logger.debug('Tamanho do RDD %s' % rdd.count())

    sqlContext = sql.SQLContext(sc)

    datamap = rdd.map(lambda rdd: get_row(rdd))

    logger.debug('datamap [%s]' % datamap.collect())

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

    dt_datamap = sc.parallelize(datamap.collect())

    df_row = sqlContext.createDataFrame(dt_datamap, schema=['host', 'timestamp', 'request', 'http', 'bytes'])

    df_row.write.options(catalog=catalog, newtable='5', zkUrl='localhost:2181').format(data_source_format).\
        mode('overwrite').save()


def main(sc):

    logger = getlogger('pipes.load_stream')

    host = "localhost"
    port = 9898
    batchInterval = 5

    logger.debug('Hostname sinks [%s]:[%s]' %(host, port))

    #Criando um context stream e setando o tamanho do intervalo
    ssc = StreamingContext(sc, batchInterval)

    streams = ssc.textFileStream("/serverlogs")

    #streams = FlumeUtils.createStream(ssc, host, port)

    streams.foreachRDD(SaveRecord)

    # Inicia a coleta e processamento do stream de dados
    ssc.start()

    # Aguarda a computacao ser finalizada
    ssc.awaitTermination()

    ssc.stop()
    logger.debug('Todas tarefas do pipe executadas com sucesso!')

if __name__ == "__main__":

    sc = SparkContext(appName="local[*]")

    main(sc)