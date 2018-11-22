import sys
import logging
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

# Gera logging
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

    return {'host': host, 'timestamp': timestamp, 'request': request, 'http': http, 'bytes': bytes}


def executar_insert_tabela(tabela_final, tabela_tmp, logger, hqlContext):

    logger.debug('Iniciando o insert na tabela %s:' %tabela_final)

    sql_command = '''
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;
        
        INSERT INTO TABLE %s
        PARTITION (data)
        SELECT 
            bytes,
            host,
            http,
            request,
            data
        FROM %s;
        ''' %tabela_final %tabela_tmp

    logger.debug('Comando a ser executado: %s' % sql_command)

    hqlContext.sql(sql_command)


def main(sc):

    logger = getlogger('carga.serverlogs')
    spark = SparkSession(sc)
    hqlContext = HiveContext(sc)

    logger.debug('Inicio de Processo')

    # Lendo o arquivo texto e criando um RDD em memoria com Spark
    line = sc.textFile("/home/cloudera/Projeto6/serverlogs")

    dataset = line.map(lambda line: get_row(line))

    # converte para o dataframe do pandas
    serverlogs_df = dataset.toDF()
    serverlogs_pd = serverlogs_df.toPandas()

    # converte o tipo da coluna bytes para numerico
    serverlogs_pd['bytes'] = pd.to_numeric(serverlogs_pd['bytes'], errors='coerce')

    # cria a coluna data
    serverlogs_pd['data'] = pd.to_datetime(serverlogs_pd.timestamp.str[:11])

    # cria um dataframe do hive
    serverlogs_hdf = hqlContext.createDataFrame(serverlogs_pd)

    serverlogs_hdf.registerTempTable('serverlogs_tmp')

    executar_insert_tabela('default.t_serverlogs', 'serverlogs_tmp', logger, hqlContext)

    logger.debug('Todas tarefas do pipe executadas com sucesso!')

if __name__ == "__main__":

    sc = SparkContext(appName="carga_serverlogs")
    main(sc)