DROP TABLE IF EXISTS default.t_serverlogs;

CREATE EXTERNAL TABLE default.t_serverlogs
(
bytes int COMMENT 'Total​ ​de​ ​bytes​ ​retornados',
host string COMMENT 'Host fazendo a requisição​',
http string COMMENT 'Código​ ​do​ ​retorno​ ​HTTP',
request string COMMENT 'Requisição',
timestamp string COMMENT 'Data/Hora da geracao do log'
)
COMMENT 'Tabela para armazenar os logs dos servidores'
PARTITIONED BY (data date)
STORED AS ORC LOCATION 'hdfs://quickstart.cloudera:8020/user/hive/warehouse/t_serverlogs'

MSCK REPAIR TABLE default.t_serverlogs;