1) Qual o objetivo do comando cache​ ​em Spark?

Manter datasets em memória pra agilizar o acesso a seus dados

2) O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?

Sim, por que o spark mantem em memoria os resultados do "map" que é utilizado em seguida para fazer o "reduce",
ja o processo convencional MapReduce, armazena em disco os resultados de cada steps, aumetando assim o tempo de I/O.


3) Qual é a função do SparkContext​?

Estabelece uma conexão entre a aplicação e o ambiente Spark, dentro de um SparkContext
é possivel acessar os serviços do Spark e executar Jobs.

4) Explique com suas palavras o que é Resilient​ ​Distributed​ ​Datasets​ (RDD).

É uma estrutura em memória fornecida pelo Spark para a manipulação de dados distribuídos entre os nós do cluster, 
abstraindo a localização física destes dados.

5) GroupByKey​ ​é menos eficiente que reduceByKey​ ​em grandes dataset. Por quê?
????

6) Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")

Ler todos os arquivos de um determinado diretorio no HDFS, e para cada linha de cada arquivo, mapeia as palavras, 
contabiliza sua ocorrencia, e no final geral um arquivo com cada palavra e a quantidade de vezes que foi encontrada.





