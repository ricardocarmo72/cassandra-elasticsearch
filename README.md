# cassandra-elasticsearch
Sincroniza dados entre os bancos Cassandra e ElasticSearch

Esta rotina sincroniza dados entre os bancos noSQL, Cassandra e ElasticSearch.
Deve ser executada como um processo daemon e pode ser iniciada a partir do terminal:

Para executar:
$python synchronize start 60

Para interromper:
$python synchronize stop

Para reiniciar:
$python synchronize restart

O parâmetro numérico após o "start" indica o intervalo em segundos a cada sincronização.
Para o correto funcionamento, é requerido que as seguintes tabelas tenham sido criadas nos dois bancos:

mymodel: Armazena dados genéricos com a coluna ID como chave primária.\n
mymodelactivity: Armazena um log de alterações efetuadas em mymodel, salvando ID, tipo de alteração (inclusão, atualização) e data/hora.

No banco Cassandra, estas tabelas podem ser criadas via cqlsh, usando os comandos abaixo:
 
create table mymodel(
    id uuid,
    other_field varchar,
    primary key (id));

create table mymodelactivity(
    id uuid,
    activity_code varchar,
    interaction_time timestamp,
    primary key (activity_code,interaction_time)
    ) with clustering order by (interaction_time desc);
CREATE INDEX ON mymodelactivity (id);
    
No banco ElasticSearch, via python shell, utilize os seguintes comandos:

es = Elasticsearch()
es.indices.create(index='mymodel', ignore=400)
es.indices.create(index='mymodelactivity', ignore=400)

As colunas serão definidas automaticamente no primeiro insert efetuado.
