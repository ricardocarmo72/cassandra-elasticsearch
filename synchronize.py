#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, time
import datetime
from daemon import Daemon
from uuid import UUID
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


class MyDaemon(Daemon):
    interval = 10
    last_sync = datetime.datetime.strptime('01/01/1980','%d/%m/%Y')
    
    def run(self):
        while True:
            self.last_sync = synchronize(self.last_sync)
            time.sleep(self.interval)


def synchronize(dt_init):
    '''Efetua a sincronização de dados entre os bancos Cassandra e ElasticSearch, nos dois sentidos'''
    
    ret_c = cassandra_to_elasticsearch(dt_init)
    ret_e = elasticsearch_to_cassandra(dt_init)
    
    return min(ret_c,ret_e)
    
    
def cassandra_to_elasticsearch(dt_init):
    ''' Sincroniza dados do banco Cassandra para o banco ElasticSearch.'''
    
    retorno = dt_init
    
    ''' Conecta com o banco Cassandra '''
    cluster = Cluster()
    session = cluster.connect('demo')
    
    ''' Conecta com o banco ElasticSearch '''
    es = Elasticsearch()
    
    '''Obtém as alterações no banco Cassandra realizadas a partir de <dt_init>'''
    stat = session.prepare('select * from mymodelactivity where activity_code in :code and interaction_time > :dt')
    dic = {}
    dic['dt']=dt_init
    dic['code']=['I','U']
    results = session.execute(stat,dic)
    
    for result in results:
        dt_result = result.interaction_time 
        if dt_result>retorno:
            '''A próxima sincronização ocorrerá a partir desta data/hora'''
            retorno = dt_result
        
        '''Verifica se precisa atualizar no ElasticSearch'''
        insere = False
        atualiza = False
        try:
            dic = es.get(index='mymodelactivity', id=result.id)
            str_interaction_time = dic['_source']['interaction_time']
            interaction_time = datetime.datetime.strptime(str_interaction_time, '%Y-%m-%dT%H:%M:%S.%f')
            
            atualiza = (interaction_time < dt_result)
        except:
            insere = True
        
        if insere or atualiza:
            '''Obtém os dados a serem replicados da tabela mymodel'''
            dados = session.execute('select * from mymodel where id=%s' % result.id)
            
            es.index(index = 'mymodelactivity', 
                     id = result.id,
                     doc_type = 'generic',
                     body = {'activity_code': 'I' if insere else 'U',
                             'interaction_time': result.interaction_time }) 
            
            es.index(index = 'mymodel', 
                     id = result.id,
                     doc_type = 'generic',
                     body = {'other_field': dados[0].other_field })
    
    return retorno 


def elasticsearch_to_cassandra(dt_init):
    ''' Sincroniza dados do banco ElasticSearch para o banco Cassandra.
        Precisamos da variável <retorno> para as próximas sincronizações '''
    retorno = dt_init
    
    ''' Conecta com o banco ElasticSearch '''
    es = Elasticsearch()
    
    ''' Conecta com o banco Cassandra '''
    cluster = Cluster()
    session = cluster.connect('demo')
    
    '''Obtém as alterações no banco ElasticSearch realizadas a partir de <dt_init>'''
    dic = es.search(index='mymodelactivity', 
                    doc_type='generic',
                    body={"size": 1000,
                          "query":
                          {"range": 
                           {"interaction_time" : 
                            {"from": dt_init, "to": datetime.datetime.now()}
                           }
                          },
                          "sort": {"interaction_time": "desc"},
                         }
                    )    
    
    for hit in dic['hits']['hits']:
        str_interaction_time = hit['_source']['interaction_time']
        dt_result = datetime.datetime.strptime(str_interaction_time, '%Y-%m-%dT%H:%M:%S.%f')
        if dt_result>retorno:
            '''A próxima sincronização ocorrerá a partir desta data/hora'''
            retorno = dt_result
            
        '''Verifica se precisa atualizar no Cassandra'''
        insere = False
        atualiza = False
        
        stat = session.prepare('select * from mymodelactivity where id=:id')
        dic = {}
        dic['id']=UUID(hit['_id'])
        result = session.execute(stat,dic)
        if result:
            interaction_time = result[0].interaction_time
            atualiza = (interaction_time < dt_result)
        else:
            insere = True
        
        if insere or atualiza:
            '''Obtém os dados a serem replicados da tabela mymodel'''
            dados = es.get(index='mymodel', id=hit['_id'])
            
            cql_statement = session.prepare("insert into mymodelactivity (id,activity_code,interaction_time) values (:id,:activity_code,:interaction_time) using ttl 86400")
            dic = {}
            dic['id']               = UUID(hit['_id'])
            dic['activity_code']    = 'I' if insere else 'U'
            dic['interaction_time'] = dt_result
            session.execute(cql_statement, dic)
            
            cql_statement = session.prepare("insert into mymodel (id,other_field) values (:id,:other_field)")
            dic = {}
            dic['id']           = UUID(hit['_id'])
            dic['other_field']  = dados['_source']['other_field']
            
            session.execute(cql_statement, dic)
    
    return retorno
 
 
if __name__ == "__main__":
    daemon = MyDaemon('/tmp/sycnhronize.pid')
    
    if len(sys.argv) == 2 and sys.argv[1] == 'stop':
        daemon.stop()
    elif len(sys.argv) == 2 and sys.argv[1] == 'restart':
        daemon.restart()
    elif len(sys.argv) == 3 and sys.argv[1] == 'start':
        try:
            daemon.interval = int(sys.argv[2])
        except:
            print "Intervalo %s inválido, entre com o intervalo em segundos." % sys.argv[2]
            sys.exit(2)
        daemon.start()
        
    else:
        print "Uso: %s start <intervalo>|stop|restart" % sys.argv[0]
        sys.exit(2)
        
    sys.exit(0)
