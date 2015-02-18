#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid
import datetime
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

def popula_cassandra():
    '''Popula dados no banco Cassandra, para testes'''
    
    ''' Conecta com o banco Cassandra '''
    cluster = Cluster()
    session = cluster.connect('demo')
    
    for i in range(10):
        uid = uuid.uuid4()
        dt = datetime.datetime.now()
        
        cql_statement = session.prepare("insert into mymodel (id,other_field) values (:id,:other_field)")
        dic = {}
        dic['id'] = uid
        dic['other_field'] = 'foo'
        session.execute(cql_statement, dic)
        
        cql_statement = session.prepare("insert into mymodelactivity (id,activity_code,interaction_time) values (:id,:activity_code,:interaction_time) using ttl 86400")
        dic = {}
        dic['id'] = uid
        dic['activity_code'] = 'I'
        dic['interaction_time'] = dt
        session.execute(cql_statement, dic)
    
    return


def popula_elasticsearch():
    '''Popula dados no banco ElasticSearch, para testes'''
    
    ''' Conecta com o banco ElasticSearch '''
    es = Elasticsearch()
    
    for i in range(10):
        uid = uuid.uuid4()
        dt = datetime.datetime.now()
        
        res = es.index(index = 'mymodelactivity', 
                       id = uid,
                       doc_type = 'generic',
                       body = {'activity_code': 'I',
                               'interaction_time': dt }) 
        print res
        res = es.index(index = 'mymodel', 
                       id = uid,
                       doc_type = 'generic',
                       body = {'other_field': 'foo' })
        print res
    return
