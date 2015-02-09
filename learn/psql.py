#!/usr/bin/python

import subprocess
try:
    import psycopg2
    has_psycopg2=True
except ImportError:
        has_psycopg2=False
class PgSql:
    def __init__(self):
        self.dbName=''
    def connect(self,dbName):
        if self.dbName!=dbName:
            self.conn= psycopg2.connect('dbname='+dbName)
            self.conn.set_session(isolation_level='READ COMMITTED', readonly=True, deferrable=None, autocommit=True)
            self.cur=self.conn.cursor()
            self.dbName=dbName   
    def getSqlResult(self,sql,db=None):
        dbName='postgres' if db==None else db
        if has_psycopg2:
            self.connect(dbName)
            self.cur.execute(sql)
            return self.cur.fetchall()
        else:
            data=subprocess.check_output(['psql','-d%s'%dbName,'-c', r"copy (%s) to stdout with delimiter E'\t'"%sql])
            return [ tuple(line.split('\t')) for line in data.split('\n') if len(line)>0 ]
            
psql=PgSql()
has_psycopg2=False
print has_psycopg2
sql=r"""
                with lw as(
                    select w.pid as waiting_id,b.pid as blocking_id 
                    from pg_locks w,pg_locks b 
                    where b.granted and not w.granted and w.pid<>b.pid and (w.transactionid=b.transactionid or (w.database=b.database and w.relation=b.relation))
                ) 
                select pid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,state,replace(replace(query,E'\n',''),E'\t','') as query 
                from pg_stat_activity s 
                left outer join lw on s.pid=lw.waiting_id
                """

for r in psql.getSqlResult(' '.join(sql.split())):
    print r

