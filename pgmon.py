#!/usr/bin/python
import datetime,re, os
import subprocess,threading,signal
import curses
try:
    import psutil
except ImportError:
        has_psutil=False
try:
    import psycopg2
    has_psycopg2=True
except ImportError:
        has_psycopg2=False

has_psycopg2=False
has_psutil=False

######################################################################
###   Python 2.6 backport
#######################################################################
if "check_output" not in dir( subprocess ):
    def f(*popenargs, **kwargs):
        if 'stdout' in kwargs:
            raise ValueError('stdout argument not allowed, it will be overridden.')
        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        return output
    subprocess.check_output = f

import ctypes as c
_get_dict = c.pythonapi._PyObject_GetDictPtr
_get_dict.restype = c.POINTER(c.py_object)
_get_dict.argtypes = [c.py_object]

from datetime import timedelta
try:
    timedelta.total_seconds # new in 2.7
except AttributeError:
    def total_seconds(td):
        return float((td.microseconds +
                      (td.seconds + td.days * 24 * 3600) * 10**6)) / 10**6
    d = _get_dict(timedelta)[0]
    d['total_seconds'] = total_seconds
###############################################################################

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
            data=subprocess.check_output(['psql','-d%s'%dbName,'-c', r"copy (%s) to stdout with delimiter E'\t' NULL as ' -- '" % ' '.join(sql.split())])
            return [ tuple(line.split('\t')) for line in data.split('\n') if len(line)>0 ]
stats_items={
    'db': {'columns':['db','size_pretty','sessions','xact_commit','tps','blks_hit','blks_read','hit_ratio','tup_iud','tup_returned'],
            'formats':['s','s','s','s','.1f','s','s','s','s','s']
           },
    'session':{'columns':['pid','cpu','mem','read','write','db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','blking_id','locks','state','query'],
                'formats':['s','.1f','.1f','.1f','.1f','s','s','s','s','s','s','s','s','s','s','float_s']
              },
    'table': {'columns':['tbl_id','scm','tbl','tbl_sz','idx_sz','xid_age','seq_scn','idx_scn','tup_i','tup_u','tup_d','live_tup','dead_tup','lst_autovcm','lst_autoanz','a_vcm_n','a_anz_n'],
            'formats':['s','s','s','s','s','s','s','s','s','s','s','s','s','s','s','s']
            },
    'index':{'columns':['scm_id','scm','tbl','idx','tbl_sz','idx_sz','idx_scn','idx_tup_rd'],
            'formats':['s','s','s','s','s','s','s','s']
            },
    'lock':{'columns':['pid','relname','locktype','mode','virtualxid','transactionid','granted','blocked_by'],
            'formats':['s','s','s','s','s','s','s','s']
            }
    }

class PgStats:
    sqls_all_vertions={
        '8.4':{
            'db_list':"""
                select extract(epoch from now()) as now,datname as db,pg_database_size(datname) as size,numbackends as sessions,xact_commit,xact_rollback,blks_hit,blks_read,(tup_inserted+tup_updated+tup_deleted) as tup_iud,tup_returned
                from pg_stat_database
                where datname not in ('template0','template1','postgres')
                """,
            'session_list':"""
                with lw as(
                    select w.pid as waiting_id,b.pid as blocking_id
                    from pg_locks w,pg_locks b
                    where b.granted and not w.granted and w.pid<>b.pid and (w.transactionid=b.transactionid or (w.database=b.database and w.relation=b.relation))
                )
                select * from (
                    select procpid as backend_id, datname as db, usename as user,'' as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,locks,'' as state,replace(replace(current_query,E'\n',''),E'\t','') as query
                    from pg_stat_activity s
                    left outer join lw on s.procpid=lw.waiting_id
                    left outer join (select pid,count(1) as locks from pg_locks group by pid) lc on s.procpid=lc.pid
                )t
                """,
            'table_list':"""
                select st.relid,schemaname as scm, relname as tbl,  relpages*8,coalesce(indpages,0)*8,xid_age,coalesce(seq_scan,0),coalesce(idx_scan,0),n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup,last_autovacuum::timestamp(0) as lst_autovcm, last_autoanalyze::timestamp(0) as lst_autoanz,'' as autovcm_n,'' as  autoanz_n
                    from pg_stat_user_tables st,
                ( select relid,t.relpages+coalesce(ts.relpages,0)+coalesce(ti.relpages,0) as relpages,indpages,t.xid_age
                    from
                      (select oid as relid,relpages::bigint,age(relfrozenxid) as xid_age,reltoastrelid from pg_class where relkind='r') t
                      left outer join (select sum(relpages) as indpages,indrelid from pg_class i, pg_index r where relkind='i' and i.oid=r.indexrelid group by indrelid) i on t.relid=i.indrelid
                      left outer join pg_class ts on t.reltoastrelid=ts.oid
                      left outer join pg_class ti on ts.reltoastidxid=ti.oid
                )p
                where st.relid=p.relid
                """,
            'index_list':"""
                select indexrelid,schemaname as scm,st.relname as tbl,indexrelname as idx,r.relpages*8 as tbl_sz, i.relpages*8 as idx_sz, idx_scan,idx_tup_read
                from pg_stat_user_indexes st, pg_class r, pg_class i
                where st.relid=r.oid and st.indexrelid=i.oid
                """,
            'lock_list':"""
                select c.pid,relname,c.locktype,c.mode,c.virtualxid,c.transactionid,c.granted,h.pid as blocked_by
                from pg_locks c
                    left outer join pg_locks h on h.granted and not c.granted and (h.transactionid=c.transactionid or (h.database=c.database and h.relation=c.relation))
                    left outer join pg_class r on c.relation=r.oid
                where c.pid=
                """
        },
        '9.1':{
            'db_list':"""
                select extract(epoch from now()) as now,datname as db,pg_database_size(datname) as size,numbackends as sessions,xact_commit,xact_rollback,blks_hit,blks_read,(tup_inserted+tup_updated+tup_deleted) as tup_iud,tup_returned
                from pg_stat_database
                where datname not in ('template0','template1','postgres')
                """,
            'session_list':"""
                with lw as(
                    select w.pid as waiting_id,b.pid as blocking_id
                    from pg_locks w,pg_locks b
                    where b.granted and not w.granted and w.pid<>b.pid and (w.transactionid=b.transactionid or (w.database=b.database and w.relation=b.relation))
                )
                select procpid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,locks,'' as state,replace(replace(current_query,E'\n',''),E'\t','') as query
                from pg_stat_activity s
                left outer join lw on s.procpid=lw.waiting_id
                left outer join (select pid,count(1) as locks from pg_locks group by pid) lc on s.procpid=lc.pid
                """,
            'table_list':"""
                select st.relid,schemaname as scm, relname as tbl,  relpages*8,coalesce(indpages,0)*8,xid_age,coalesce(seq_scan,0),coalesce(idx_scan,0),n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup,last_autovacuum::timestamp(0) as lst_autovcm, last_autoanalyze::timestamp(0) as lst_autoanz,autovacuum_count as autovcm_n,autoanalyze_count as  autoanz_n
                    from pg_stat_user_tables st,
                ( select relid,t.relpages+coalesce(ts.relpages,0)+coalesce(ti.relpages,0) as relpages,indpages,t.xid_age
                    from
                      (select oid as relid,relpages::bigint,age(relfrozenxid) as xid_age,reltoastrelid from pg_class where relkind='r') t
                      left outer join (select sum(relpages) as indpages,indrelid from pg_class i, pg_index r where relkind='i' and i.oid=r.indexrelid group by indrelid) i on t.relid=i.indrelid
                      left outer join pg_class ts on t.reltoastrelid=ts.oid
                      left outer join pg_class ti on ts.reltoastidxid=ti.oid
                )p
                where st.relid=p.relid
                """,
            'index_list':"""
                select indexrelid,schemaname as scm,st.relname as tbl,indexrelname as idx,r.relpages*8 as tbl_sz, i.relpages*8 as idx_sz, idx_scan,idx_tup_read
                from pg_stat_user_indexes st, pg_class r, pg_class i
                where st.relid=r.oid and st.indexrelid=i.oid
                """,
            'rep_list':"""
                select client_addr,application_name,usename,sync_state,state,pg_current_xlog_location() as cur_location,sent_lsn-cur_lsn as sent_dif,flush_lsn-cur_lsn as flush_dif,replay_lsn-cur_lsn as replay_dif
                from (
                    select *, (('x'||lpad(cur_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(cur_loc[2],8,'0'))::bit(32)::bigint) as cur_lsn,
                        (('x'||lpad(sent_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(sent_loc[2],8,'0'))::bit(32)::bigint) as sent_lsn,
                        (('x'||lpad(flush_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(flush_loc[2],8,'0'))::bit(32)::bigint) as flush_lsn,
                        (('x'||lpad(replay_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(replay_loc[2],8,'0'))::bit(32)::bigint) as replay_lsn
                    from  (
                        select *,regexp_split_to_array(sent_location,'/') as sent_loc,
                            regexp_split_to_array(flush_location,'/') as flush_loc,
                            regexp_split_to_array(replay_location,'/') as replay_loc,
                            regexp_split_to_array(pg_current_xlog_location(),'/') as cur_loc
                        from pg_stat_replication
                    )t1
                )t2
                """
        },
        '9.3':{
            'db_list':"""
                select extract(epoch from now()) as now,datname as db,pg_database_size(datname) as size,numbackends as sessions,xact_commit,xact_rollback,blks_hit,blks_read,(tup_inserted+tup_updated+tup_deleted) as tup_iud,tup_returned
                from pg_stat_database
                where datname not in ('template0','template1','postgres')
                """,
            'session_list':"""
                with lw as(
                    select w.pid as waiting_id,b.pid as blocking_id
                    from pg_locks w,pg_locks b
                    where b.granted and not w.granted and w.pid<>b.pid and (w.transactionid=b.transactionid or (w.database=b.database and w.relation=b.relation))
                )
                select * from (
                    select s.pid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,locks,state,replace(replace(query,E'\\n',''),E'\\t','') as query
                    from pg_stat_activity s
                    left outer join lw on s.pid=lw.waiting_id
                    left outer join (select pid,count(1) as locks from pg_locks group by pid) lc on s.pid=lc.pid
                )t
                """,
            'table_list':
                """select st.relid,schemaname as scm, relname as tbl,  relpages*8, coalesce(indpages,0)*8, xid_age,coalesce(seq_scan,0),coalesce(idx_scan,0),n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup,last_autovacuum::timestamp(0) as lst_autovcm, last_autoanalyze::timestamp(0) as lst_autoanz,autovacuum_count as autovcm_n,autoanalyze_count as  autoanz_n
                    from pg_stat_user_tables st,
                ( select relid,t.relpages+coalesce(ts.relpages,0)+coalesce(ti.relpages,0) as relpages,indpages,t.xid_age
                    from
                      (select oid as relid,relpages::bigint,age(relfrozenxid) as xid_age,reltoastrelid from pg_class where relkind='r') t
                      left outer join (select sum(relpages) as indpages,indrelid from pg_class i, pg_index r where relkind='i' and i.oid=r.indexrelid group by indrelid) i on t.relid=i.indrelid
                      left outer join pg_class ts on t.reltoastrelid=ts.oid
                      left outer join pg_class ti on ts.reltoastidxid=ti.oid
                )p
                where st.relid=p.relid
                """,
            'index_list':"""
                select indexrelid,schemaname as scm,st.relname as tbl,indexrelname as idx,r.relpages*8 as tbl_sz, i.relpages*8 as idx_sz, idx_scan,idx_tup_read
                from pg_stat_user_indexes st, pg_class r, pg_class i
                where st.relid=r.oid and st.indexrelid=i.oid
                """,
            'lock_list':"""
                select c.pid,relname,c.locktype,c.mode,c.virtualxid,c.transactionid,c.granted,h.pid as blocked_by
                from pg_locks c
                    left outer join pg_locks h on h.granted and not c.granted and (h.transactionid=c.transactionid or (h.database=c.database and h.relation=c.relation))
                    left outer join pg_class r on c.relation=r.oid
                where c.pid=
                """,
            'rep_list':"""
                select client_addr,application_name,usename,sync_state,state,pg_current_xlog_location() as cur_location,sent_lsn-cur_lsn as sent_dif,flush_lsn-cur_lsn as flush_dif,replay_lsn-cur_lsn as replay_dif
                from (
                    select *, (('x'||lpad(cur_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(cur_loc[2],8,'0'))::bit(32)::bigint) as cur_lsn,
                        (('x'||lpad(sent_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(sent_loc[2],8,'0'))::bit(32)::bigint) as sent_lsn,
                        (('x'||lpad(flush_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(flush_loc[2],8,'0'))::bit(32)::bigint) as flush_lsn,
                        (('x'||lpad(replay_loc[1],8,'0'))::bit(32)::bigint)*x'100000000'::bigint+(('x'||lpad(replay_loc[2],8,'0'))::bit(32)::bigint) as replay_lsn
                    from  (
                        select *,regexp_split_to_array(sent_location,'/') as sent_loc,
                            regexp_split_to_array(flush_location,'/') as flush_loc,
                            regexp_split_to_array(replay_location,'/') as replay_loc,
                            regexp_split_to_array(pg_current_xlog_location(),'/') as cur_loc
                        from pg_stat_replication
                    )t1
                )t2
                """
        }
    }
    db_list={}
    pg_settings={}
    #i=0
    dbname=''
    def getSqlResult(self,sql,db=None):
        return self.psql.getSqlResult(sql,db)
    def __init__(self):
        self.psql=PgSql()
        self.sqls=self.sqls_all_vertions[re.findall('\d+\.\d+',self.getPgVersion())[0]]
        self.getRepMode()
    def getRepMode(self):
        rep_mod=''
        if float(re.findall('(\d+\.\d+)',self.getPgVersion())[0]) > 9:
            sql_rep_mode="select pg_is_in_recovery()::text,(select count(1) from pg_stat_replication) as rep_cnt"
            rs_rep_mod=self.getSqlResult(sql_rep_mode)
            if len(rs_rep_mod)>0:
                pg_is_in_recovery,rep_cnt=rs_rep_mod[0]
                if pg_is_in_recovery=='false' and int(rep_cnt)>0:
                    rep_mod='master'
                elif pg_is_in_recovery=='true':
                    rep_mod='standby'
                    if 'sync_app_name' not in self.pg_settings:
                        fl_recovery_conf=os.path.join(self.getPgPath(),'recovery.conf')
                        conninfo=subprocess.check_output(['grep','primary_conninfo',fl_recovery_conf])
                        self.pg_settings['sync_master']=re.findall('host=([\w\.]+) ', conninfo)[0]
                        self.pg_settings['sync_app_name']=re.findall('application_name=([\w\-\.]+)', conninfo)[0]
            self.pg_settings['rep_mod']=rep_mod
        return rep_mod
    def getRepStatus(self):
        if self.getRepMode()=='master':
            return {'rep_mod':'master','rep_list':self.getSqlResult(self.sqls['rep_list'])}
        elif self.getRepMode()=='standby':
            sql= "select pg_last_xlog_receive_location(),pg_last_xlog_replay_location(),pg_last_xact_replay_timestamp()::timestamp(0)::text"
            rep_status=self.getSqlResult(sql)
            return {'rep_mod':'standby','rep_xlog_rcv_loc':rep_status[0][0],'rep_xlog_replay_loc':rep_status[0][1],'rep_xlog_replay_tm':rep_status[0][2]}
        else:
            return {'rep_mod':'standalone'}
    def getPgPath(self):
        if 'data_directory' not in self.pg_settings:
            path_data=''
            cmdlines= [' '.join(p.cmdline()) for p in psutil.process_iter() if p.name()=='postgres'] if has_psutil else [p['COMMAND'] for p in PsStats.getPs('postgres')]
            for p in cmdlines:
                if len(re.findall('\-D ([^ ]+) ',p))>0:
                    path_bin=p.split()[0]
                    path_data=re.findall('\-D ([^ ]+) ',p)[0]
                    break
            if path_data=='':
                raise Exception('No postgres processes founed, please make sure postgres is running!')
            #path_data=self.getSqlResult("select setting from pg_settings where name='data_directory'")[0][0]
            self.pg_settings['data_directory']=path_data
            self.pg_settings['bin_directory']=path_bin
        return self.pg_settings['data_directory']
    def getPgVersion(self):
        if 'version' not in self.pg_settings:
            pg_data=self.getPgPath()
            pg_ver=self.getSqlResult(r"select substring(version() from E'PostgreSQL ([^ ]+) on')")[0][0]
            self.pg_settings['version']=pg_ver
        return self.pg_settings['version']
    def getPgStartTime(self):
        if 'start_time' not in self.pg_settings:
            sql_start_time="SELECT pg_postmaster_start_time()::timestamp(0)"
            self.pg_settings['start_time']=self.getSqlResult(sql_start_time)[0][0]
        return self.pg_settings['start_time']
    def getTableList(self,db):
        tl={}
        for r in self.getSqlResult(self.sqls['table_list'],db):
            tl[r[0]]=dict(zip(stats_items['table']['columns'],r[:3]+tuple([long(t) for t in r[3:13]])+r[13:15]+(long(r[15] if r[15]!='' else 0),(long(r[16] if r[16]!='' else 0)),)))
        return tl
    def getIndexList(self,db):
        il={}
        for r in self.getSqlResult(self.sqls['index_list'],db):
            il[r[0]]=dict(zip(stats_items['index']['columns'],r[:4]+tuple([long(t) for t in r[4:]])))
        return il
    def getLockList(self,db,pid):
        rows=self.getSqlResult(self.sqls['lock_list']+str(pid),db)
        return [dict(zip(stats_items['lock']['columns'],r)) for r in rows]
    def getSessionList(self):
        sl={}
        for r in self.getSqlResult(self.sqls['session_list']):
            sl[r[0]]=dict(zip(['pid','db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','blking_id','locks','state','query'],r))
        return sl
    def getSessionDetail(self,pid):
        sn_ = self.getSqlResult(self.sqls['session_list']+' where backend_id='+pid)
        if len(sn_)>0:
            sn=dict(zip(['pid','db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','blking_id','locks','state','query'],sn_[0]))
            db=sn['db']
            locks=self.getLockList(db,pid)
            return {'session':sn,'locks':locks}
        return None
    def terminateBackend(self,pid):
        _rs = self.getSqlResult('select pg_terminate_backend(%s)' % pid)
        return len(_rs)>0 and _rs[0][0]=='t'
    def getDbList(self):
        #self.i+=1
        db_list={}#'i':self.i}
        for db in self.getSqlResult(self.sqls['db_list']):
            db_list[db[1]]={'db':db[1],'snap_tm':float(db[0]),'size':long(db[2]),'size_pretty':bytes2human(long(db[2])),'sessions':int(db[3]),'xact_commit':long(db[4]),'xact_rollback':long(db[5]),'blks_hit':long(db[6]),'blks_read':long(db[7]),'tup_iud':long(db[8]),'tup_returned':long(db[9])}
            delt_tm=db_list[db[1]]['snap_tm']-self.db_list[db[1]]['snap_tm'] if db[1] in self.db_list else 0
            db_list[db[1]]['tps'] = (db_list[db[1]]['xact_commit']-self.db_list[db[1]]['xact_commit'])/delt_tm if delt_tm>0 else 0
            db_list[db[1]]['hit_ratio']=db_list[db[1]]['blks_hit']*100/(db_list[db[1]]['blks_hit']+db_list[db[1]]['blks_read']) if db_list[db[1]]['blks_hit']+db_list[db[1]]['blks_read']>0 else 0
            delt_hit=db_list[db[1]]['blks_hit']-self.db_list[db[1]]['blks_hit'] if db[1] in self.db_list else 0
            delt_read=db_list[db[1]]['blks_read']-self.db_list[db[1]]['blks_read'] if db[1] in self.db_list else 0
            db_list[db[1]]['hit_ratio_delt']=delt_hit*100/(delt_hit+delt_read) if delt_hit+delt_read>0 else db_list[db[1]]['hit_ratio']
        self.db_list=db_list
        #print db_list
        return self.db_list
    def getPgControlData(self):
        pg_data=self.getPgPath()
        pg_bin=self.pg_settings['bin_directory']
        ctl_data=subprocess.check_output([pg_bin+'/pg_controldata', pg_data])
        pg_state=re.findall('Database cluster state: +(.+)\n',ctl_data)
        last_checkpoint_tm=re.findall('Time of latest checkpoint: +(.+)\n',ctl_data)
        return {'pg_state':pg_state[0],'last_checkpoint_tm':last_checkpoint_tm[0]}
    def getPgBackupStatus(self):
        pg_data=self.getPgPath()
        lbl_data=''
        try:
            lbl_data=open(pg_data+'/backup_label').readlines()  #lable file exists, backup in processing
        except IOError:
            lbl_fls=[f for f in os.listdir(pg_data+'/pg_xlog') if f.endswith('.backup')]
            if len(lbl_fls)>0:
                lbl_data=lbl_data=open(pg_data+'/pg_xlog/'+lbl_fls[0]).readlines()
        lbl=''.join(re.findall('LABEL: (.+)$',lbl_data))
        start=''.join(re.findall('START TIME: (.+)$',lbl_data))
        stop=''.join(re.findall('STOP TIME: (.+)$',lbl_data))
        return {'lable':lbl,'start':start,'stop':stop}
    def getPgArchiveStatus(self):
        cmdlines= [' '.join(p.cmdline()) for p in psutil.process_iter() if p.name()=='postgres'] if has_psutil else [p['COMMAND'] for p in PsStats.getPs('postgres')]
        ps=[p for p in cmdlines if p.startswith('postgres: archiver process')]
        status=''
        lag=0
        if len(ps)>0:
            pg_data=self.getPgPath()
            status=re.findall('postgres: archiver process +(.+)',ps[0])[0]
            lag=len([f for f in os.listdir(pg_data+'/pg_xlog/archive_status') if f.endswith('.ready')])
        return {'status':status,'lag':lag}
def bytes2human(n):
    # http://code.activestate.com/recipes/578019
    # >>> bytes2human(10000)
    # '9.8K'
    # >>> bytes2human(100001221)
    # '95.4M'
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n

def avg(list):
    l=len(list)
    return sum(list)/l if l>0 else 0

class PsStats:
    re_time=re.compile(r'(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)')
    def __init__(self,user='postgres'):
        self.user=user
        self.shareLock=threading.Lock()
        self.ps={}
        self.stopRequestEvent=threading.Event()
        self.thd_ps=threading.Thread(target=self.background_worker)
        self.thd_ps.start()
    @staticmethod
    def getPs(user):
        ps=[p.split() for p in subprocess.check_output(('ps -u '+user+' -o pid,cputime,etime,stat,%mem,command').split()).split('\n') if len(p)>3]
        nms=ps[0] # PID     TIME     ELAPSED STAT %MEM COMMAND
        return [ {'PID':p[0], 'TIME':p[1], 'ELAPSED':p[2], 'STAT':p[3], '%MEM':p[4], 'COMMAND':' '.join(p[5:])} for p in ps[1:]]
    def background_worker(self):
        sleepInterval=2
        while not self.stopRequestEvent.is_set():
            for p in PsStats.getPs(self.user):
                pid=p['PID']
                nv=p
                d,h,m,s=self.re_time.findall(p['TIME'])[0]
                nv['cputimes'] = (0 if d=='' else 3600*24*int(d)) + (0 if h=='' else 3600*int(h)) + int(m)*60 + int(s)
                d,h,m,s=self.re_time.findall(p['ELAPSED'])[0]
                nv['etimes'] = (0 if d=='' else 3600*24*int(d)) + (0 if h=='' else 3600*int(h)) + int(m)*60 + int(s)
                with self.shareLock:
                    if pid in self.ps: # existing process
                        ov=self.ps[pid]
                        nv['%CPU']= '%.1f' % (100.0 * (nv['cputimes'] - ov['cputimes']) / (0.000000000001 + nv['etimes'] - ov['etimes']) )
                    else:
                        nv['%CPU']= '0.0'
                self.ps[pid]=nv
            self.stopRequestEvent.wait(sleepInterval)
        self.thd_ps=None
    def getPsStats(self,pid):
        with self.shareLock:
            p=self.ps.get(str(pid),{'%MEM': '0', '%CPU': '0', 'STAT': 'R'})
            return {'cpu':float(p['%CPU']),'mem':float(p['%MEM']),'status':p['STAT'],'read_t':0,'write_t':0}
    def __del__(self):
        self.stop()   #no use here since the thread hold a ref on this object
    def stop(self):   #need to be called explicitly
        if self.thd_ps:
            self.stopRequestEvent.set()
            self.thd_ps.join()

class OsStats:
    #io_pre=psutil.disk_io_counters(True)
    def __init__(self,path_data):
        self.path_data=path_data
        self.path_wal=path_data+'/pg_xlog/'
        self.disk_data=subprocess.check_output(['df',self.path_data]).split('\n')[1].split()[0]
        self.disk_data_nm=''.join(re.findall('/([^/]+)$',self.disk_data))
        self.disk_wal=subprocess.check_output(['df',self.path_wal]).split('\n')[1].split()[0]
        self.disk_wal_nm=''.join(re.findall('/([^/]+)$',self.disk_wal))
        if has_psutil==False:
            self.tmp_file='/tmp/pgmon_psql_tmp_%s.txt' % datetime.datetime.now().strftime('%Y%m%d%H%M%s%f')
            self.fw_iostat=open(self.tmp_file, "wb")
            self.f_iostat=open(self.tmp_file,'r')
            self.p_iostat = subprocess.Popen(["iostat","-xk","2"], stdout = self.fw_iostat, stderr = self.fw_iostat, bufsize = 1)
            self.re_io=re.compile('\n(\w+(\s+[\d\.]+)+)')
            self.re_cpu=re.compile('\n((\s+[\d\.]+){6})')
            self.iowait=self.idle=0.0
            self.data_rd=self.data_wt=self.data_utl=0.0
            self.wal_rd=self.wal_wt=self.wal_utl=0.0
            self.psstats=PsStats()
    def __del__(self):
        if has_psutil==False:
            self.p_iostat.terminate()
            self.fw_iostat.close()
            self.f_iostat.close()
            subprocess.check_output("rm /tmp/pgmon_psql_tmp_*",shell=True)
            self.psstats.stop()
            del self.psstats
    def update(self):
            self.updateStat()
            self.updateDf()
    def updateStat(self):
        if has_psutil:
            vm=psutil.virtual_memory()
            self.vm_total,self.vm_cached,self.vm_free=vm.total,vm.cached,vm.free
            cpu=psutil.cpu_times_percent()
            self.iowait,self.idle=cpu.iowait,cpu.idle
            io_cur=psutil.disk_io_counters(True)
            disk_nm=self.disk_data.split('/')[2]
            wt_data=io_cur[disk_nm].write_bytes#(io_cur[self.disk_data].write_bytes-io_pre[self.disk_data].write_bytes)/(io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time)*1000/1024/1024 if io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time>0 else 0
            rd_data=io_cur[disk_nm].read_bytes#(io_cur[self.disk_data].read_bytes-io_pre[self.disk_data].read_bytes)/(io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time)*1000/1024/1024 if io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time>0 else 0
            disk_nm=self.disk_wal.split('/')[2]
            wt_wal=io_cur[disk_nm].write_bytes#(io_cur[self.disk_wal].write_bytes-io_pre[self.disk_wal].write_bytes)/(io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time)*1000/1024/1024 if io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time>0 else 0
            rd_wal=io_cur[disk_nm].read_bytes#(io_cur[self.disk_wal].read_bytes-io_pre[self.disk_wal].read_bytes)/(io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time)*1000/1024/1024 if io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time>0 else 0
        else:
            vm=subprocess.check_output(['free']).split('\n')[1].split()
            self.vm_total,self.vm_cached,self.vm_free=long(vm[1])*1024,long(vm[6])*1024,long(vm[3])*1024
            iostat=self.f_iostat.read()
            cs=self.re_cpu.findall(iostat)
            if len(cs)>0:
                self.iowait=avg([float(c[0].split()[3]) for c in cs])
                self.idle=avg([float(c[0].split()[5]) for c in cs])
            ios=self.re_io.findall(iostat)
            if len(ios)>0:
                self.data_rd=avg([float(c[0].split()[5]) for c in ios if c[0].split()[0]==self.disk_data_nm])
                self.data_wt=avg([float(c[0].split()[6]) for c in ios if c[0].split()[0]==self.disk_data_nm])
                self.data_utl=avg([float(c[0].split()[-1]) for c in ios if c[0].split()[0]==self.disk_data_nm])
                self.wal_rd=avg([float(c[0].split()[5]) for c in ios if c[0].split()[0]==self.disk_wal_nm])
                self.wal_wt=avg([float(c[0].split()[6]) for c in ios if c[0].split()[0]==self.disk_wal_nm])
                self.wal_utl=avg([float(c[0].split()[-1]) for c in ios if c[0].split()[0]==self.disk_wal_nm])
    def updateDf(self):
        if has_psutil:
            usage=psutil.disk_usage(self.path_data)
            self.data_used,self.data_percent=usage.used,usage.percent
            usage=psutil.disk_usage(self.path_wal)
            self.wal_used,self.wal_percent=usage.used,usage.percent
        else:
            for df in [d.split() for d in subprocess.check_output(['df']).split('\n')[1:] if len(d)>6]:
                if df[0]==self.disk_data:
                    self.data_used,self.data_percent=long(df[2])*1024,float(df[4][:-1])/100
                if df[0]==self.disk_wal:
                    self.wal_used,self.wal_percent=long(df[2])*1024,float(df[4][:-1])/100
    def getPsStats(self,pid):
        if has_psutil:
            try:
                p=psutil.Process(pid)
                io=p.io_counters()
                return {'cpu':p.cpu_percent(),'mem':p.memory_percent(),'status':p.status(),'read_t':io.read_bytes,'write_t':io.write_bytes}
            except Exception:
                return {'cpu':0,'mem':0,'status':'NA','read_t':0,'write_t':0}
        else:
            return self.psstats.getPsStats(pid)
    def getStorageStats(self):
        return {'disk_data':self.disk_data,'usage_data':bytes2human(self.data_used),'usage_data%':self.data_percent,'read_data_t':self.data_rd,'write_data_t':self.data_wt,'util_data':self.data_utl,
                'disk_wal':self.disk_wal,'usage_wal':bytes2human(self.wal_used),'usage_wal%':self.wal_percent,'read_wal_t':self.wal_rd,'write_wal_t':self.wal_wt,'util_wal':self.wal_utl
                }
    def getCpuStats(self):
        return {'iowait':self.iowait,'idle':self.idle}
    def getMemStats(self):
        def getMemoryOfProcess(p):
            try:
                return reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]), [(m.pss,m.private_clean+m.private_dirty) for m in p.get_memory_maps()])
            except Exception:
                return (0,0)
        if has_psutil:
            ps_postgres=[p for p in psutil.process_iter() if p.name()=='postgres']
            mem_shared,mem_private=reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]),[getMemoryOfProcess(p) for p in ps_postgres if psutil.pid_exists(p.pid)])
        else:
            mem_shared,mem_private=0,0
        return {'total':bytes2human(self.vm_total),'cached':bytes2human(self.vm_cached),'free':bytes2human(self.vm_free),'pg_share':bytes2human(mem_shared),'pg_private':bytes2human(mem_private)}

class StatsListener:
    def __init__(self,statsName,dbName='postgres'):
        self.stats={}
        self.stats_modified=False
        self.stats_tm=None
        self.collector=None
        self.statsName=statsName
        self.dbName=dbName
        self.pid=None
        self.shareLock=threading.Lock()
    def updateStats(self,stats):
        with self.shareLock:
            self.stats=stats
            self.stats_modified=True
            self.stats_tm=datetime.datetime.utcnow()
    def getStats(self):
        with self.shareLock:
            self.stats_modified=False
            return self.stats.copy()
    def setCollector(self,collector):
        with self.shareLock:
            self.collector=collector
    def setActive(self):
        with self.shareLock:
            if self.collector!=None:
                self.collector.setActiveListener(self.statsName)
    def refreshData(self):
        with self.shareLock:
            if self.collector!=None:
                self.collector.refresh()
class StatsCollector:
    shareLock=threading.Lock()
    stopRequestEvent=threading.Event()
    active_statsName = ''
    listners={}
    workerThread=None
    refresh_requested=False
    def getStatsState(self):
        self.stats_os.update()
        last_stats_tm=self.listners[self.active_statsName].stats_tm
        last_stats=self.listners[self.active_statsName].stats
        stats_stg=self.stats_os.getStorageStats()
        if last_stats_tm:
            s0=last_stats['storage']
            dur=(datetime.datetime.utcnow()-last_stats_tm).total_seconds()
            stats_stg['read_data']=(stats_stg['read_data_t']-s0['read_data_t'])/dur/1024
            stats_stg['write_data']=(stats_stg['write_data_t']-s0['write_data_t'])/dur/1024
            stats_stg['read_wal']=(stats_stg['read_wal_t']-s0['read_wal_t'])/dur/1024
            stats_stg['write_wal']=(stats_stg['write_wal_t']-s0['write_wal_t'])/dur/1024
        else:
            stats_stg['read_data']=stats_stg['read_data_t']/1024
            stats_stg['write_data']=stats_stg['write_data_t']/1024
            stats_stg['read_wal']=stats_stg['read_wal_t']/1024
            stats_stg['write_wal']=stats_stg['write_wal_t']/1024
        return {'ver':self.stats_pg.getPgVersion(),'up':self.stats_pg.getPgStartTime(),'cpu':self.stats_os.getCpuStats(),'memory':self.stats_os.getMemStats(),'storage':stats_stg,'streaming_rep':self.stats_pg.getRepStatus()}
    def collectStats(self):
        def updatePsStatsToSession(pid,s):
            s.update(self.stats_os.getPsStats(int(pid)))
            if last_stats_tm and pid in last_stats['session']:
                dur=(datetime.datetime.utcnow()-last_stats_tm).total_seconds()
                s0=last_stats['session'][pid]
                s['read']=(s['read_t']-s0['read_t'])/dur/1024
                s['write']=(s['write_t']-s0['write_t'])/dur/1024
            else:
                s['read']=s['read_t']/1024.0
                s['write']=s['write_t']/1024.0
        #print self.active_statsName
        if self.active_statsName=='index':
            db=self.listners[self.active_statsName].dbName
            stats=self.getStatsState()
            stats['index']=self.stats_pg.getIndexList(db)
            with self.shareLock:
                if self.active_statsName=='index':
                    self.listners[self.active_statsName].updateStats(stats)
        if self.active_statsName=='table':
            db=self.listners[self.active_statsName].dbName
            stats=self.getStatsState()
            stats['table']=self.stats_pg.getTableList(db)
            with self.shareLock:
                if self.active_statsName=='table':
                    self.listners[self.active_statsName].updateStats(stats)
        if self.active_statsName=='db':
            stats=self.getStatsState()
            stats['db']=self.stats_pg.getDbList()
            with self.shareLock:
                #print 'in collecting:'
                if self.active_statsName=='db':
                    self.listners[self.active_statsName].updateStats(stats)
        if self.active_statsName=='session':
            session_list=self.stats_pg.getSessionList()
            last_stats_tm=self.listners[self.active_statsName].stats_tm
            last_stats=self.listners[self.active_statsName].stats
            for pid,s in session_list.iteritems():  #append process stats cpu,mem,io
                updatePsStatsToSession(pid,s)
            stats=self.getStatsState()
            stats['session']=session_list
            with self.shareLock:
                if self.active_statsName=='session':
                    self.listners[self.active_statsName].updateStats(stats)
        if self.active_statsName=='session_detail':
            pid=self.listners[self.active_statsName].pid
            stats=self.getStatsState()
            stats['session_detail']=self.stats_pg.getSessionDetail(pid)
            with self.shareLock:
                if self.active_statsName=='session_detail':
                    self.listners[self.active_statsName].updateStats(stats)
    def __init__(self,updateInterval=600):
        self.stats_pg=PgStats()
        self.stats_os=OsStats(self.stats_pg.getPgPath())
        self.updateInterval=updateInterval
        self.workerThread=threading.Thread(target=self.working)
        self.workerThread.start()
    def working(self):
        sleepInterval=0.5
        sleepDur=0
        active_statsName=''
        while not self.stopRequestEvent.is_set():
            if self.refresh_requested or sleepDur>=self.updateInterval or self.active_statsName!=active_statsName:
                sleepDur=0
                active_statsName=self.active_statsName
                self.refresh_requested=False
                self.collectStats()
            sleepDur+=sleepInterval
            self.stopRequestEvent.wait(sleepInterval)
    def __del__(self):
        self.stop()
    def stop(self):
        if self.workerThread:
            self.stopRequestEvent.set()
            self.workerThread.join()
            self.workerThread=None
        del self.stats_os
        del self.stats_pg
    def addListener(self,listener):
        with self.shareLock:
            self.listners[listener.statsName]=listener
            listener.setCollector(self)
    def setActiveListener(self,statsName):
        with self.shareLock:
            self.active_statsName=statsName
    def refresh(self):
        with self.shareLock:
            self.refresh_requested=True
class BaseView:
    lines=[]
    refresh_required=True
    filter_display=''
    filter_name=''
    msg=''
    order_display=''
    order_name=''
    app=None
    def __init__(self,activeKey):
        self.activeKey=activeKey
    def setWin(self,app):
        self.app=app
        win = self.app.stdscr
        self.height,self.width=win.getmaxyx()
    def updateContent(self):
        self.refresh_required=False
    def updateView(self):
        win = self.app.stdscr
        self.height,self.width=win.getmaxyx()
        self.updateContent()
        win.erase()
        for y,line in enumerate(self.lines[:self.height-1]):
            win.addstr(y,0,line[:self.width])
        if len(self.order_name)>0 or len(self.filter_name)>0 or len(self.msg)>0:
            win.addstr(self.height-1,0,self.filter_display+'\t'+self.order_display+'\t'+self.msg)
    def setMessage(self,message):
        self.msg=message;
        self.updateView()
    def setActive(self):
        self.refresh_required=True
        return True
    def getInput(self,initial=''):
        win=self.app.stdscr
        curses.echo()
        curses.curs_set(1)
        curses.nocbreak()
        win.move(self.height-1,0)
        win.clrtoeol()
        win.addstr(self.height-1,0,initial)
        win.timeout(-1) #disable read timeout
        input=win.getstr(self.height-1,len(initial)+1)
        win.timeout(self.app.loopInterval)
        curses.curs_set(0)
        curses.noecho()
        curses.cbreak()
        return input
    def setFilter(self):
        self.filter_name=self.getInput()
        if len(self.filter_name)>0:
            self.filter_display='filtered by: '+self.filter_name
        else:
            self.filter_display=''
        self.refresh_required=True
    def getSortColumns(self):
        return []
    def setOrder(self):
        options=self.getSortColumns()
        selected=self.getInput(' '.join([str(i)+':'+nm for i,nm in enumerate(options)]))
        if selected.isdigit() and int(selected)<len(options):
            self.order_name=options[int(selected)]
            self.order_display='ordered by: '+self.order_name
        else:
            self.order_display=''
        self.refresh_required=True
    def isSortable(self):
        return True
    def isFiltable(self):
        return True
    def onEvent(self,key):
        return False
class HelpView(BaseView):
    lines=['Help',
            'pgmon: an easy to use monitoring tool for PostgreSql, inspired by Linux\'s top and IBM\'s db2top',
            '',
            'Interactive commands',
            'h: print this screen',
            'd: database view,  list all databases',
            's: session view, list all current sessions',
            '   k: kill/terminate a connection/backend',
            't: table view, list all user tables for a specified database',
            'i: index view, list all user index for a specified database',
            'l: locks view, list all locks for a specified session id',
            'b: background writer view, show stats about background writer',
            '/: filter current view via regex, start with "! " for invert match',
            'r: refresh'
            ]
    def __init__(self):
        BaseView.__init__(self,'h')
    def isSortable(self):
        return False
    def isFiltable(self):
        return False
def formatPgStateLines(stats):
    rep=stats['streaming_rep']
    header=['pgmon - PostgreSQL version:%s,  started at %s  streaming rep mode: %s' % (stats['ver'],stats['up'],rep['rep_mod']),
            'cpu: %5.1f idle, %5.1f iowait,  memory:  %s total,  %s free,  %s cached,  %s pg_share,  %s pg_private' % (stats['cpu']['idle'],stats['cpu']['iowait'],stats['memory']['total'],stats['memory']['free'],stats['memory']['cached'],stats['memory']['pg_share'],stats['memory']['pg_private']),
            'pg_data(%s): %sB/%s%% used,%8.1fread,%8.1fwrite;    pg_wal(%s): %sB/%s%% used,%8.1fread,%8.1fwrite' % (stats['storage']['disk_data'],stats['storage']['usage_data'],stats['storage']['usage_data%']*100,stats['storage']['read_data'],stats['storage']['write_data'],stats['storage']['disk_wal'],stats['storage']['usage_wal'],stats['storage']['usage_wal%']*100,stats['storage']['read_wal'],stats['storage']['write_wal'])
            ]
    if rep['rep_mod']=='master':
        for clt in rep['rep_list']:
            header.append('sync_clt:%s@%s state:%s/%s LSN:%s diffs(sent/flush/replay):%s/%s/%s' % (clt[1],clt[0],clt[3],clt[4],clt[5],clt[6],clt[7],clt[8]))
    elif rep['rep_mod']=='standby':
        header.append('rep: Standby last_xlog_rcv:%s last_xlog_replay:%s last_xlog_replay_time:%s' % (rep['rep_xlog_rcv_loc'],rep['rep_xlog_replay_loc'],rep['rep_xlog_replay_tm']))
    header.append('')
    return header
class IndexView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'i')
        StatsListener.__init__(self,'index')
    def getSortColumns(self):
        return ['tbl_sz','idx_sz','idx_scn','idx_tup_rd']
    def setActive(self):
        db=self.getInput('database name:')
        if len(db)>0:
            self.dbName=db
            StatsListener.setActive(self)
            return BaseView.setActive(self)
        return False
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    indexs=sorted(stats['index'].values(),key=lambda s:s[self.order_name], reverse=True)
                else:
                    indexs=stats['index'].values()
                self.lines = formatPgStateLines(stats) + formatTable(indexs,stats_items['index']['columns'],stats_items['index']['formats'],self.filter_name)
                BaseView.updateContent(self)
class TableView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'t')
        StatsListener.__init__(self,'table')
    def getSortColumns(self):
        return ['tbl_sz','idx_sz','seq_scn','idx_scn','tup_i','tup_u','tup_d','live_tup','dead_tup']
    def setActive(self):
        db=self.getInput('database name:')
        if len(db)>0:
            self.dbName=db
            StatsListener.setActive(self)
            return BaseView.setActive(self)
        return False
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    tables=sorted(stats['table'].values(),key=lambda s:s[self.order_name], reverse=True)
                else:
                    tables=stats['table'].values()
                self.lines = formatPgStateLines(stats) + formatTable(tables,stats_items['table']['columns'],stats_items['table']['formats'],self.filter_name)
                BaseView.updateContent(self)
class SessionDetailView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'l')
        StatsListener.__init__(self,'session_detail')
    def getSortColumns(self):
        return ['tbl_sz','idx_sz','seq_scn','idx_scn','tup_i','tup_u','tup_d','live_tup','dead_tup']
    def setActive(self):
        pid=self.getInput('backend id:')
        if len(pid)>0:
            self.pid=pid
            StatsListener.setActive(self)
            return BaseView.setActive(self)
        return False
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                self.lines = formatPgStateLines(stats)
                if stats['session_detail']!=None:
                    sn=stats['session_detail']['session']
                    self.lines.append('session:'+self.pid)
                    self.lines.append('  '.join(['%s:%s'% (k,sn[k]) for k in ['db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','state']]))
                    self.lines.append('current query: '+' '.join(sn['query'].split()))
                    self.lines.append('')
                    self.lines.append('locks requested by:'+self.pid)
                    self.lines.extend(formatTable(stats['session_detail']['locks'],stats_items['lock']['columns'],stats_items['lock']['formats'],self.filter_name))
                else:
                    self.lines.append('not existing pid %s:' % self.pid)
                BaseView.updateContent(self)
def formatTable(rows,columns,formats,filter_raw):
    invert = (filter_raw[:2]=='! ')
    filter = filter_raw[2:] if invert else filter_raw
    max_lens=[max(len(c),max([0]+[len(('%'+f) % r[c]) for r in rows])) if f!='float_s' else 0 for f,c in zip(formats,columns)]
    return [''.join([('%'+str(l+1)+'s' if l>0 else ' %s') % c for l,c in zip(max_lens,columns)])]+ \
           [l for l in  \
                [ ''.join([('%'+str(l+1)+f if f!='float_s' else ' %s') % r[c] for l,f,c in zip(max_lens,formats,columns)]) for r in rows] \
            if len(filter)==0 or ((re.search(filter,l)!=None)!=invert)]
class SessionView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'s')
        StatsListener.__init__(self,'session')
    def getSortColumns(self):
        return ['cpu','mem','read','write','bknd_age','xact_age','query_age']
    def setActive(self):
        StatsListener.setActive(self)
        return BaseView.setActive(self)
    def onEvent(self,key):
        if key=='k':
            pid=self.getInput('please input the backend_id which you want to terminate: ')
            if pid!='':
                if self.collector.stats_pg.terminateBackend(pid):
                    self.setMessage('backend %s terminated' % pid)
                    self.refreshData()
                else:
                    self.setMessage('backend %s terminate failed' % pid)
            return True
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    sessions=sorted(stats['session'].values(),key=lambda s:s[self.order_name], reverse=True)
                else:
                    sessions=stats['session'].values()
                for s in sessions:
                    s['query']=' '.join(s['query'].split())
                self.lines = formatPgStateLines(stats) + formatTable(sessions,stats_items['session']['columns'],stats_items['session']['formats'],self.filter_name)
                BaseView.updateContent(self)
class DBView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'d')
        StatsListener.__init__(self,'db')
    def getSortColumns(self):
        return ['size','sessions','tps','hit_ratio']
    def setActive(self):
        StatsListener.setActive(self)
        return BaseView.setActive(self)
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    dbs=sorted(stats['db'].values(),key=lambda s:s[self.order_name],reverse=True)
                else:
                    dbs=stats['db'].values()
                self.lines= formatPgStateLines(stats) + formatTable(dbs,stats_items['db']['columns'],stats_items['db']['formats'],self.filter_name)
                BaseView.updateContent(self)

class CursesApp:
    views={}
    currentView=None
    loopInterval=300
    def __init__(self):
        self.stdscr = curses.initscr() #init curses library
        curses.noecho() #disable automatic echoing of keys
        curses.cbreak() #enable react to keys instantly instead of requiring Enter
        self.stdscr.keypad(1) #enable keypad mode
        curses.curs_set(0) #disable blinking cursor
    def exit(self):
        curses.curs_set(1)
        self.stdscr.keypad(0)
        curses.nocbreak()
        curses.echo()
        curses.endwin()
    def addView(self,view):
        key=view.activeKey
        self.views[key]=view
        view.setWin(self)
        if len(self.views)==1:
            self.currentView=view
    def handelKeyEvent(self,key_val):
        if key_val >= 256:
            return
        key=chr(key_val)
        if key == 'q':
            self.running=False
        elif key == '/' and self.currentView.isFiltable(): # start regex input
            self.currentView.setFilter()
        elif key == 'o' and self.currentView.isSortable(): # start sorting select
            self.currentView.setOrder()
        elif key == 'r': #manual data refresh
            if isinstance(self.currentView,StatsListener):
                self.currentView.refreshData()
        elif key in self.views:
            self.setActiveView(key)
        else:
            self.currentView.onEvent(key)
    def setActiveView(self,view_name):
        if view_name in self.views and self.views[view_name].setActive():
            self.currentView=self.views[view_name]

    def refreshScreen(self):
        self.stdscr.noutrefresh()
        self.currentView.updateView()
        curses.doupdate()
    def run(self):
        self.running=True
        self.stdscr.timeout(self.loopInterval) #delay every loopInterval miliseconds
        #self.stdscr.nodelay(1)
        #curses.halfdelay(5)
        while self.running==True:
            c = self.stdscr.getch()
            if c != curses.ERR :
                self.handelKeyEvent(c)
            self.refreshScreen()

class PgMonApp(CursesApp,StatsCollector):
    stats_views={}
    def __init__(self):
        StatsCollector.__init__(self)
        CursesApp.__init__(self)
        self.addStatsView(HelpView())
        self.addStatsView(DBView())
        self.addStatsView(SessionView())
        self.addStatsView(TableView())
        self.addStatsView(IndexView())
        self.addStatsView(SessionDetailView())
    def addStatsView(self,view):
        self.stats_views[view.activeKey]=view
        CursesApp.addView(self,view)
        if isinstance(view,StatsListener):
            StatsCollector.addListener(self,view)
    def run(self):
        CursesApp.run(self)
        StatsCollector.stop(self)
        CursesApp.exit(self)


if __name__ == '__main__':
    os.chdir('/tmp') #change current working directory to /tmp
    app=PgMonApp()
    def signal_handler(signal,frame):
        print('Ctrl+C received! exiting..')
        app.running=False
    hdl_old=signal.signal(signal.SIGINT,signal_handler)
    app.run()
