#!/usr/bin/python
import subprocess
import os
import datetime
import re
import threading
import signal
import curses
import psycopg2
import psutil

stats_items={
    'db': {'columns':['database','size','session','xact_commit','tps','blks_hit','blks_read','hit%','tup_iud','tup_returned'],
            'formats':['%12s%8s%8s%18s%10.1f%18s%18s%9s%18s%18s']
           },
    'session':{'columns':['pid','cpu','mem','read','write','db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','blking_id','state','query'],
                'formats':['s','.1f','.1f','.1f','.1f','s','s','s','s','s','s','s','s','s','float_s']
              },
    'table': {'columns':['tbl_id','scm','tbl','tbl_sz','idx_sz','frzxid','seq_scn','idx_scn','tup_i','tup_u','tup_d','live_tp','dead_tp','lst_autovcm','lst_autoanz','a_vcm_n','a_anz_n'],
            'formats':['s','s','s','s','s','s','s','s','s','s','s','s','s','s','s','s']
            },
    'index':{'columns':['scm_id','scm','tbl','idx','tbl_sz','idx_sz','idx_scn','idx_tup_rd'],
            'formats':['s','s','s','s','s','s','s','s']
            }
    }
    
class PgStats:
    sqls_all_vertions={
        '8.4':{
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
                select procpid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,'' as state,current_query as query 
                from pg_stat_activity s 
                left outer join lw on s.procpid=lw.waiting_id
                """,
            'table_list':
                """
                select st.relid,schemaname as scm, relname as tbl,  relpages,indpages,relfrozenxid,seq_scan,idx_scan,n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup,last_autovacuum::timestamp(0) as lst_autovcm, last_autoanalyze::timestamp(0) as lst_autoanz,autovacuum_count as autovcm_n,autoanalyze_count as  autoanz_n 
                    from pg_stat_user_tables st, 
                ( select relid,t.relpages+coalesce(ts.relpages,0)+coalesce(ti.relpages,0) as relpages,indpages,t.relfrozenxid 
                    from  
                      (select oid as relid,relpages::bigint,relfrozenxid,reltoastrelid from pg_class where relkind='r') t 
                      left outer join (select sum(relpages) as indpages,indrelid from pg_class i, pg_index r where relkind='i' and i.oid=r.indexrelid group by indrelid) i on t.relid=i.indrelid 
                      left outer join pg_class ts on t.reltoastrelid=ts.oid
                      left outer join pg_class ti on ts.reltoastidxid=ti.oid
                )p 
                where st.relid=p.relid
                """,
            'index_list':"""
                select indexrelid,schemaname as scm,st.relname as tbl,indexrelname as idx,r.relpages as tbl_sz, i.relpages as idx_sz, idx_scan,idx_tup_read 
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
                select pid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,(now()-query_start)::interval(0) query_age,lw.blocking_id,state,query as query 
                from pg_stat_activity s 
                left outer join lw on s.pid=lw.waiting_id
                """,
            'table_list':
                """select st.relid,schemaname as scm, relname as tbl,  relpages,indpages,relfrozenxid,seq_scan,idx_scan,n_tup_ins,n_tup_upd,n_tup_del,n_live_tup,n_dead_tup,last_autovacuum::timestamp(0) as lst_autovcm, last_autoanalyze::timestamp(0) as lst_autoanz,autovacuum_count as autovcm_n,autoanalyze_count as  autoanz_n 
                    from pg_stat_user_tables st, 
                ( select relid,t.relpages+coalesce(ts.relpages,0)+coalesce(ti.relpages,0) as relpages,indpages,t.relfrozenxid 
                    from  
                      (select oid as relid,relpages::bigint,relfrozenxid,reltoastrelid from pg_class where relkind='r') t 
                      left outer join (select sum(relpages) as indpages,indrelid from pg_class i, pg_index r where relkind='i' and i.oid=r.indexrelid group by indrelid) i on t.relid=i.indrelid 
                      left outer join pg_class ts on t.reltoastrelid=ts.oid
                      left outer join pg_class ti on ts.reltoastidxid=ti.oid
                )p 
                where st.relid=p.relid
                """,
            'index_list':"""
                select indexrelid,schemaname as scm,st.relname as tbl,indexrelname as idx,r.relpages as tbl_sz, i.relpages as idx_sz, idx_scan,idx_tup_read 
                from pg_stat_user_indexes st, pg_class r, pg_class i 
                where st.relid=r.oid and st.indexrelid=i.oid
                """
        }
    }
    db_list={}
    pg_settings={}
    #i=0
    dbname=''
    def getSqlResult(self,sql,db=None):
        if db!=None:
            self.connect(db)
        self.cur.execute(sql)
        return self.cur.fetchall()
    def __init__(self):
        self.connect('postgres')
        self.sqls=self.sqls_all_vertions[re.findall('\d+\.\d+',self.getPgVersion())[0]]
        self.getRepMode()
    def connect(self,dbName):
        if self.dbname!=dbName:
            self.conn= psycopg2.connect('dbname='+dbName)
            self.conn.set_session(isolation_level='READ COMMITTED', readonly=True, deferrable=None, autocommit=True)
            self.cur=self.conn.cursor()
            self.dbname=dbName            
    def getRepMode(self):
        rep_mod=''
        if float(re.findall('(\d+\.\d+)',self.getPgVersion())[0]) > 9:
            sql_rep_mode="select pg_is_in_recovery()::text,(select count(1) from pg_stat_replication) as rep_cnt"
            rs_rep_mod=self.getSqlResult(sql_rep_mode)
            if len(rs_rep_mod)>0:
                pg_is_in_recovery,rep_cnt=rs_rep_mod[0]
                if pg_is_in_recovery=='false' and rep_cnt>0:
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
            for p in psutil.process_iter():
                cmds=p.cmdline()
                if p.name()=='postgres' and len(re.findall('\-D ([^ ]+) ',' '.join(cmds)))>0:
                    path_bin=re.sub('/[^/]+$','',cmds[0])
                    path_data=re.findall('\-D ([^ ]+) ',' '.join(cmds))[0]    
                    break
            if path_data=='':
                raise Exception('No postgres processes founed, please make sure postgres is running!')
            #self.cur.execute("select setting from pg_settings where name='data_directory'")
            #path_data=self.cur.fetchone()[0]
            self.pg_settings['data_directory']=path_data
            self.pg_settings['bin_directory']=path_bin
        return self.pg_settings['data_directory']
    def getPgVersion(self):
        if 'version' not in self.pg_settings:
            pg_data=self.getPgPath()
            pg_ver=''.join(open(pg_data+'/PG_VERSION').readline().split())
            #self.cur.execute("select substring(version() from 'PostgreSQL ([\d\.]+) on')")
            #pg_ver=self.cur.fetchone()[0]
            self.pg_settings['version']=pg_ver
        return self.pg_settings['version']
    def getPgStartTime(self):
        if 'start_time' not in self.pg_settings:
            self.cur.execute("SELECT pg_postmaster_start_time()::timestamp(0)")
            start_time=self.cur.fetchone()[0]
            self.pg_settings['start_time']=start_time
        return self.pg_settings['start_time']
    def getTableList(self,db):
        self.connect(db)
        self.cur.execute(self.sqls['table_list'])
        tl={}
        for r in self.cur.fetchall():
            tl[r[0]]=dict(zip(stats_items['table']['columns'],r))
        return tl
    def getIndexList(self,db):
        self.connect(db)
        self.cur.execute(self.sqls['index_list'])
        il={}
        for r in self.cur.fetchall():
            il[r[0]]=dict(zip(stats_items['index']['columns'],r))
        return il   
    def getSessionList(self):
        self.cur.execute(self.sqls['session_list'])
        sl={}
        for r in self.cur.fetchall():
            sl[r[0]]=dict(zip(['pid','db','user','clt_app','clt_addr','bknd_age','xact_age','query_age','blking_id','state','query'],r))
        return sl
    def getDbList(self):
        self.cur.execute(self.sqls['db_list'])
        #self.i+=1
        db_list={}#'i':self.i}
        for db in self.cur.fetchall():
            db_list[db[1]]={'snap_tm':db[0],'size':db[2],'sessions':db[3],'xact_commit':db[4],'xact_rollback':db[5],'blks_hit':db[6],'blks_read':db[7],'tup_iud':db[8],'tup_returned':db[9]}
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
        ps=[p for p in psutil.process_iter() if ''.join(p.cmdline()).startswith('postgres: archiver process')]
        status=''
        lag=0
        if len(ps)>0:
            status=re.findall('postgres: archiver process +(.+)',''.join(ps[0].cmdline()))[0]
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
class OsStats:
    #io_pre=psutil.disk_io_counters(True)
    def __init__(self,path_data):
        self.path_data=path_data
        self.path_wal=path_data+'/pg_xlog/'
        self.disk_data=re.findall('/dev/(\w+) ',subprocess.check_output(['df',self.path_data]))[0]
        self.disk_wal=re.findall('/dev/(\w+) ',subprocess.check_output(['df',self.path_wal]))[0]
    def getPsStats(self,pid):
        stat={}
        if psutil.pid_exists(pid):
            p=psutil.Process(pid)
            io=p.io_counters()
            stat={'cpu':p.cpu_percent(),'mem':p.memory_percent(),'status':p.status(),'read_t':io.read_bytes,'write_t':io.write_bytes}
        return stat

    def getStorageStats(self):
        usage_data=psutil.disk_usage(self.path_data)
        usage_wal=psutil.disk_usage(self.path_wal)
        #io_pre=self.io_pre
        io_cur=psutil.disk_io_counters(True)
        wt_data=io_cur[self.disk_data].write_bytes#(io_cur[self.disk_data].write_bytes-io_pre[self.disk_data].write_bytes)/(io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time)*1000/1024/1024 if io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time>0 else 0
        rd_data=io_cur[self.disk_data].read_bytes#(io_cur[self.disk_data].read_bytes-io_pre[self.disk_data].read_bytes)/(io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time)*1000/1024/1024 if io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time>0 else 0
        wt_wal=io_cur[self.disk_wal].write_bytes#(io_cur[self.disk_wal].write_bytes-io_pre[self.disk_wal].write_bytes)/(io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time)*1000/1024/1024 if io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time>0 else 0
        rd_wal=io_cur[self.disk_wal].read_bytes#(io_cur[self.disk_wal].read_bytes-io_pre[self.disk_wal].read_bytes)/(io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time)*1000/1024/1024 if io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time>0 else 0
        #self.io_pre=io_cur
        return {'disk_data':self.disk_data,'usage_data':bytes2human(usage_data.used),'usage_data%':usage_data.percent,'read_data_t':rd_data,'write_data_t':wt_data,
                'disk_wal':self.disk_wal,'usage_wal':bytes2human(usage_wal.used),'usage_wal%':usage_wal.percent,'read_wal_t':rd_wal,'write_wal_t':wt_wal}
    def getCpuStats(self):
        cpu=psutil.cpu_times_percent()
        return {'iowait':cpu.iowait,'idle':cpu.idle}
    def getMemStats(self):
        def getMemoryOfProcess(p):
            try:
                return reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]), [(m.pss,m.private_clean+m.private_dirty) for m in p.get_memory_maps()]) 
            except Exception:
                return (0,0)
        ps_postgres=[p for p in psutil.process_iter() if p.name()=='postgres']
        mem_shared,mem_private=reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]),[getMemoryOfProcess(p) for p in ps_postgres if psutil.pid_exists(p.pid)])
        vm=psutil.virtual_memory()
        return {'total':bytes2human(vm.total),'cached':bytes2human(vm.cached),'free':bytes2human(vm.free),'pg_share':bytes2human(mem_shared),'pg_private':bytes2human(mem_private)}
class StatsListener:
    statsName=''
    dbName=''
    stats={}
    stats_modified=False
    shareLock=threading.Lock()
    stats_tm=None
    def __init__(self,statsName,dbName='postgres'):
        self.statsName=statsName
        self.dbName=dbName
    def updateStats(self,stats):
        with self.shareLock:
            self.stats=stats
            self.stats_modified=True
            self.stats_tm=datetime.datetime.utcnow()
    def getStats(self):
        with self.shareLock:
            self.stats_modified=False
            return self.stats.copy()
class StatsCollector:
    shareLock=threading.Lock()
    stopRequestEvent=threading.Event()
    active_statsName = ''
    listners={}
    workerThread=None
    def getStatsState(self):
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
                s.update(self.stats_os.getPsStats(pid))
                if last_stats_tm and pid in last_stats['session']:
                    dur=(datetime.datetime.utcnow()-last_stats_tm).total_seconds()
                    s0=last_stats['session'][pid]
                    s['read']=(s['read_t']-s0['read_t'])/dur/1024
                    s['write']=(s['write_t']-s0['write_t'])/dur/1024
                else:
                    s['read']=s['read_t']/1024.0
                    s['write']=s['write_t']/1024.0                
            stats=self.getStatsState()
            stats['session']=session_list
            with self.shareLock:
                if self.active_statsName=='session':
                    self.listners[self.active_statsName].updateStats(stats)
    def __init__(self,updateInterval=2):        
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
            if sleepDur>=self.updateInterval or self.active_statsName!=active_statsName:
                sleepDur=0
                active_statsName=self.active_statsName
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
    def addListener(self,listener):
        with self.shareLock:
            self.listners[listener.statsName]=listener
    def setActiveListener(self,statsName):        
        with self.shareLock:
            self.active_statsName=statsName



class BaseView:
    lines=[]
    refresh_required=True
    filter_display=''
    filter_name=''
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
        if len(self.order_name)>0 or len(self.filter_name)>0:
            win.addstr(self.height-1,0,self.filter_display+'\t'+self.order_display)
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
    def isSortable(self):
        return True
    def isFiltable(self):
        return True
class HelpView(BaseView):
    lines=['Help',
            'pgmon: an easy to use monitoring tool for PostgreSql, inspired by Linux\'s top and IBM\'s db2top',
            '',
            'Interactive commands',
            'h: print this screen',
            'd: database view,  list all databases',
            's: session view, list all current sessions'
            't: table view, list all user tables',
            'i: index view, list all user index',
            'b: background writer view, show stats about background writer']
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
            'pg_data(%s): %sB/%s%% used,%8.1fread,%8.1fwrite;    pg_wal(%s): %sB/%s%% used,%8.1fread,%8.1fwrite' % (stats['storage']['disk_data'],stats['storage']['usage_data'],stats['storage']['usage_data%'],stats['storage']['read_data'],stats['storage']['write_data'],stats['storage']['disk_wal'],stats['storage']['usage_wal'],stats['storage']['usage_wal%'],stats['storage']['read_wal'],stats['storage']['write_wal'])
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
            return BaseView.setActive(self)
        return False
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    indexs=sorted(stats['index'].items(),key=lambda s:s[1][self.order_name], reverse=True)
                else:
                    indexs=stats['index'].items()
                self.lines = formatPgStateLines(stats) + formatTable(indexs,stats_items['index']['columns'],stats_items['index']['formats'],self.filter_name)
                BaseView.updateContent(self)            
class TableView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'t')
        StatsListener.__init__(self,'table')    
    def getSortColumns(self):
        return ['tbl_sz','idx_sz','seq_scan','idx_scan','n_dead_tup']
    def setActive(self):
        db=self.getInput('database name:')
        if len(db)>0:
            self.dbName=db
            return BaseView.setActive(self)
        return False
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    tables=sorted(stats['table'].items(),key=lambda s:s[1][self.order_name], reverse=True)
                else:
                    tables=stats['table'].items()
                self.lines = formatPgStateLines(stats) + formatTable(tables,stats_items['table']['columns'],stats_items['table']['formats'],self.filter_name)
                BaseView.updateContent(self)    
def formatTable(rows,columns,formats,filter):
    max_lens=[max(len(c),max([0]+[len(('%'+f) % r[c]) for id,r in rows])) if f!='float_s' else 0 for f,c in zip(formats,columns)]
    return [''.join([('%'+str(l+1)+'s' if l>0 else ' %s') % c for l,c in zip(max_lens,columns)])]+ \
           [l for l in  \
                [ ''.join([('%'+str(l+1)+f if f!='float_s' else ' %s') % r[c] for l,f,c in zip(max_lens,formats,columns)]) for id,r in rows] \
            if len(filter)==0 or re.search(filter,l)]
class SessionView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'s')
        StatsListener.__init__(self,'session')    
    def getSortColumns(self):
        return ['cpu','mem','read','write','bknd_age','xact_age','query_age']
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    sessions=sorted(stats['session'].items(),key=lambda s:s[1][self.order_name], reverse=True)
                else:
                    sessions=stats['session'].items()
                for s in sessions:
                    s[1]['query']=' '.join(s[1]['query'].split())
                self.lines = formatPgStateLines(stats)+ formatTable(sessions,stats_items['session']['columns'],stats_items['session']['formats'],self.filter_name)
                BaseView.updateContent(self)    
class DBView(BaseView,StatsListener):
    def __init__(self):
        BaseView.__init__(self,'d')
        StatsListener.__init__(self,'db')
    def getSortColumns(self):
        return ['size','sessions','tps','hit_ratio']
    def updateContent(self):
        if self.stats_modified or self.refresh_required:
            stats=self.getStats()
            if 'ver' in stats:
                if self.order_name!='':
                    dbs=sorted(stats['db'].items(),key=lambda s:s[1][self.order_name],reverse=True)
                else:
                    dbs=stats['db'].items()
                self.lines= \
                    formatPgStateLines(stats) +\
                    ['%12s%8s%8s%18s%10s%18s%18s%9s%18s%18s' % ('database','size','session','xact_commit','tps','blks_hit','blks_read','hit%','tup_iud','tup_returned')] + \
                    ['%12s%8s%8s%18s%10.1f%18s%18s%9s%18s%18s' % (db,bytes2human(ms['size']),ms['sessions'],ms['xact_commit'],ms['tps'],ms['blks_hit'],ms['blks_read'],str(ms['hit_ratio_delt'])+'/'+str(ms['hit_ratio']),ms['tup_iud'],ms['tup_returned']) \
                        for db,ms in dbs if len(self.filter_name)==0 or re.search(self.filter_name,db)
                    ]
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
        elif key in self.views:
            self.setActiveView(key)
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
    def addStatsView(self,view):
        self.stats_views[view.activeKey]=view
        CursesApp.addView(self,view)
        if isinstance(view,StatsListener):
            StatsCollector.addListener(self,view)
    def setActiveView(self,view_name):
        CursesApp.setActiveView(self,view_name)
        if isinstance(self.stats_views[view_name],StatsListener):
            StatsCollector.setActiveListener(self,self.stats_views[view_name].statsName)
        else:
            StatsCollector.setActiveListener(self,'')
    def run(self):
        CursesApp.run(self)
        StatsCollector.stop(self)
        CursesApp.exit(self)
        
        
if __name__ == '__main__':
    app=PgMonApp()
    def signal_handler(signal,frame):
        print('Ctrl+C received! exiting..')
        app.running=False
    signal.signal(signal.SIGINT,signal_handler)
    app.run()
            
