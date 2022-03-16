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
                select procpid as backend_id, datname as db, usename as user,application_name as clt_app, client_addr as clt_addr,(now()-backend_start)::interval(0) backend_age,(now()-xact_start)::interval(0) as xact_age,case when state='active' then (now()-query_start)::interval(0) else null end as query_age,lw.blocking_id,locks,'' as state,replace(replace(current_query,E'\n',''),E'\t','') as query
                from pg_stat_activity s
                left outer join lw on s.procpid=lw.waiting_id
                left outer join (select pid,count(1) as locks from pg_locks group by pid) lc on s.procpid=lc.pid
                where s.procpid<>pg_backend_pid()
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