#!/usr/bin/python

import subprocess,datetime,re
try:
    import psutil
except ImportError:
        has_psutil=False
        
has_psutil=False   
def avg(list):
    return sum(list)/len(list)
class OsStats:
    #io_pre=psutil.disk_io_counters(True)
    def __init__(self,path_data):
        self.path_data=path_data
        self.path_wal=path_data+'/pg_xlog/'
        self.disk_data=subprocess.check_output(['df',self.path_data]]).split('\n')[1].split()[0]
        self.disk_wal=subprocess.check_output(['df',self.path_wal]).split('\n')[1].split()[0]
        if has_psutil==False:
            self.tmp_file='/tmp/pgmon_psql_tmp_%s.txt' % datetime.datetime.now().strftime('%Y%m%d%H%M%s%f')
            self.fw_iostat=open(tmp_file, "wb")
            self.f_iostat=open(tmp_file,'r')
            self.p_iostat = subprocess.Popen(["iostat","-xk","2"], stdout = self.fw_iostat, stderr = self.fw_iostat, bufsize = 1)
            self.re_io=re.compile('\n(\w+(\s+[\d\.]+)+)')
            self.re_cpu=re.compile('\n((\s+[\d\.]+){6})')
            self.iowait=self.idle=0.0
            self.data_rd=self.data_wt=self.data_utl=0.0
            self.wal_rd=sel.wal_wt=self.wal_utl=0.0
    def __del__(self):
        if has_psutil==False:
            self.p_iostat.terminate()
            self.fw_iostat.close()
            self.f_iostat.close()
    def update(self):
            self.updateStat()
            self.updatePs()
            self.updateDf()
    def updateStat(self):
        if has_psutil==False:
            vm=psutil.virtual_memory()
            self.vm_total,self.vm_cached,self.vm_free=vm.total,vm.cached,vm.free
            cpu=psutil.cpu_times_percent()
            self.iowait,self.idle=cpu.iowait,cpu.idle
            io_cur=psutil.disk_io_counters(True)
            wt_data=io_cur[self.disk_data].write_bytes#(io_cur[self.disk_data].write_bytes-io_pre[self.disk_data].write_bytes)/(io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time)*1000/1024/1024 if io_cur[self.disk_data].write_time-io_pre[self.disk_data].write_time>0 else 0
            rd_data=io_cur[self.disk_data].read_bytes#(io_cur[self.disk_data].read_bytes-io_pre[self.disk_data].read_bytes)/(io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time)*1000/1024/1024 if io_cur[self.disk_data].read_time-io_pre[self.disk_data].read_time>0 else 0
            wt_wal=io_cur[self.disk_wal].write_bytes#(io_cur[self.disk_wal].write_bytes-io_pre[self.disk_wal].write_bytes)/(io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time)*1000/1024/1024 if io_cur[self.disk_wal].write_time-io_pre[self.disk_wal].write_time>0 else 0
            rd_wal=io_cur[self.disk_wal].read_bytes#(io_cur[self.disk_wal].read_bytes-io_pre[self.disk_wal].read_bytes)/(io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time)*1000/1024/1024 if io_cur[self.disk_wal].read_time-io_pre[self.disk_wal].read_time>0 else 0
        else:
            vm=subprocess.check_output(['free']).split('\n')[1].split()
            self.vm_total,self.vm_cached,self.vm_free=long(vm[1]),long(vm[6]),long(vm[3])
            iostat=self.f_iostat.read()
            cs=self.re_cpu.findall(iostat)
            if len(cs)>0:
                self.iowait=avg([float(c[0].split()[3]) for c in cs])
                self.idle=avg([float(c[0].split()[5]) for c in cs])
            ios=self.re_io.findall(iostat)
            if len(ios)>0:
                self.data_rd=avg([float(c[0].split()[5]) for c in ios if c[0].split()[0]==self.disk_data])
                self.data_wt=avg([float(c[0].split()[6]) for c in ios if c[0].split()[0]==self.disk_data])
                self.data_utl=avg([float(c[0].split()[-1]) for c in ios if c[0].split()[0]==self.disk_data])
                self.wal_rd=avg([float(c[0].split()[5]) for c in ios if c[0].split()[0]==self.disk_wal])
                self.wal_wt=avg([float(c[0].split()[6]) for c in ios if c[0].split()[0]==self.disk_wal])
                self.wal_utl=avg([float(c[0].split()[-1]) for c in ios if c[0].split()[0]==self.disk_wal])    
    def updatePs(self):
        self.ps=[p.split() for p in subprocess.check_output(['ps','aux']).split('\n')]
    def udpateDf(self):
        if has_psutil:
            usage=psutil.disk_usage(self.path_data)
            self.data_used,self.data_percent=usage.used,usage.percent
            usage=psutil.disk_usage(self.path_wal)
            self.wal_used,self.wal_percent=usage.used,usage.percent
        else:
            for df in [d.split() for d in subprocess.check_output(['df']).split('\n')[1:]]:
                if d[0]==self.disk_data:
                    self.data_used,self.data_percent=long(df[9])),float(df[11][:-1])/100
                if d[0]==self.disk_wal:
                    self.wal_used,self.wal_percent=long(df[9])),float(df[11][:-1])/100
    def getPsStats(self,pid):
        try:
            if has_psutil:
                p=psutil.Process(pid)
                io=p.io_counters()
                return {'cpu':p.cpu_percent(),'mem':p.memory_percent(),'status':p.status(),'read_t':io.read_bytes,'write_t':io.write_bytes}
            else:
                for p in self.ps:
                    if long(p[1])==pid:
                        return {'cpu':float(p[2]),'mem':float(p[3]),'status':p[7],'read_t':0,'write_t':0}
        except Exception:
            return {'cpu':0,'mem':0,'status':'NA','read_t':0,'write_t':0}
    def getStorageStats(self):
        return {'disk_data':self.disk_data,'usage_data':bytes2human(self.data_used),'usage_data%':self.data_percent,'read_data_t':self.data_rd,'write_data_t':self.data_wt,'util_data':self.data_utl,
                'disk_wal':self.disk_wal,'usage_wal':bytes2human(sel.fwal_used),'usage_wal%':self.wal_percent,'read_wal_t':self.wal_rd,'write_wal_t':self.wal_wt,'util_wal':self.wal_utl
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
        
        