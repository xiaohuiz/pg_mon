from subprocess import Popen, PIPE
tmp_file='/tmp/pgmon_psql_tmp_%s.txt' % datetime.datetime.now().strftime('%Y%m%d%H%M%s%f')

fw = open(tmp_file, "wb")
fr = open(tmp_file, "r")
p_iostat = Popen(["iostat","-xk","2"], stdin = PIPE, stdout = fw, stderr = fw, bufsize = 1)

p.stdin.write("1\n")
out = fr.read()
p.stdin.write("5\n")
out = fr.read()
fw.close()
fr.close()


lines=str.split('\n')
re.findall('\s+([\d\.]+)',lines[1])
re.findall('\s+([\d\.]+)',lines[4])