import os,sys,re
import shlex,subprocess,glob

#os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/"

test_dir = PATH +"/" + sorted(glob.glob("tests-*"),key=os.path.getmtime,reverse=True)[0]
os.chdir(test_dir)
names_ = "*"
names = sorted(glob.glob(names_),key=os.path.getmtime)

for name in names:
    os.chdir(name)
    print(os.getcwd())
# Execute
    m = re.search("NODE_CNT-(\d*)_",name)
    if m: nnodes = int(m.group(1))
    else: sys.exit()
    pids = []
    for n in range(nnodes):
        cmd = "env GRAPHITE_HOME=\"{}\" ./rundb -nid{}".format(PATH,n)
        print(cmd)
        cmd = shlex.split(cmd)
        ofile_n = "{}_{}.out".format(n,name)
        ofile = open(ofile_n,'w')
        #cmd = "./rundb -nid{} >> {}_{}.out &".format(n,n,name)
        p = subprocess.Popen(cmd,stdout=ofile)
        pids.insert(0,p)
    for n in range(nnodes):
        pids[n].wait()
    os.chdir(test_dir)

