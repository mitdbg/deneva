import os,glob

user="rhardin"
machine = "schroedinger.csail.mit.edu"
project_dir = "/home/rhardin/research/ddbms/distDBX/"

PATH=os.getcwd()
result_dir = PATH + "/results/"

test_dir_name = sorted(glob.glob("tests-*"),key=os.path.getmtime,reverse=True)[0]
test_dir = PATH +"/" + test_dir_name

cmd = "tar -czvf {}.tgz {}".format(test_dir_name,test_dir_name)
os.system(cmd)
cmd = "scp {}.tgz {}@{}:{}results/".format(test_dir_name,user,machine,project_dir)
os.system(cmd)

