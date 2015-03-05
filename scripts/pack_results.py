import os,glob

#os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/"

test_dir_name = sorted(glob.glob("tests-*"),key=os.path.getmtime,reverse=True)[0]
test_dir = PATH +"/" + test_dir_name

cmd = "tar -czvf {}.tgz {}".format(test_dir_name,test_dir_name)
os.system(cmd)
cmd = "scp {}.tgz rhardin@schroedinger.csail.mit.edu:/home/rhardin/research/ddbms/distDBX/results/".format(test_dir_name)
os.system(cmd)

