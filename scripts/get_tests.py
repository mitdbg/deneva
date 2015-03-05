import os

#os.chdir('..')

PATH=os.getcwd()

user="rhardin"
machine = "schroedinger.csail.mit.edu"
project_dir = "/home/rhardin/research/ddbms/distDBX/"

cmd = "scp {}@{}:{}tests.tgz .".format(user,machine,project_dir)
os.system(cmd)

cmd = "tar -xvf tests.tgz"
os.system(cmd)
