import os

#os.chdir('..')

PATH=os.getcwd()

cmd = "scp rhardin@schroedinger.csail.mit.edu:/home/rhardin/research/ddbms/distDBX/tests.tgz ."
os.system(cmd)

cmd = "tar -xvf tests.tgz"
os.system(cmd)
