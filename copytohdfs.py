import subprocess

local_folder_path = '/home/elshacgr/taxidata'
hdfs_folder_path = '/user/elshacgr/data'

subprocess.run(['hadoop', 'fs', '-put', local_folder_path, hdfs_folder_path])
