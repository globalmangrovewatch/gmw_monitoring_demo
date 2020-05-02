import os
import subprocess
import glob
from multiprocessing import Pool

def unzip_file(param):
    zip_file = os.path.abspath(param[0])
    out_dir = os.path.abspath(param[1])
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    c_pwd = os.getcwd()
    os.chdir(out_dir)
    cmd = "unzip '{}'".format(zip_file)
    print("Running '{}' output directory: '{}'".format(cmd, out_dir))
    subprocess.call(cmd, shell=True)
    os.chdir(c_pwd)

srtm_files = glob.glob("/scratch/a.pfb/SRTM/RAW/*.zip")

run_params = []
for srtm_file in srtm_files:
    run_params.append([srtm_file, '/scratch/a.pfb/SRTM/extracted'])

p = Pool(25)
p.map(unzip_file, run_params)


