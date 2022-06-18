#!/usr/bin/env bash

export http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"
export https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"

singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/gmw-monitor-img.sif python gen_process_cmds.py --gen

#sh ./run_exe_analysis.sh

# Once all processing has completed...
#singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/gmw-monitor-img.sif python gen_process_cmds.py --check

