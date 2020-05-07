#!/usr/bin/env bash

singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif python gen_process_cmds.py --gen

sh ./gen_exe_hpccmds.sh

# Once all processing has completed...
#singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif python gen_process_cmds.py --check
