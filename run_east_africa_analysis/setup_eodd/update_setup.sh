#!/usr/bin/env bash

singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif eoddsetup.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json --update
