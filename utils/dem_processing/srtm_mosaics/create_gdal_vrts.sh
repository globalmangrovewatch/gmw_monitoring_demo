#!/usr/bin/env bash

singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb \
/scratch/a.pfb/sw_imgs/au-eoed-dev.sif gdalbuildvrt /scratch/a.pfb/SRTM/srtm_min_val_1_global_mosaic_1arc_v3.vrt \
/scratch/a.pfb/SRTM/min_val_1_tiles/*.kea


singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb \
/scratch/a.pfb/sw_imgs/au-eoed-dev.sif gdalbuildvrt /scratch/a.pfb/SRTM/srtm_min_val_0_global_mosaic_1arc_v3.vrt \
/scratch/a.pfb/SRTM/min_val_0_tiles/*.kea


