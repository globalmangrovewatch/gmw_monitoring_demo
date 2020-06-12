singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json -s LandsatGOOG --usranalysis
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json -s Sentinel1ASF --usranalysis
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json -s Sentinel2GOOG --usranalysis


