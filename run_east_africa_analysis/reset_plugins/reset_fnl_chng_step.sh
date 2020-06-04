singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s LandsatGOOG --usranalysis --plugin LandsatGMWChangeFnl
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel1ASF --usranalysis --plugin Sentinel1GMWChangeFnl
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel2GOOG --usranalysis --plugin Sentinel1GMWChangeFnl

singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s LandsatGOOG --usranalysis --plugin UploadToGoog
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel1ASF --usranalysis --plugin UploadToGoog
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel2GOOG --usranalysis --plugin UploadToGoog


