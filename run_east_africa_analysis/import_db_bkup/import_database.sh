
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddimportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s LandsatGOOG -i /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_LandsatGOOG_20200609.json
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddimportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel1ASF -i /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_Sentinel1ASF_20200609.json
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddimportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel2GOOG -i /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_Sentinel2GOOG_20200609.json
