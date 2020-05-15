
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddexportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s LandsatGOOG -o /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_LandsatGOOG_$(date +'%Y%m%d').json
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddexportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel1ASF -o /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_Sentinel1ASF_$(date +'%Y%m%d').json
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddexportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json -s Sentinel2GOOG -o /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_ea_Sentinel2GOOG_$(date +'%Y%m%d').json
