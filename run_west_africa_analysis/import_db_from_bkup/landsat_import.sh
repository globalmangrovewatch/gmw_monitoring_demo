
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddresetimgs.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json -s LandsatGOOG --cleartab
singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-py-dev.sif eoddimportdb.py -c /scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json -s LandsatGOOG -i /scratch/a.pfb/gmw_monitoring/db_bkup/bkup_wa_LandsatGOOG_20200515.json

