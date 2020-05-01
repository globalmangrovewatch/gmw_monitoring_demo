import glob
import os
import rsgislib.imageutils.imagelut


out_scores_dir = '/scratch/a.pfb/gmw_monitoring/monitoring/scores_imgs'
out_uid_dir = '/scratch/a.pfb/gmw_monitoring/monitoring/uid_imgs'

vec_file = '/scratch/a.pfb/gmw_monitoring/monitoring/gmw_monitor_lut.gpkg'

scr_imgs = glob.glob(os.path.join(out_scores_dir, '*.kea'))
rsgislib.imageutils.imagelut.createImgExtentLUT(scr_imgs, vec_file, 'score_imgs', 'GPKG', ignore_none_imgs=False, out_proj_wgs84=False, overwrite_lut_file=False)


uid_imgs = glob.glob(os.path.join(out_uid_dir, '*.kea'))
rsgislib.imageutils.imagelut.createImgExtentLUT(uid_imgs, vec_file, 'uid_imgs', 'GPKG', ignore_none_imgs=False, out_proj_wgs84=False, overwrite_lut_file=False)


