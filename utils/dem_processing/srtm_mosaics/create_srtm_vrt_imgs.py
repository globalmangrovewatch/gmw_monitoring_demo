import glob
import rsgislib.imageutils

min_1_imgs = glob.glob("/scratch/a.pfb/SRTM/min_val_1_tiles/*.kea")
min_1_vrt = "/scratch/a.pfb/SRTM/srtm_min_val_1_global_mosaic_1arc_v3.vrt"
rsgislib.imageutils.gdal_mosaic_images_vrt(min_1_imgs, min_1_vrt)

min_0_imgs = glob.glob("/scratch/a.pfb/SRTM/min_val_0_tiles/*.kea")
min_0_vrt = "/scratch/a.pfb/SRTM/srtm_min_val_0_global_mosaic_1arc_v3.vrt"
rsgislib.imageutils.gdal_mosaic_images_vrt(min_0_imgs, min_0_vrt)


min_1_kea = "/scratch/a.pfb/SRTM/srtm_min_val_1_global_mosaic_1arc_v3.kea"
rsgislib.imageutils.gdal_translate(min_1_vrt, min_1_kea, gdal_format='KEA')

min_0_kea = "/scratch/a.pfb/SRTM/srtm_min_val_0_global_mosaic_1arc_v3.kea"
rsgislib.imageutils.gdal_translate(min_0_vrt, min_0_kea, gdal_format='KEA')
