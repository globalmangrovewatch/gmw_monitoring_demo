import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import glob
import datetime
import os.path


def create_chng_period_lyr(tile_luts_files, vec_lyr_dir, score_imgs, out_scr_img_dir, out_img_dir,
                           out_tile_vecs_dir, limit_year, limit_month, limit_day, out_vec_file):

    rsgis_utils = rsgislib.RSGISPyUtils()

    limit_date = datetime.datetime(year=limit_year, month=limit_month, day=limit_day)
    timedeltadiff = datetime.timedelta()

    for tile_lut_file in tile_luts_files:
        tile_lut = rsgis_utils.readJSON2Dict(tile_lut_file)
        first = True
        max_time_diff = 0
        max_time_key = ''
        for tile_obs_key in tile_lut:
            tile_obs_date = datetime.datetime.fromisoformat(tile_obs_key)
            time_dif = tile_obs_date - limit_date

            if time_dif < timedeltadiff:
                if first:
                    max_time_key = tile_obs_key
                    max_time_diff = time_dif
                    first = False
                elif time_dif > max_time_diff:
                    max_time_key = tile_obs_key
                    max_time_diff = time_dif
        if not first:
            basename = os.path.splitext(os.path.basename(tile_lut_file))[0]
            tile = basename.replace('gmw_tile_', '').replace('_lut', '')
            print(tile)
            tile_ref_img = rsgis_utils.findFileNone(score_imgs, '*{}*.kea'.format(tile))
            if tile_ref_img is not None:
                print(tile_ref_img)
            else:
                raise Exception("Could not find tile images - {}".format(tile))

            hist_chng_vec_file = os.path.join(vec_lyr_dir, tile_lut[max_time_key]['file'])
            hist_chng_vec_lyr = tile_lut[max_time_key]['layer']
            tile_scr_img = os.path.join(out_scr_img_dir, "gmw_tile_{}_hist_chng_score.kea".format(tile))
            rsgislib.vectorutils.rasteriseVecLyr(hist_chng_vec_file, hist_chng_vec_lyr, tile_ref_img, tile_scr_img,
                                                 gdalformat='KEA', burnVal=1, datatype=rsgislib.TYPE_8UINT, vecAtt='Score',
                                                 vecExt=False, thematic=True, nodata=0)

            tile_chng_img = os.path.join(out_img_dir, "gmw_tile_{}_hist_chng.kea".format(tile))
            rsgislib.imagecalc.imageMath(tile_scr_img, tile_chng_img, 'b1>4?1:0', 'KEA', rsgislib.TYPE_8UINT)

            tile_chng_vec = os.path.join(out_tile_vecs_dir, "gmw_tile_{}_hist_chng.gpkg".format(tile))
            rsgislib.vectorutils.polygoniseRaster2VecLyr(tile_chng_vec, 'tile', 'GPKG', tile_chng_img, imgBandNo=1,
                                                         maskImg=tile_chng_img, imgMaskBandNo=1, replace_file=True,
                                                         replace_lyr=True, pxl_val_fieldname='PXLVAL')

    vec_files = glob.glob(os.path.join(out_tile_vecs_dir, '*.gpkg'))
    rsgislib.vectorutils.mergeVectors2GPKG(vec_files, out_vec_file, 'chng', False)


tile_luts_files = glob.glob("/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_tile_luts/*.json")
vec_lyr_dir = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_vecs'
score_imgs = "/Users/pete/Temp/gmw_monitoring_files/scores_imgs"
out_scr_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_scr_imgs"
out_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_imgs_2018"
out_tile_vecs_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_vecs_2018"
limit_year = 2019
limit_month = 5
limit_day = 1
out_vec_file = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_hist_chng_2018.gpkg'

create_chng_period_lyr(tile_luts_files, vec_lyr_dir, score_imgs, out_scr_img_dir, out_img_dir,
                           out_tile_vecs_dir, limit_year, limit_month, limit_day, out_vec_file)


tile_luts_files = glob.glob("/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_tile_luts/*.json")
vec_lyr_dir = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_vecs'
score_imgs = "/Users/pete/Temp/gmw_monitoring_files/scores_imgs"
out_scr_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_scr_imgs"
out_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_imgs_2019"
out_tile_vecs_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_vecs_2019"
limit_year = 2020
limit_month = 1
limit_day = 1
out_vec_file = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_hist_chng_2019.gpkg'

create_chng_period_lyr(tile_luts_files, vec_lyr_dir, score_imgs, out_scr_img_dir, out_img_dir,
                           out_tile_vecs_dir, limit_year, limit_month, limit_day, out_vec_file)

tile_luts_files = glob.glob("/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_tile_luts/*.json")
vec_lyr_dir = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_chng_fnl_vecs'
score_imgs = "/Users/pete/Temp/gmw_monitoring_files/scores_imgs"
out_scr_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_scr_imgs"
out_img_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_imgs_2020"
out_tile_vecs_dir = "/Users/pete/Temp/gmw_monitoring_rslts/hist_chng_vecs_2020"
limit_year = 2020
limit_month = 6
limit_day = 1
out_vec_file = '/Users/pete/Temp/gmw_monitoring_rslts/gmw_hist_chng_2020.gpkg'

create_chng_period_lyr(tile_luts_files, vec_lyr_dir, score_imgs, out_scr_img_dir, out_img_dir,
                           out_tile_vecs_dir, limit_year, limit_month, limit_day, out_vec_file)
