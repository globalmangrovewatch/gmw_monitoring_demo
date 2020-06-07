from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.rastergis
import rsgislib.rastergis.ratutils
import rsgislib.imageutils.imagelut
import rsgislib.imageutils
import rsgislib.segmentation

import logging
import os
import shutil
import subprocess
import numpy
import datetime

logger = logging.getLogger(__name__)

def delete_vector_file(vec_file, feedback=True):
    from osgeo import gdal
    import os
    ds_in_vec = gdal.OpenEx(vec_file, gdal.OF_READONLY)
    if ds_in_vec is None:
        raise Exception("Could not open '{}'".format(vec_file))
    file_lst = ds_in_vec.GetFileList()
    for cfile in file_lst:
        if feedback:
            print("Deleting: {}".format(cfile))
        os.remove(cfile)

def update_uid_image(uid_img, chng_img, nxt_scr5_img, clrsky_img, year_obs, day_year_obs, tmp_uid_tile):
    from rios import applier
    try:
        import tqdm
        progress_bar = rsgislib.TQDMProgressBar()
    except:
        from rios import cuiprogress
        progress_bar = cuiprogress.GDALProgressBar()

    infiles = applier.FilenameAssociations()
    infiles.uid_img = uid_img
    infiles.chng_img = chng_img
    infiles.nxt_scr5_img = nxt_scr5_img
    infiles.clrsky_img = clrsky_img
    outfiles = applier.FilenameAssociations()
    outfiles.uid_img_out = tmp_uid_tile
    otherargs = applier.OtherInputs()
    otherargs.year_obs = year_obs
    otherargs.day_year_obs = day_year_obs
    aControls = applier.ApplierControls()
    aControls.progress = progress_bar
    aControls.omitPyramids = True
    aControls.calcStats = False

    def _update_uid_img(info, inputs, outputs, otherargs):
        """
        This is an internal rios function
        """
        uid_img_arr = numpy.array(inputs.uid_img, dtype=numpy.uint32, copy=True)
        start_year = inputs.uid_img[0]
        chng_feats = inputs.chng_img[0]
        new_chng_pxls = numpy.zeros_like(start_year)
        new_chng_pxls[(start_year==0)&(chng_feats==1)] = 1

        chng_scr5_year = inputs.uid_img[4]
        new_scr5_pxls = numpy.zeros_like(chng_scr5_year)
        new_scr5_pxls[(chng_scr5_year == 0) & (inputs.nxt_scr5_img[0] == 1)] = 1

        uid_img_arr[0, new_chng_pxls==1] = otherargs.year_obs
        uid_img_arr[1, new_chng_pxls==1] = otherargs.day_year_obs
        uid_img_arr[2, inputs.clrsky_img[0]==1] = otherargs.year_obs
        uid_img_arr[3, inputs.clrsky_img[0]==1] = otherargs.day_year_obs
        uid_img_arr[4, new_scr5_pxls==1] = otherargs.year_obs
        uid_img_arr[5, new_scr5_pxls==1] = otherargs.day_year_obs

        outputs.uid_img_out = uid_img_arr

    applier.apply(_update_uid_img, infiles, outfiles, otherargs, controls=aControls)


class LandsatGMWChange(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["chng_lut_file", "chng_score_lut", "chng_uid_lut", "tmp_path",
                        "out_data_path", "base_chngs"]
        EODataDownUserAnalysis.__init__(self, analysis_name='LandsatGMWChangeFnl', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None
        success = True
        out_dict = None
        try:
            rsgis_utils = rsgislib.RSGISPyUtils()
            eodd_utils = EODataDownUtils()

            scn_ext_info = scn_db_obj.ExtendedInfo
            if (scn_ext_info is not None) and ('LandsatGMWScnChange' in scn_ext_info):
                scn_chng_info = scn_ext_info['LandsatGMWScnChange']
                if isinstance(scn_chng_info, dict) and ('chng_feats_vec' in scn_chng_info):
                    chng_feats_vec_file = scn_chng_info['chng_feats_vec']
                    logger.debug("Have change features vector file: {}".format(chng_feats_vec_file))
                    chng_feats_vec_lyrs = rsgislib.vectorutils.getVecLyrsLst(chng_feats_vec_file)
                    if len(chng_feats_vec_lyrs) != 1:
                        raise Exception("The number of layers is not equal to one. Don't know what has happened...")
                    chng_feats_vec_lyr = chng_feats_vec_lyrs[0]
                    logger.debug("Have change features vector layer: {}".format(chng_feats_vec_lyr))

                    clearsky_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*clearsky.tif")
                    logger.debug("Clear Sky Mask File: {}".format(clearsky_img_file))
                    if clearsky_img_file is None:
                        raise Exception("The clear sky image is not available. The previous plugin probably failed.")

                    basename = rsgis_utils.get_file_basename(chng_feats_vec_file)
                    basename = basename.replace('_chng_gmw_vec', '')
                    logger.debug("The basename for the processing is: {}".format(basename))

                    base_tmp_dir = os.path.join(self.params["tmp_path"], "{}_{}_gmw_chng".format(scn_db_obj.Product_ID, scn_db_obj.PID))
                    if not os.path.exists(base_tmp_dir):
                        os.mkdir(base_tmp_dir)

                    clr_sky_vec_file = os.path.join(base_tmp_dir, "{}_clearsky_vec.gpkg".format(basename))
                    rsgislib.vectorutils.polygoniseRaster2VecLyr(clr_sky_vec_file, 'clrsky', 'GPKG', clearsky_img_file,
                                                                 imgBandNo=1, maskImg=clearsky_img_file, imgMaskBandNo=1,
                                                                 replace_file=True, replace_lyr=True,
                                                                 pxl_val_fieldname='PXLVAL')

                    gmw_chng_wgs84_vec = os.path.join(base_tmp_dir, "{}_chng_gmw_vec_wgs84.gpkg".format(basename))
                    if os.path.exists(gmw_chng_wgs84_vec):
                        delete_vector_file(gmw_chng_wgs84_vec)
                    cmd = "ogr2ogr -f GPKG -nln {0} -t_srs EPSG:4326 {1} {2} {0}".format(chng_feats_vec_lyr, gmw_chng_wgs84_vec, chng_feats_vec_file)
                    logger.debug("Going to run command: '{}'".format(cmd))
                    try:
                        subprocess.check_call(cmd, shell=True)
                    except OSError as e:
                        raise Exception('Failed to run command: ' + cmd)

                    clr_sky_wgs84_vec_file = os.path.join(base_tmp_dir, "{}_clearsky_vec_wgs84.gpkg".format(basename))
                    if os.path.exists(clr_sky_wgs84_vec_file):
                        delete_vector_file(clr_sky_wgs84_vec_file)
                    cmd = "ogr2ogr -f GPKG -nln clrsky -t_srs EPSG:4326 {} {} clrsky".format(clr_sky_wgs84_vec_file, clr_sky_vec_file)
                    logger.debug("Going to run command: '{}'".format(cmd))
                    try:
                        subprocess.check_call(cmd, shell=True)
                    except OSError as e:
                        raise Exception('Failed to run command: ' + cmd)

                    lyr_bbox = rsgis_utils.getVecLayerExtent(clr_sky_wgs84_vec_file, 'clrsky')
                    print(lyr_bbox)

                    score_tiles = rsgislib.imageutils.imagelut.query_img_lut(lyr_bbox, self.params["chng_lut_file"], self.params["chng_score_lut"])
                    uid_tiles = rsgislib.imageutils.imagelut.query_img_lut(lyr_bbox, self.params["chng_lut_file"], self.params["chng_uid_lut"])

                    if len(score_tiles) != len(uid_tiles):
                        raise Exception("The number of score and unique ID tiles are different - this shouldn't be possible.")

                    if len(score_tiles) > 0:
                        tile_imgs_dict = dict()
                        for scr_tile in score_tiles:
                            print(scr_tile)
                            tile_basename = rsgis_utils.get_file_basename(scr_tile).replace("_chng_scr", "").replace("gmw_", "")
                            print(tile_basename)
                            tile_imgs_dict[tile_basename] = dict()

                            uid_tile = None
                            for uid_tile_tmp in uid_tiles:
                                if tile_basename in uid_tile_tmp:
                                    uid_tile = uid_tile_tmp
                                    break
                            if uid_tile is None:
                                raise Exception("Could not find the UID tile for the scr_tile - something has gone wrong!")
                            print(uid_tile)

                            gmw_tile_chng_img = os.path.join(base_tmp_dir, "{}_{}_chng_feats.kea".format(basename, tile_basename))
                            rsgislib.vectorutils.rasteriseVecLyr(gmw_chng_wgs84_vec, chng_feats_vec_lyr, scr_tile, gmw_tile_chng_img, gdalformat="KEA", burnVal=1)
                            rsgislib.rastergis.populateStats(gmw_tile_chng_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                            chng_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_chng_img, 'Histogram')
                            if chng_feat_pxl_counts.shape[0] > 1:
                                n_chng_pxls = chng_feat_pxl_counts[1]
                            else:
                                n_chng_pxls = 0
                            logger.debug("There are '{}' change pixels within the tile {}.".format(n_chng_pxls, tile_basename))

                            if n_chng_pxls > 0:
                                base_chng_tile_path = None
                                zero_date_delta = datetime.timedelta()
                                min_delta = 0
                                min_delta_path = ''
                                first = True
                                for base_chng_year in self.params["base_chngs"]:
                                    base_chng_date = datetime.datetime(year=self.params["base_chngs"][base_chng_year]["year"],
                                                                       month=self.params["base_chngs"][base_chng_year]["month"],
                                                                       day=self.params["base_chngs"][base_chng_year]["day"])
                                    date_delta = scn_db_obj.Sensing_Time - base_chng_date
                                    if date_delta > zero_date_delta:
                                        if first:
                                            min_delta = date_delta
                                            min_delta_path = self.params["base_chngs"][base_chng_year]["path"]
                                            first = False
                                        elif date_delta < min_delta:
                                            min_delta = date_delta
                                            min_delta_path = self.params["base_chngs"][base_chng_year]["path"]
                                if not first:
                                    base_chng_tile_path = min_delta_path

                                if base_chng_tile_path is not None:
                                    base_chng_tile_img = rsgis_utils.findFileNone(base_chng_tile_path, "*{}*.kea".format(tile_basename))
                                    if base_chng_tile_img is not None:
                                        # Mask change features
                                        gmw_tile_chng_mskd_img = os.path.join(base_tmp_dir, "{}_{}_chng_feats_mskd.kea".format(basename, tile_basename))
                                        rsgislib.imageutils.maskImage(gmw_tile_chng_img, base_chng_tile_img, gmw_tile_chng_mskd_img, 'KEA', rsgislib.TYPE_8UINT, 0, 1)
                                        rsgislib.rastergis.populateStats(gmw_tile_chng_mskd_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                                        chng_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_chng_mskd_img, 'Histogram')
                                        if chng_feat_pxl_counts.shape[0] > 1:
                                            n_chng_pxls = chng_feat_pxl_counts[1]
                                        else:
                                            n_chng_pxls = 0
                                        logger.debug("There are '{}' change pixels following masking within the tile {}.".format(n_chng_pxls, tile_basename))
                                        gmw_tile_chng_img = gmw_tile_chng_mskd_img
                                        if n_chng_pxls > 0:
                                            # Mask the Score Image
                                            eodd_utils.get_file_lock(scr_tile, sleep_period=1, wait_iters=120, use_except=True)
                                            scr_tile_tmp = os.path.join(base_tmp_dir, "{}_{}_score_tmp.kea".format(basename, tile_basename))
                                            rsgislib.imageutils.maskImage(scr_tile, base_chng_tile_img, scr_tile_tmp, 'KEA', rsgislib.TYPE_8UINT, 0, 1)
                                            # Replace the score image.
                                            rsgislib.imageutils.gdal_translate(scr_tile_tmp, scr_tile, gdal_format='KEA')
                                            eodd_utils.release_file_lock(scr_tile)

                                            # Mask the Score Image
                                            eodd_utils.get_file_lock(uid_tile, sleep_period=1, wait_iters=120, use_except=True)
                                            uid_tile_tmp = os.path.join(base_tmp_dir, "{}_{}_uid_tmp.kea".format(basename, tile_basename))
                                            rsgislib.imageutils.maskImage(uid_tile, base_chng_tile_img, uid_tile_tmp, 'KEA', rsgislib.TYPE_32UINT, 0, 1)
                                            # Replace the score image.
                                            rsgislib.imageutils.gdal_translate(uid_tile_tmp, uid_tile, gdal_format='KEA')
                                            eodd_utils.release_file_lock(uid_tile)

                                if n_chng_pxls > 0:
                                    gmw_tile_clrsky_img = os.path.join(base_tmp_dir,"{}_{}_clrsky_feats.kea".format(basename, tile_basename))
                                    rsgislib.vectorutils.rasteriseVecLyr(clr_sky_wgs84_vec_file, 'clrsky', scr_tile, gmw_tile_clrsky_img, gdalformat="KEA", burnVal=1)
                                    rsgislib.rastergis.populateStats(gmw_tile_clrsky_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                                    clrsky_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_clrsky_img, 'Histogram')
                                    if clrsky_feat_pxl_counts.shape[0] > 1:
                                        n_clrcky_pxls = clrsky_feat_pxl_counts[1]
                                    else:
                                        n_clrcky_pxls = 0
                                    logger.debug("There are '{}' clear sky pixels within the tile {}.".format(n_clrcky_pxls, tile_basename))
                                    # Clear sky pixels are important as we reduce the score where an observation has taken place.

                                    if (n_clrcky_pxls > 0):
                                        logger.debug("There are clear sky pixels within the tile ({}) so continuing.".format(tile_basename))
                                        out_scn_dir = os.path.join(self.params['out_data_path'], "{}_{}_scn_chng_tiles".format(scn_db_obj.Product_ID, scn_db_obj.PID))
                                        if not os.path.exists(out_scn_dir):
                                            os.mkdir(out_scn_dir)

                                        eodd_utils.get_file_lock(scr_tile, sleep_period=1, wait_iters=120, use_except=True)
                                        gmw_tile_reached_5scr_img = os.path.join(base_tmp_dir, "{}_{}_reached_5scr.kea".format(basename, tile_basename))
                                        band_defs = [rsgislib.imagecalc.BandDefn('score', scr_tile, 1),
                                                     rsgislib.imagecalc.BandDefn('chng', gmw_tile_chng_img, 1)]
                                        exp = '(chng==1)&&(score>=3)&&(score<5)?1:0'  # optical data change is a score of 2 (SAR 1)
                                        rsgislib.imagecalc.bandMath(gmw_tile_reached_5scr_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs, False, False)

                                        # Increment the score
                                        band_defs = [rsgislib.imagecalc.BandDefn('score', scr_tile, 1),
                                                     rsgislib.imagecalc.BandDefn('clrsky', gmw_tile_clrsky_img, 1),
                                                     rsgislib.imagecalc.BandDefn('chng', gmw_tile_chng_img, 1)]
                                        exp = '(chng==1)&&(score<5)?(score+2)>5?5:(score+2):(clrsky==1)&&(chng==0)&&(score>0)&&(score<5)?score-1:score' # optical data change is a score of 2 (SAR 1)
                                        lcl_tile_scr_img = os.path.join(out_scn_dir, "{}_{}_score.kea".format(basename, tile_basename))
                                        rsgislib.imagecalc.bandMath(lcl_tile_scr_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs, False, False)
                                        rsgislib.imageutils.popImageStats(lcl_tile_scr_img, usenodataval=True, nodataval=0, calcpyramids=True)
                                        tile_imgs_dict[tile_basename]['score'] = lcl_tile_scr_img

                                        # Overwrite the global score.
                                        rsgislib.imageutils.gdal_translate(lcl_tile_scr_img, scr_tile, 'KEA')
                                        rsgislib.imageutils.popImageStats(scr_tile, usenodataval=True, nodataval=0, calcpyramids=True)
                                        eodd_utils.release_file_lock(scr_tile)

                                        # Update the UID image
                                        eodd_utils.get_file_lock(uid_tile, sleep_period=1, wait_iters=120, use_except=True)
                                        acq_date = scn_db_obj.Sensing_Time
                                        year_obs = acq_date.year
                                        day_year_obs = acq_date.timetuple().tm_yday
                                        lcl_tile_uid_img = os.path.join(out_scn_dir, "{}_{}_uid.kea".format(basename, tile_basename))
                                        if os.path.exists(lcl_tile_uid_img):
                                            rsgis_utils.deleteFileWithBasename(lcl_tile_uid_img)
                                        update_uid_image(uid_tile, gmw_tile_chng_img, gmw_tile_reached_5scr_img, gmw_tile_clrsky_img, year_obs, day_year_obs, lcl_tile_uid_img)
                                        rsgislib.imageutils.popImageStats(lcl_tile_uid_img, usenodataval=True, nodataval=0, calcpyramids=True)
                                        tile_imgs_dict[tile_basename]['uid'] = lcl_tile_uid_img

                                        # Overwrite the global UID image.
                                        rsgislib.imageutils.gdal_translate(lcl_tile_uid_img, uid_tile, 'KEA')
                                        rsgislib.imageutils.popImageStats(uid_tile, usenodataval=True, nodataval=0, calcpyramids=True)
                                        eodd_utils.release_file_lock(uid_tile)

                                else:
                                    logger.debug("There are no change pixels within the tile skipping.")
                            else:
                                logger.debug("There are no change pixels within the tile skipping.")

                        out_dict = tile_imgs_dict
                        success = True
                        scn_tile_lut_file = os.path.join(out_scn_dir, "{}_tiles_lut.json".format(basename))
                        rsgis_utils.writeDict2JSON(tile_imgs_dict, scn_tile_lut_file)
                    else:
                        logger.error("There are no tiles intersecting with the change features. Need to check what's happened here; Landsat PID: {}".format(scn_db_obj.PID))

                    # Remove the tmp directory to clean up...
                    if os.path.exists(base_tmp_dir):
                        shutil.rmtree(base_tmp_dir)

                else:
                    logger.debug("No scene change vector was available so assume there were no change features.")
            else:
                logger.debug("No scene change vector was available so assume there were no change features.")

        except Exception as e:
            logger.debug("An error occurred during plugin processing. See stacktrace...", stack_info=True)
            logger.exception(e)
            success = False

        return success, out_dict

