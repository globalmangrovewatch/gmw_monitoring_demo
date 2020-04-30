from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis

import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.rastergis
import rsgislib.imageutils.imagelut
import rsgislib.imageutils

import logging
import os
import shutil
import subprocess

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

def update_uid_image(score_img, uid_img, chng_img, year_obs, day_year_obs):
    from rios import applier
    try:
        import tqdm
        progress_bar = rsgislib.TQDMProgressBar()
    except:
        from rios import cuiprogress
        progress_bar = cuiprogress.GDALProgressBar()

    infiles = applier.FilenameAssociations()
    infiles.score_img = score_img
    infiles.uid_img = uid_img
    infiles.chng_img = chng_img
    outfiles = applier.FilenameAssociations()
    outfiles.uid_img = uid_img
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
        uid_img_arr = inputs.uid_img
        print(inputs.score_img.shape)
        print(uid_img_arr.shape)
        print(inputs.chng_img.shape)
        print(otherargs.year_obs)
        print(otherargs.day_year_obs)
        print("\n\n")



        outputs.uid_img = uid_img_arr

    applier.apply(_update_uid_img, infiles, outfiles, otherargs, controls=aControls)


class LandsatGMWChange(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["chng_lut_file", "chng_score_lut", "chng_uid_lut", "tmp_path"]
        EODataDownUserAnalysis.__init__(self, analysis_name='LandsatGMWChangeFnl', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        success = True
        out_dict = None
        try:
            rsgis_utils = rsgislib.RSGISPyUtils()

            scn_ext_info = scn_db_obj.ExtendedInfo
            if 'LandsatGMWChangeTest' in scn_ext_info:
                scn_chng_info = scn_ext_info['LandsatGMWChangeTest']
                if 'chng_feats_vec' in scn_chng_info:
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

                    clr_sky_vec_file = os.path.join(base_tmp_dir, "{}_clearsky_vec.geojson")
                    rsgislib.vectorutils.polygoniseRaster2VecLyr(clr_sky_vec_file, 'clrsky', 'GEOJSON', clearsky_img_file,
                                                                 imgBandNo=1, maskImg=clearsky_img_file, imgMaskBandNo=1,
                                                                 replace_file=True, replace_lyr=True,
                                                                 pxl_val_fieldname='PXLVAL')

                    gmw_chng_wgs84_vec = os.path.join(base_tmp_dir, "{}_chng_gmw_vec_wgs84.geojson".format(basename))
                    if os.path.exists(gmw_chng_wgs84_vec):
                        delete_vector_file(gmw_chng_wgs84_vec)
                    cmd = "ogr2ogr -f GEOJSON -nln {0} -t_srs EPSG:4326 {1} {2} {0}".format(chng_feats_vec_lyr, gmw_chng_wgs84_vec, chng_feats_vec_file)
                    logger.debug("Going to run command: '{}'".format(cmd))
                    try:
                        subprocess.check_call(cmd, shell=True)
                    except OSError as e:
                        raise Exception('Failed to run command: ' + cmd)

                    clr_sky_wgs84_vec_file = os.path.join(base_tmp_dir, "{}_clearsky_vec_wgs84.geojson".format(basename))
                    if os.path.exists(clr_sky_wgs84_vec_file):
                        delete_vector_file(clr_sky_wgs84_vec_file)
                    cmd = "ogr2ogr -f GEOJSON -nln clrsky -t_srs EPSG:4326 {} {} clrsky".format(clr_sky_wgs84_vec_file, clr_sky_vec_file)
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
                        for scr_tile in score_tiles:
                            print(scr_tile)
                            tile_basename = rsgis_utils.get_file_basename(scr_tile).replace("_chng_scr", "")
                            print(tile_basename)
                            uid_tile = None
                            for uid_tile_tmp in uid_tiles:
                                if tile_basename in uid_tile_tmp:
                                    uid_tile = uid_tile_tmp
                                    break
                            if uid_tile is None:
                                raise Exception("Could not find the UID tile for the scr_tile - something has gone wrong!")
                            print(uid_tile)

                            gmw_tile_chng_img = os.path.join(base_tmp_dir, "{}_{}_chng_feats.kea".format(basename, tile_basename))
                            rsgislib.vectorutils.rasteriseVecLyr(gmw_chng_wgs84_vec, chng_feats_vec_lyr, scr_tile,
                                                                 gmw_tile_chng_img, gdalformat="KEA", burnVal=1)
                            rsgislib.rastergis.populateStats(gmw_tile_chng_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                            chng_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_chng_img, 'Histogram')
                            if chng_feat_pxl_counts.shape[0] > 1:
                                n_chng_pxls = chng_feat_pxl_counts[1]
                            else:
                                n_chng_pxls = 0
                            logger.debug("There are '{}' change pixels within the tile {}.".format(n_chng_pxls, tile_basename))


                            gmw_tile_clrsky_img = os.path.join(base_tmp_dir,"{}_{}_clrsky_feats.kea".format(basename, tile_basename))
                            rsgislib.vectorutils.rasteriseVecLyr(clr_sky_wgs84_vec_file, 'clrsky', scr_tile,
                                                                 gmw_tile_clrsky_img, gdalformat="KEA", burnVal=1)
                            rsgislib.rastergis.populateStats(gmw_tile_clrsky_img, addclrtab=True, calcpyramids=True,
                                                             ignorezero=True)
                            clrsky_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_clrsky_img,
                                                                                             'Histogram')
                            if clrsky_feat_pxl_counts.shape[0] > 1:
                                n_clrcky_pxls = clrsky_feat_pxl_counts[1]
                            else:
                                n_clrcky_pxls = 0
                            logger.debug("There are '{}' clear sky pixels within the tile {}.".format(n_clrcky_pxls, tile_basename))

                            if (n_clrcky_pxls > 0):
                                logger.debug("There are clear sky pixels within the tile ({}) so continuing.".format(tile_basename))

                                # Increment the score
                                band_defs = [rsgislib.imagecalc.BandDefn('score', scr_tile, 1),
                                             rsgislib.imagecalc.BandDefn('clrsky', gmw_tile_clrsky_img, 1),
                                             rsgislib.imagecalc.BandDefn('chng', gmw_tile_chng_img, 1)]
                                exp = '(chng==1)&&(score<5)?score+2:(clrsky==1)&&(chng==0)&&(score>0)&&(score<5)?score-1:score' # optical data change is a score of 2 (SAR 1)
                                rsgislib.imagecalc.bandMath(scr_tile, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs, False, True)
                                rsgislib.imageutils.popImageStats(scr_tile, usenodataval=True, nodataval=0, calcpyramids=True)


                                # Update the UID image
                                acq_date = scn_db_obj.Date_Acquired
                                year_obs = acq_date.year
                                day_year_obs = acq_date.timetuple().tm_yday
                                update_uid_image(scr_tile, uid_tile, gmw_tile_chng_img, year_obs, day_year_obs)



                            else:
                                logger.debug("There are no change pixels within the tile skipping.")
                                success = True
                    else:
                        logger.error("There are no tiles intersecting with the change features. Need to check what's happened here; Landsat PID: {}".format(scn_db_obj.PID))
                        success = True


                    # Remove the tmp directory to clean up...
                    # if os.path.exists(base_tmp_dir):
                    #    shutil.rmtree(base_tmp_dir)

                else:
                    logger.debug("No scene change vector was available so assume there were no change features.")
                    success = True
            else:
                logger.debug("No scene change vector was available so assume there were no change features.")
                success = True

        except Exception as e:
            logger.debug("An error occurred during plugin processing. See stacktrace...", stack_info=True)
            logger.exception(e)
            success = False

        success = False
        return success, out_dict


"""
{
    "path":"/Users/pete/Development/globalmangrovewatch/gmw_monitoring_demo/eodd_plugins/landsat",
    "module":"landsat_gmw_chng",
    "class":"LandsatGMWChange",
    "params":
    {
        "chng_lut_file":"/Users/pete/Temp/gmw_monitoring_files/gmw_monitor_lut.gpkg",
        "chng_score_lut":"score_imgs",
        "chng_uid_lut":"uid_imgs",
        "out_vec_path":"/Users/pete/Temp/eodd_user_analysis/vec_outs",
        "tmp_path":"/Users/pete/Temp/eodd_user_analysis/plugin_tmp"

    }
}
"""