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

def update_uid_image(uid_img, chng_img, vld_img, year_obs, day_year_obs, tmp_uid_tile):
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
    infiles.vld_img = vld_img
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
        uid_img_arr = numpy.copy(inputs.uid_img)
        start_year = uid_img_arr[0]
        chng_feats = inputs.chng_img[0]
        new_chng_pxls = numpy.zeros_like(start_year)
        new_chng_pxls[(start_year==0)&(chng_feats==1)] = 1

        uid_img_arr[0, new_chng_pxls==1] = otherargs.year_obs
        uid_img_arr[1, new_chng_pxls==1] = otherargs.day_year_obs
        uid_img_arr[2, inputs.vld_img[0]==1] = otherargs.year_obs
        uid_img_arr[3, inputs.vld_img[0]==1] = otherargs.day_year_obs

        outputs.uid_img_out = uid_img_arr

    applier.apply(_update_uid_img, infiles, outfiles, otherargs, controls=aControls)


class Sentinel1GMWChange(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["chng_lut_file", "chng_score_lut", "chng_uid_lut", "tmp_path", "out_vec_path", "chng_vec_luts"]
        EODataDownUserAnalysis.__init__(self, analysis_name='Sentinel1GMWChangeFnl', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        success = True
        out_dict = None
        try:
            rsgis_utils = rsgislib.RSGISPyUtils()
            eodd_utils = EODataDownUtils()

            scn_ext_info = scn_db_obj.ExtendedInfo
            if 'Sentinel1GMWScnChange' in scn_ext_info:
                scn_chng_info = scn_ext_info['Sentinel1GMWScnChange']
                if 'chng_feats_vec' in scn_chng_info:
                    chng_feats_vec_file = scn_chng_info['chng_feats_vec']
                    logger.debug("Have change features vector file: {}".format(chng_feats_vec_file))
                    chng_feats_vec_lyrs = rsgislib.vectorutils.getVecLyrsLst(chng_feats_vec_file)
                    if len(chng_feats_vec_lyrs) != 1:
                        raise Exception("The number of layers is not equal to one. Don't know what has happened...")
                    chng_feats_vec_lyr = chng_feats_vec_lyrs[0]
                    logger.debug("Have change features vector layer: {}".format(chng_feats_vec_lyr))

                    valid_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*vmsk.tif")
                    logger.debug("Valid Mask File: {}".format(valid_img_file))
                    if valid_img_file is None:
                        raise Exception("The valid image is not available. The previous plugin probably failed.")

                    basename = rsgis_utils.get_file_basename(chng_feats_vec_file)
                    basename = basename.replace('_chng_gmw_vec', '')
                    logger.debug("The basename for the processing is: {}".format(basename))

                    base_tmp_dir = os.path.join(self.params["tmp_path"], "{}_{}_gmw_chng".format(scn_db_obj.Product_ID, scn_db_obj.PID))
                    if not os.path.exists(base_tmp_dir):
                        os.mkdir(base_tmp_dir)

                    valid_vec_file = os.path.join(base_tmp_dir, "{}_valid_vec.geojson".format(basename))
                    rsgislib.vectorutils.polygoniseRaster2VecLyr(valid_img_file, 'vld', 'GEOJSON', valid_img_file,
                                                                 imgBandNo=1, maskImg=valid_img_file, imgMaskBandNo=1,
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

                    valid_wgs84_vec_file = os.path.join(base_tmp_dir, "{}_valid_vec_wgs84.geojson".format(basename))
                    if os.path.exists(valid_wgs84_vec_file):
                        delete_vector_file(valid_wgs84_vec_file)
                    cmd = "ogr2ogr -f GEOJSON -nln valid -t_srs EPSG:4326 {} {} vld".format(valid_wgs84_vec_file, valid_vec_file)
                    logger.debug("Going to run command: '{}'".format(cmd))
                    try:
                        subprocess.check_call(cmd, shell=True)
                    except OSError as e:
                        raise Exception('Failed to run command: ' + cmd)

                    lyr_bbox = rsgis_utils.getVecLayerExtent(valid_wgs84_vec_file, 'valid')
                    print(lyr_bbox)

                    score_tiles = rsgislib.imageutils.imagelut.query_img_lut(lyr_bbox, self.params["chng_lut_file"], self.params["chng_score_lut"])
                    uid_tiles = rsgislib.imageutils.imagelut.query_img_lut(lyr_bbox, self.params["chng_lut_file"], self.params["chng_uid_lut"])

                    if len(score_tiles) != len(uid_tiles):
                        raise Exception("The number of score and unique ID tiles are different - this shouldn't be possible.")

                    if len(score_tiles) > 0:
                        for scr_tile in score_tiles:
                            print(scr_tile)
                            tile_basename = rsgis_utils.get_file_basename(scr_tile).replace("_chng_scr", "").replace("gmw_", "")
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


                            gmw_tile_vld_img = os.path.join(base_tmp_dir,"{}_{}_vld_feats.kea".format(basename, tile_basename))
                            rsgislib.vectorutils.rasteriseVecLyr(valid_wgs84_vec_file, 'valid', scr_tile,
                                                                 gmw_tile_vld_img, gdalformat="KEA", burnVal=1)
                            rsgislib.rastergis.populateStats(gmw_tile_vld_img, addclrtab=True, calcpyramids=True,
                                                             ignorezero=True)
                            vld_feat_pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_tile_vld_img,
                                                                                             'Histogram')
                            if vld_feat_pxl_counts.shape[0] > 1:
                                n_vld_pxls = vld_feat_pxl_counts[1]
                            else:
                                n_vld_pxls = 0
                            logger.debug("There are '{}' valid pixels within the tile {}.".format(n_vld_pxls, tile_basename))

                            if (n_vld_pxls > 0):
                                logger.debug("There are valid pixels within the tile ({}) so continuing.".format(tile_basename))

                                eodd_utils.get_file_lock(scr_tile, sleep_period=1, wait_iters=120, use_except=True)
                                # Increment the score
                                band_defs = [rsgislib.imagecalc.BandDefn('score', scr_tile, 1),
                                             rsgislib.imagecalc.BandDefn('vld', gmw_tile_vld_img, 1),
                                             rsgislib.imagecalc.BandDefn('chng', gmw_tile_chng_img, 1)]
                                exp = '(chng==1)&&(score<5)?(score+1)>5?5:(score+1):(vld==1)&&(chng==0)&&(score>0)&&(score<5)?score-1:score' # optical data change is a score of 2 (SAR 1)
                                rsgislib.imagecalc.bandMath(scr_tile, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs, False, True)
                                rsgislib.imageutils.popImageStats(scr_tile, usenodataval=True, nodataval=0, calcpyramids=True)
                                eodd_utils.release_file_lock(scr_tile)

                                # Update the UID image
                                acq_date = scn_db_obj.Date_Acquired
                                year_obs = acq_date.year
                                day_year_obs = acq_date.timetuple().tm_yday
                                tmp_uid_tile = os.path.join(base_tmp_dir, "{}_{}_tmp_uid_tile.kea".format(basename, tile_basename))
                                update_uid_image(uid_tile, gmw_tile_chng_img, gmw_tile_vld_img, year_obs, day_year_obs, tmp_uid_tile)
                                rsgislib.imageutils.popImageStats(tmp_uid_tile, usenodataval=True, nodataval=0, calcpyramids=True)

                                # Overwrite the UID image.
                                eodd_utils.get_file_lock(uid_tile, sleep_period=1, wait_iters=120, use_except=True)
                                rsgislib.imageutils.gdal_translate(tmp_uid_tile, uid_tile, 'KEA')
                                rsgislib.imageutils.popImageStats(tmp_uid_tile, usenodataval=True, nodataval=0, calcpyramids=True)
                                eodd_utils.release_file_lock(uid_tile)

                                # Clump to create UIDs
                                tile_clumps = os.path.join(base_tmp_dir,"{}_{}_clumps.kea".format(basename, tile_basename))
                                rsgislib.segmentation.unionOfClumps(tile_clumps, 'KEA', [scr_tile, tmp_uid_tile], 0, False)
                                rsgislib.rastergis.populateStats(tile_clumps, addclrtab=True, calcpyramids=True, ignorezero=True)

                                # Masked UID clumps
                                tile_mskd_clumps = os.path.join(base_tmp_dir, "{}_{}_clumps_mskd.kea".format(basename, tile_basename))
                                band_defs = [rsgislib.imagecalc.BandDefn('score', scr_tile, 1),
                                             rsgislib.imagecalc.BandDefn('clumps', tile_clumps, 1)]
                                exp = 'score>0?clumps:0'
                                rsgislib.imagecalc.bandMath(tile_mskd_clumps, exp, 'KEA', rsgislib.TYPE_32UINT, band_defs, False, False)
                                rsgislib.rastergis.populateStats(tile_mskd_clumps, addclrtab=True, calcpyramids=True, ignorezero=True)

                                tile_mskd_relbl_clumps = os.path.join(base_tmp_dir, "{}_{}_clumps_mskd_relbl.kea".format(basename, tile_basename))
                                rsgislib.segmentation.relabelClumps(tile_mskd_clumps, tile_mskd_relbl_clumps, 'KEA', False)
                                rsgislib.rastergis.populateStats(tile_mskd_relbl_clumps, addclrtab=True, calcpyramids=True, ignorezero=True)

                                # Check there are
                                chng_hist = rsgislib.rastergis.ratutils.getColumnData(tile_mskd_relbl_clumps, "Histogram")
                                if chng_hist.shape[0] > 1:
                                    # Populate clumps with attribute info.
                                    bs = []
                                    bs.append(rsgislib.rastergis.BandAttStats(band=1, meanField='FirstObsYear'))
                                    bs.append(rsgislib.rastergis.BandAttStats(band=2, meanField='FirstObsDay'))
                                    bs.append(rsgislib.rastergis.BandAttStats(band=3, meanField='LastObsYear'))
                                    bs.append(rsgislib.rastergis.BandAttStats(band=4, meanField='LastObsDay'))
                                    rsgislib.rastergis.populateRATWithStats(tmp_uid_tile, tile_mskd_relbl_clumps, bs)

                                    # Populate clumps with attribute info.
                                    bs = []
                                    bs.append(rsgislib.rastergis.BandAttStats(band=1, meanField='Score'))
                                    rsgislib.rastergis.populateRATWithStats(scr_tile, tile_mskd_relbl_clumps, bs)

                                    obs_date_str = acq_date.strftime("%Y%m%d")
                                    obs_date_iso_str = acq_date.isoformat()

                                    out_tile_vec_file_name = "{}_{}_chngs.gpkg".format(obs_date_str, basename)
                                    out_tile_vec_file = os.path.join(self.params["out_vec_path"], out_tile_vec_file_name)
                                    rsgislib.vectorutils.polygoniseRaster2VecLyr(out_tile_vec_file, tile_basename, 'GPKG',
                                                                                 tile_mskd_relbl_clumps, imgBandNo=1,
                                                                                 maskImg=tile_mskd_relbl_clumps,
                                                                                 imgMaskBandNo=1, replace_file=False,
                                                                                 replace_lyr=True,
                                                                                 pxl_val_fieldname='ratfid')

                                    rsgislib.vectorutils.copyRATCols2VectorLyr(out_tile_vec_file, tile_basename, 'ratfid',
                                                                               tile_mskd_relbl_clumps,
                                                                                ["FirstObsYear", "FirstObsDay",
                                                                                 "LastObsYear", "LastObsDay",
                                                                                 "Score"],
                                                                               outcolnames=None, outcoltypes=None)

                                    if out_dict is None:
                                        out_dict = dict()
                                        out_dict['out_chng_vec'] = list()
                                        out_dict['out_vec_luts'] = list()
                                    out_dict['out_chng_vec'].append(out_tile_vec_file)

                                    # Update (create) the JSON LUT file.
                                    lut_file_name = "gmw_{}_lut.json".format(tile_basename)
                                    lut_file_path = os.path.join(self.params["chng_vec_luts"], lut_file_name)
                                    eodd_utils.get_file_lock(lut_file_path, sleep_period=1, wait_iters=120, use_except=True)
                                    if os.path.exists(lut_file_path):
                                        lut_dict = rsgis_utils.readJSON2Dict(lut_file_path)
                                    else:
                                        lut_dict = dict()

                                    lut_dict[obs_date_iso_str] = dict()
                                    lut_dict[obs_date_iso_str]["file"] = out_tile_vec_file_name
                                    lut_dict[obs_date_iso_str]["layer"] = tile_basename

                                    rsgis_utils.writeDict2JSON(lut_dict, lut_file_path)
                                    eodd_utils.release_file_lock(lut_file_path)
                                    out_dict['out_vec_luts'].append(lut_file_path)
                            else:
                                logger.debug("There are no change pixels within the tile skipping.")
                                success = True

                    else:
                        logger.error("There are no tiles intersecting with the change features. Need to check what's happened here; Sentinel-1 PID: {}".format(scn_db_obj.PID))
                        success = True

                    # Remove the tmp directory to clean up...
                    if os.path.exists(base_tmp_dir):
                        shutil.rmtree(base_tmp_dir)

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