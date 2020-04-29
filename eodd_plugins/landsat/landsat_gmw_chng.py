from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis

import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.imagecalc.calcindices
import rsgislib.rastergis
import rsgislib.rastergis.ratutils
import rsgislib.imageutils.imagelut

import logging
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)


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

                    basename = rsgis_utils.get_file_basename(chng_feats_vec_file)
                    basename = basename.replace('_chng_gmw_vec', '')
                    logger.debug("The basename for the processing is: {}".format(basename))

                    base_tmp_dir = os.path.join(self.params["tmp_path"], "{}_{}_gmw_chng".format(scn_db_obj.Product_ID, scn_db_obj.PID))
                    if not os.path.exists(base_tmp_dir):
                        os.mkdir(base_tmp_dir)

                    gmw_chng_wgs84_vec = os.path.join(base_tmp_dir, "{}_chng_gmw_vec_wgs84.geojson".format(basename))
                    """
                    cmd = "ogr2ogr -f GEOJSON -overwrite -nln gmw_chng -t_srs EPSG:4326 {} {} gmw_chng".format(gmw_chng_wgs84_vec, chng_feats_vec_file)
                    logger.debug("Going to run command: '{}'".format(cmd))
                    try:
                        subprocess.check_call(cmd, shell=True)
                    except OSError as e:
                        raise Exception('Failed to run command: ' + cmd)
                    """

                    lyr_bbox = rsgis_utils.getVecLayerExtent(gmw_chng_wgs84_vec, 'gmw_chng')
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