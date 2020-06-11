from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib
import rsgislib.imagecalibration

import logging
import os
import shutil

logger = logging.getLogger(__name__)


class LandsatClearSky(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["tmp_path"]
        EODataDownUserAnalysis.__init__(self, analysis_name='LandsatClearSky', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj, plgin_objs):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None, False
        success = True
        outputs = False
        out_info = None

        rsgis_utils = rsgislib.RSGISPyUtils()
        eodd_utils = EODataDownUtils()

        # Find the the valid image mask
        valid_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*valid.tif")
        logger.debug("Valid Image File: {}".format(valid_img_file))

        # Find the the cloud image mask
        cloud_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*clouds.tif")
        logger.debug("Cloud Mask File: {}".format(cloud_img_file))

        basename = rsgis_utils.get_file_basename(valid_img_file)
        basename = basename.replace('_valid', '')
        logger.debug("The basename for the processing is: {}".format(basename))

        base_tmp_dir = os.path.join(self.params["tmp_path"], "{}_{}_clearsky".format(scn_db_obj.Product_ID, scn_db_obj.PID))
        if not os.path.exists(base_tmp_dir):
            os.mkdir(base_tmp_dir)

        process_tmp_dir = os.path.join(base_tmp_dir, "processing")
        if not os.path.exists(process_tmp_dir):
            os.mkdir(process_tmp_dir)

        clear_sky_img = os.path.join(base_tmp_dir, "{}_clearsky.kea".format(basename))

        rsgislib.imagecalibration.calcClearSkyRegions(cloud_img_file, valid_img_file, clear_sky_img, 'KEA',
                                                      tmpPath=process_tmp_dir, deleteTmpFiles=False,
                                                      initClearSkyRegionDist=5000, initClearSkyRegionMinSize=3000,
                                                      finalClearSkyRegionDist=1000, morphSize=21)

        clear_sky_tif_img = eodd_utils.translateCloudOpGTIFF(clear_sky_img, scn_db_obj.ARDProduct_Path)

        if os.path.exists(base_tmp_dir):
            shutil.rmtree(base_tmp_dir)

        outputs = True
        out_info = {"clearskyimg": clear_sky_tif_img}

        return success, out_info, outputs
