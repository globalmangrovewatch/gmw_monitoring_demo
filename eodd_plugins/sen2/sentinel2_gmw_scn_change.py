from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis

import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.imagecalc.calcindices
import rsgislib.rastergis
import rsgislib.rastergis.ratutils

import logging
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)

class Sentinel2GMWSceneChange(EODataDownUserAnalysis):
    
    def __init__(self):
        usr_req_keys = ["gmw_vec_file", "gmw_vec_lyr", "out_vec_path", "tmp_path"]
        EODataDownUserAnalysis.__init__(self, analysis_name='Sentinel2GMWScnChange', req_keys=usr_req_keys)
        
    def perform_analysis(self, scn_db_obj, sen_obj, plgin_objs):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None, False
        success = True
        outputs = False
        out_dict = None

        rsgis_utils = rsgislib.RSGISPyUtils()

        # Find the input image image.
        img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*vmsk_mclds_topshad_rad_srefdem_stdsref.tif")
        logger.debug("Image File: {}".format(img_file))

        # Find the the valid image mask
        valid_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*valid.tif")
        logger.debug("Valid Image File: {}".format(valid_img_file))

        # Find the the cloud image mask
        clearsky_img_file = rsgis_utils.findFileNone(scn_db_obj.ARDProduct_Path, "*clearsky.tif")
        logger.debug("Clear Sky Mask File: {}".format(clearsky_img_file))

        # Create basename to be used throughout analysis
        basename = rsgis_utils.get_file_basename(valid_img_file)
        basename = basename.replace('_valid', '')
        logger.debug("The basename for the processing is: {}".format(basename))

        # Create tmp directory
        base_tmp_dir = os.path.join(self.params["tmp_path"], "{}_{}_scn_chng".format(scn_db_obj.Product_ID, scn_db_obj.PID))
        if not os.path.exists(base_tmp_dir):
            os.mkdir(base_tmp_dir)

        # Create output directory
        out_dir = os.path.join(self.params["out_vec_path"], "{}_{}".format(scn_db_obj.Product_ID, scn_db_obj.PID))
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)

        # Get image BBOX as WGS84
        img_bbox = rsgis_utils.getImageBBOXInProj(img_file, 4326)

        # Get image EPSG code for projection
        img_epsg = rsgis_utils.getEPSGCode(img_file)

        gmw_ds, gmw_lyr = rsgislib.vectorutils.open_gdal_vec_lyr(self.params["gmw_vec_file"], self.params["gmw_vec_lyr"])

        # Get GMW for image BBOX
        gmw_roi_mem_ds, gmw_roi_mem_lyr = rsgislib.vectorutils.subsetEnvsVecLyrObj(gmw_lyr, img_bbox)

        # Close GMW data layer
        gmw_ds = None

        # count number of geometries in ROI layer.
        n_feats = gmw_roi_mem_lyr.GetFeatureCount(True)
        logger.debug("There are '{}' gmw geometries within BBOX of the input image.".format(n_feats))
        if n_feats > 0:
            logger.debug("Proceeding with analysis as image intersects with some GMW polygons.")
            # Write the ROI GMW layer to disk.
            gmw_roi_vec_file = os.path.join(base_tmp_dir, "{}_gmw_vec_lyr.geojson".format(basename))
            rsgislib.vectorutils.writeVecLyr2File(gmw_roi_mem_lyr, gmw_roi_vec_file, 'gmw_roi', 'GEOJSON', options=['OVERWRITE=YES'], replace=True)

            # Close ROI memory layer
            gmw_roi_mem_ds = None

            # Reproject the ROI GMW Layer
            gmw_roi_prj_vec_file = os.path.join(base_tmp_dir, "{}_gmw_vec_lyr_prj.geojson".format(basename))
            cmd = "ogr2ogr -f GEOJSON -overwrite -nln gmw_roi -t_srs EPSG:{} {} {} gmw_roi".format(img_epsg, gmw_roi_prj_vec_file, gmw_roi_vec_file)
            logger.debug("Going to run command: '{}'".format(cmd))
            try:
                subprocess.check_call(cmd, shell=True)
            except OSError as e:
                raise Exception('Failed to run command: ' + cmd)

            # Rasterise the GMW.
            gmw_base_msk_img = os.path.join(base_tmp_dir, "{}_gmw_base.kea".format(basename))
            rsgislib.vectorutils.rasteriseVecLyr(gmw_roi_prj_vec_file, 'gmw_roi', img_file,
                                                 gmw_base_msk_img, gdalformat="KEA", burnVal=1)

            # Do band maths to limit the GMW extent to areas of valid data.
            gmw_msk_img = os.path.join(base_tmp_dir, "{}_gmw.kea".format(basename))
            band_defs = [rsgislib.imagecalc.BandDefn('valid', valid_img_file, 1),
                         rsgislib.imagecalc.BandDefn('clrsky', clearsky_img_file, 1),
                         rsgislib.imagecalc.BandDefn('gmw', gmw_base_msk_img, 1)]
            exp = '(gmw==1) && (valid==1) && (clrsky==1)?1:0'
            rsgislib.imagecalc.bandMath(gmw_msk_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs)
            rsgislib.rastergis.populateStats(gmw_msk_img, addclrtab=True, calcpyramids=True, ignorezero=True)

            # Check there are valid pixels.
            pxl_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_msk_img, 'Histogram')
            if pxl_counts.shape[0] > 1:
                n_gmw_pxls = pxl_counts[1]
            else:
                n_gmw_pxls = 0
            logger.debug("There are '{}' gmw pixels within the valid area of the input image.".format(n_gmw_pxls))

            if n_gmw_pxls > 100:
                out_dict = dict()
                out_dict["gmw_scn_pxls"] = n_gmw_pxls
                logger.debug("There are sufficient GMW pixels to continue analysis")

                # Calculate NDVI
                rBand = 3
                nBand = 7
                ndvi_img = os.path.join(base_tmp_dir, "{}_ndvi.kea".format(basename))
                rsgislib.imagecalc.calcindices.calcNDVI(img_file, rBand, nBand, ndvi_img, stats=False, gdalformat='KEA')

                # Threshold the NDVI within GMW area.
                gmw_chng_img = os.path.join(base_tmp_dir, "{}_chng_gmw.kea".format(basename))
                band_defs = [rsgislib.imagecalc.BandDefn('ndvi', ndvi_img, 1),
                             rsgislib.imagecalc.BandDefn('gmw', gmw_msk_img, 1)]
                exp = '(gmw==1) && (ndvi<0.2)?1:0'
                rsgislib.imagecalc.bandMath(gmw_chng_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs)
                rsgislib.rastergis.populateStats(gmw_chng_img, addclrtab=True, calcpyramids=True, ignorezero=True)

                chng_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_chng_img, 'Histogram')
                if chng_counts.shape[0] > 1:
                    n_gmw_chng_pxls = chng_counts[1]
                else:
                    n_gmw_chng_pxls = 0
                logger.debug("There are '{}' change gmw pixels.".format(n_gmw_chng_pxls))
                out_dict["gmw_scn_chng_pxls"] = n_gmw_chng_pxls

                if n_gmw_chng_pxls > 0:
                    logger.debug("There are change gmw pixels to continue processing.")

                    # Vectorise the change features.
                    gmw_chng_vec = os.path.join(out_dir, "{}_chng_gmw_vec.geojson".format(basename))
                    rsgislib.vectorutils.polygoniseRaster2VecLyr(gmw_chng_vec, 'gmw_chng', 'GEOJSON', gmw_chng_img,
                                                                 imgBandNo=1, maskImg=gmw_chng_img, imgMaskBandNo=1,
                                                                 replace_file=True, replace_lyr=True,
                                                                 pxl_val_fieldname='PXLVAL')
                    out_dict["chng_feats_vec"] = gmw_chng_vec
                    out_dict["chng_found"] = True
                    outputs = True
                else:
                    out_dict["chng_found"] = False
            else:
                logger.debug("There were insufficient GMW pixels to continue analysis")
                success = True
        else:
            logger.debug("NOT proceeding with analysis as the image does not intersects with any GMW polygons.")
            # Close ROI memory layer
            gmw_roi_mem_ds = None
            success = True

        # Remove the tmp directory to clean up...
        if os.path.exists(base_tmp_dir):
            shutil.rmtree(base_tmp_dir)

        return success, out_dict, outputs

