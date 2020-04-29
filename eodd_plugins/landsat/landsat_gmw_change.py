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

def vector_translate(in_vec_file, in_vec_lyr, out_vec_file, out_vec_lyr=None, out_vec_drv='GPKG',
                     drv_create_opts=[], lyr_create_opts=[], access_mode='overwrite', src_srs=None,
                     dst_srs=None, force=False):
    """
    A function which translates a vector file to another format, similar to ogr2ogr. If you wish
    to reproject the input file then provide a destination srs (e.g., "EPSG:27700", or wkt string,
    or proj4 string).

    :param in_vec_file: the input vector file.
    :param in_vec_lyr: the input vector layer name
    :param out_vec_file: the output vector file.
    :param out_vec_lyr: the name of the output vector layer (if None then the same as the input).
    :param out_vec_drv: the output vector file format (e.g., GPKG, GEOJSON, ESRI Shapefile, etc.)
    :param drv_create_opts: a list of options for the creation of the output file.
    :param lyr_create_opts: a list of options for the creation of the output layer.
    :param access_mode: by default the function overwrites the output file but other
                        options are: ['update', 'append', 'overwrite']
    :param src_srs: provide a source spatial reference for the input vector file. Default=None.
                    can be used to provide a projection where none has been specified or the
                    information has gone missing. Can be used without performing a reprojection.
    :param dst_srs: provide a spatial reference for the output image to be reprojected to. (Default=None)
                    If specified then the file will be reprojected.
    :param force: remove output file if it exists.

    """
    from osgeo import gdal
    gdal.UseExceptions()

    if os.path.exists(out_vec_file):
        if force:
            ds_in_vec = gdal.OpenEx(out_vec_file, gdal.OF_READONLY)
            if ds_in_vec is None:
                raise Exception("Could not open '{}'".format(out_vec_file))
            file_lst = ds_in_vec.GetFileList()
            for cfile in file_lst:
                logger.debug("Deleting: {}".format(cfile))
                os.remove(cfile)
        else:
            raise Exception("The output vector file ({}) already exists, remove it and re-run.".format(out_vec_file))

    if out_vec_lyr is None:
        out_vec_lyr = in_vec_lyr

    reproject_lyr = False
    if dst_srs is not None:
        reproject_lyr = True

    if src_srs is not None:
        if dst_srs is None:
            dst_srs = src_srs

    opts = gdal.VectorTranslateOptions(options=drv_create_opts,
                                       format=out_vec_drv,
                                       accessMode=access_mode,
                                       srcSRS=src_srs,
                                       dstSRS=dst_srs,
                                       reproject=reproject_lyr,
                                       layerCreationOptions=lyr_create_opts,
                                       layers=in_vec_lyr,
                                       layerName=out_vec_lyr)

    gdal.VectorTranslate(out_vec_file, in_vec_file, options=opts)

class LandsatGMWChange(EODataDownUserAnalysis):
    
    def __init__(self):
        usr_req_keys = ["gmw_vec_file", "gmw_vec_lyr", "chng_score", "out_vec_path", "tmp_path"]
        EODataDownUserAnalysis.__init__(self, analysis_name='LandsatGMWChangeTest', req_keys=usr_req_keys)
        
    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        success = True
        out_dict = None
        try:
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
            base_tmp_dir = os.path.join(self.params["tmp_path"], basename)
            if not os.path.exists(base_tmp_dir):
                os.mkdir(base_tmp_dir)

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
                    logger.debug("There are sufficient GMW pixels to continue analysis")

                    # Calculate NDVI
                    rBand = 3
                    nBand = 4
                    if "LS8" in basename:
                        rBand = 4
                        nBand = 5
                    ndvi_img = os.path.join(base_tmp_dir, "{}_ndvi.kea".format(basename))
                    rsgislib.imagecalc.calcindices.calcNDVI(img_file, rBand, nBand, ndvi_img, stats=False, gdalformat='KEA')

                    # Threshold the NDVI within GMW area.
                    gmw_chng_img = os.path.join(base_tmp_dir, "{}_chng_gmw.kea".format(basename))
                    band_defs = [rsgislib.imagecalc.BandDefn('ndvi', ndvi_img, 1),
                                 rsgislib.imagecalc.BandDefn('gmw', gmw_msk_img, 1)]
                    exp = '(gmw==1) && (ndvi<0.4)?1:0'
                    rsgislib.imagecalc.bandMath(gmw_chng_img, exp, 'KEA', rsgislib.TYPE_8UINT, band_defs)
                    rsgislib.rastergis.populateStats(gmw_chng_img, addclrtab=True, calcpyramids=True, ignorezero=True)

                    chng_counts = rsgislib.rastergis.ratutils.getColumnData(gmw_chng_img, 'Histogram')
                    if chng_counts.shape[0] > 1:
                        n_gmw_chng_pxls = chng_counts[1]
                    else:
                        n_gmw_chng_pxls = 0
                    logger.debug("There are '{}' change gmw pixels.".format(n_gmw_chng_pxls))

                    if n_gmw_chng_pxls > 0:
                        logger.debug("There are change gmw pixels to continue processing.")

                        # Vectorise the change features.
                        gmw_chng_vec = os.path.join(base_tmp_dir, "{}_chng_gmw_vec.geojson".format(basename))
                        rsgislib.vectorutils.polygoniseRaster2VecLyr(gmw_chng_vec, 'gmw_chng', 'GEOJSON', gmw_chng_img,
                                                                     imgBandNo=1, maskImg=gmw_chng_img, imgMaskBandNo=1,
                                                                     replace_file=True, replace_lyr=True,
                                                                     pxl_val_fieldname='PXLVAL')

                        gmw_chng_wgs84_vec = os.path.join(base_tmp_dir, "{}_chng_gmw_vec_wgs84.geojson".format(basename))
                        cmd = "ogr2ogr -f GEOJSON -overwrite -nln gmw_chng -t_srs EPSG:4326 {} {} gmw_chng".format(gmw_chng_wgs84_vec, gmw_chng_vec)
                        logger.debug("Going to run command: '{}'".format(cmd))
                        try:
                            subprocess.check_call(cmd, shell=True)
                        except OSError as e:
                            raise Exception('Failed to run command: ' + cmd)


                else:
                    logger.debug("There were insufficient GMW pixels to continue analysis")
                    success = False
            else:
                logger.debug("NOT proceeding with analysis as the image does not intersects with any GMW polygons.")
                # Close ROI memory layer
                gmw_roi_mem_ds = None
                success = False

            # Remove the tmp directory to clean up...
            #if os.path.exists(base_tmp_dir):
            #    shutil.rmtree(base_tmp_dir)

        except Exception as e:
            logger.debug("An error occurred during plugin processing. See stacktrace...", stack_info=True)
            logger.exception(e)
            success = False

        success = False
        return success, out_dict



"""
{
    "path":"/Users/pete/Temp/eodd_user_analysis/scripts/plugins/landsat",
    "module":"landsat_gmw_change",
    "class":"LandsatGMWChange",
    "params":
    {
        "gmw_vec_file":"/Users/pete/Dropbox/University/Research/Data/Mangroves/GMW/GMW_MangroveExtent_WGS84_v2.0.gpkg",
        "gmw_vec_lyr":"gmw2016v2.0",
        "chng_score":"",
        "out_vec_path":"/Users/pete/Temp/eodd_user_analysis/vec_outs",
        "tmp_path":"/Users/pete/Temp/eodd_user_analysis/plugin_tmp"
        
    }
}
"""