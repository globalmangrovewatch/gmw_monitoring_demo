from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib
import rsgislib.segmentation
import rsgislib.rastergis

import logging
import os
import shutil
import datetime

logger = logging.getLogger(__name__)


def findminclumpval(clumpsImg, valsImg, valBand, out_col):
    import rsgislib.rastergis
    import rsgislib.rastergis.ratutils
    from rios import applier
    import numpy
    try:
        import tqdm
        progress_bar = rsgislib.TQDMProgressBar()
    except:
        from rios import cuiprogress
        progress_bar = cuiprogress.GDALProgressBar()

    n_clumps = rsgislib.rastergis.getRATLength(clumpsImg)
    min_vals = numpy.zeros(n_clumps, dtype=int)

    infiles = applier.FilenameAssociations()
    infiles.clumpsImg = clumpsImg
    infiles.valsImg = valsImg
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    otherargs.min_vals = min_vals
    otherargs.valBand = valBand - 1
    aControls = applier.ApplierControls()
    aControls.progress = progress_bar
    aControls.omitPyramids = True
    aControls.calcStats = False

    def _findMinVals(info, inputs, outputs, otherargs):
        """
        This is an internal rios function
        """
        unqClumpVals = numpy.unique(inputs.clumpsImg)
        clumpValsFlat = inputs.clumpsImg.flatten()
        for clump in unqClumpVals:
            pxl_vals = inputs.valsImg[otherargs.valBand].flatten()[clumpValsFlat == clump]
            pxl_vals = pxl_vals[pxl_vals > 0]
            if pxl_vals.shape[0] > 0:
                clump_min_val = pxl_vals.min()
                if otherargs.min_vals[clump] == 0:
                    otherargs.min_vals[clump] = clump_min_val
                else:
                    if clump_min_val < otherargs.min_vals[clump]:
                        otherargs.min_vals[clump] = clump_min_val

    applier.apply(_findMinVals, infiles, outfiles, otherargs, controls=aControls)

    rsgislib.rastergis.ratutils.setColumnData(clumpsImg, out_col, min_vals)


def get_days_since(year, dayofyear, base_date):
    if year < base_date.year:
        raise Exception("The year specified is before the base date.")
    date_val = datetime.date(year=int(year), month=1, day=1)
    date_val = date_val + datetime.timedelta(days=int(dayofyear - 1))
    return (date_val - base_date).days

"""
def convert_dates(uid_img, out_days_img, base_year=1970):
    from rios import applier
    import numpy
    try:
        import tqdm
        progress_bar = rsgislib.TQDMProgressBar()
    except:
        from rios import cuiprogress
        progress_bar = cuiprogress.GDALProgressBar()

    infiles = applier.FilenameAssociations()
    infiles.uid_img = uid_img
    outfiles = applier.FilenameAssociations()
    outfiles.out_days_img = out_days_img
    otherargs = applier.OtherInputs()
    otherargs.base_date = datetime.date(base_year, 1, 1)
    aControls = applier.ApplierControls()
    aControls.progress = progress_bar
    aControls.omitPyramids = True
    aControls.calcStats = False

    def _update_dates_img(info, inputs, outputs, otherargs):
        uid_img_arr = numpy.copy(inputs.uid_img)
        uid_shp = uid_img_arr.shape
        outputs.out_days_img = numpy.zeros((3, uid_shp[1], uid_shp[2]), dtype=numpy.int32)

        for y in range(uid_shp[2]):
            for x in range(uid_shp[1]):
                if numpy.max(uid_img_arr[..., x, y]) > 0:
                    if (uid_img_arr[0, x, y] > 0) and (uid_img_arr[1, x, y] >= 0):
                        outputs.out_days_img[0, x, y] = get_days_since(uid_img_arr[0, x, y], uid_img_arr[1, x, y], otherargs.base_date)
                    if (uid_img_arr[2, x, y] > 0) and (uid_img_arr[3, x, y] >= 0):
                        outputs.out_days_img[1, x, y] = get_days_since(uid_img_arr[2, x, y], uid_img_arr[3, x, y], otherargs.base_date)
                    if (uid_img_arr[4, x, y] > 0) and (uid_img_arr[5, x, y] >= 0):
                        outputs.out_days_img[2, x, y] = get_days_since(uid_img_arr[4, x, y], uid_img_arr[5, x, y], otherargs.base_date)

    applier.apply(_update_dates_img, infiles, outfiles, otherargs, controls=aControls)
"""

def create_uid_col(clumps_img, col_name='UID'):
    import numpy
    n_rows = rsgislib.rastergis.getRATLength(clumps_img)
    uid_col = numpy.arange(0, n_rows, 1, dtype=numpy.uint32)
    rsgislib.rastergis.ratutils.setColumnData(clumps_img, col_name, uid_col)

def create_date_columns_from_days_col(clumps_img, days_col, base_date, day_col, month_col, year_col):
    import numpy
    import tqdm
    # Get the days column...
    days_col_data = rsgislib.rastergis.ratutils.getColumnData(clumps_img, days_col)
    day_out_col = numpy.zeros_like(days_col_data, dtype=numpy.int16)
    month_out_col = numpy.zeros_like(days_col_data, dtype=numpy.int16)
    year_out_col = numpy.zeros_like(days_col_data, dtype=numpy.int16)
    for i in tqdm.tqdm(range(days_col_data.shape[0])):
        if days_col_data[i] > 0:
            date_val = base_date + datetime.timedelta(days=int(days_col_data[i] - 1))
            day_out_col[i] = date_val.day
            month_out_col[i] = date_val.month
            year_out_col[i] = date_val.year

    rsgislib.rastergis.ratutils.setColumnData(clumps_img, day_col, day_out_col)
    rsgislib.rastergis.ratutils.setColumnData(clumps_img, month_col, month_out_col)
    rsgislib.rastergis.ratutils.setColumnData(clumps_img, year_col, year_out_col)

class GenChngSummaryFeats(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["outdir", "tmpdir"]
        EODataDownUserAnalysis.__init__(self, analysis_name='GenChngSummaryFeats', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None
        success = True
        out_dict = None
        rsgis_utils = rsgislib.RSGISPyUtils()
        try:
            scn_ext_info = scn_db_obj.ExtendedInfo
            if (scn_ext_info is not None):
                scn_chng_info = None
                if 'LandsatGMWScnChange' in scn_ext_info:
                    scn_chng_info = scn_ext_info['LandsatGMWChangeFnl']
                elif 'Sentinel1GMWChangeFnl' in scn_ext_info:
                    scn_chng_info = scn_ext_info['Sentinel1GMWChangeFnl']
                elif 'Sentinel2GMWChangeFnl' in scn_ext_info:
                    scn_chng_info = scn_ext_info['Sentinel2GMWChangeFnl']
                else:
                    raise Exception("The previous step creating the final change features on a 1 degree grid has not been executed.")

            if isinstance(scn_chng_info, dict):
                scn_unq_name = sen_obj.get_scn_unq_name_record(scn_db_obj)
                base_tmp_dir = os.path.join(self.params["tmpdir"], "{}_tmp".format(scn_unq_name))
                if not os.path.exists(base_tmp_dir):
                    os.mkdir(base_tmp_dir)

                out_scn_dir = os.path.join(self.params["outdir"], "{}_chng_summary".format(scn_unq_name))
                if not os.path.exists(out_scn_dir):
                    os.mkdir(out_scn_dir)

                out_dict = dict()

                for tile in scn_chng_info:
                    logger.debug("Processing tile {}...".format(tile))
                    if len(scn_chng_info[tile]) > 0:
                        logger.debug("{} has data...".format(tile))
                        if ('uid' in scn_chng_info[tile]) and ('score' in scn_chng_info[tile]):
                            uid_tile_img = scn_chng_info[tile]['uid']
                            score_tile_img = scn_chng_info[tile]['score']
                        elif 'score' in scn_chng_info[tile]:
                            # This is a hack to get around a bug in an earlier version of the previous plugin...
                            uid_tile_img = scn_chng_info[tile]['score']
                            scr_base_name = rsgis_utils.get_file_basename(uid_tile_img).replace("uid", "score")
                            in_data_dir = os.path.dirname(uid_tile_img)
                            score_tile_img = rsgis_utils.findFileNone(in_data_dir, "{}*.kea".format(scr_base_name))
                            if score_tile_img is None:
                                raise Exception("Could not find the score image for tile '{}' which goes with the UID image: {}".format(tile, uid_tile_img))
                        else:
                            import pprint
                            pprint(scn_chng_info[tile])
                            raise Exception("Something has gone wrong there should either be just a score key (which is a bug version) or both uid and score keys -- corrected version.")

                        scn_base_name = rsgis_utils.get_file_basename(score_tile_img)
                        scn_base_name = scn_base_name.replace('_score', '')
                        sum_chng_img = os.path.join(out_scn_dir, "{}_chng.kea".format(scn_base_name))
                        rsgislib.segmentation.generateRegularGrid(score_tile_img, sum_chng_img, 'KEA', 3, 3, 0)
                        rsgislib.rastergis.populateStats(sum_chng_img, addclrtab=True, calcpyramids=True, ignorezero=True)
                        create_uid_col(sum_chng_img, col_name='CLP_UID')
                        rsgislib.rastergis.spatialLocation(sum_chng_img, eastings="lon", northings="lat")  # WGS84 flips the axis'.
                        rsgislib.rastergis.populateRATWithPropValidPxls(score_tile_img, sum_chng_img, outcolsname='prop_chng', nodataval=0.0)
                        rsgislib.rastergis.populateRATWithMode(score_tile_img, sum_chng_img, outcolsname='score', usenodata=True, nodataval=0, outnodata=0, modeband=1, ratband=1)
                        #############
                        # Removed as changed earlier script to output in days since 1970/1/1. Calling this function is slow.
                        #days_img = os.path.join(base_tmp_dir, "{}_day_dates.kea".format(scn_base_name))
                        #if os.path.exists(days_img):
                        #    rsgis_utils.deleteFileWithBasename(days_img)
                        #convert_dates(uid_tile_img, days_img)
                        #rsgislib.imageutils.popImageStats(days_img, usenodataval=True, nodataval=0, calcpyramids=False)
                        ##############
                        bs = []
                        bs.append(rsgislib.rastergis.BandAttStats(band=4, maxField='lastobs1970days'))
                        rsgislib.rastergis.populateRATWithStats(uid_tile_img, sum_chng_img, bs)
                        findminclumpval(sum_chng_img, uid_tile_img, 2, 'firstobs1970days')
                        findminclumpval(sum_chng_img, uid_tile_img, 6, 'scr5obs1970days')
                        base_date = datetime.date(1970, 1, 1)
                        create_date_columns_from_days_col(sum_chng_img, 'firstobs1970days', base_date, 'firstobs_day', 'firstobs_month', 'firstobs_year')
                        create_date_columns_from_days_col(sum_chng_img, 'lastobs1970days', base_date, 'lastobs_day', 'lastobs_month', 'lastobs_year')
                        create_date_columns_from_days_col(sum_chng_img, 'scr5obs1970days', base_date, 'scr5obs_day', 'scr5obs_month', 'scr5obs_year')
                        out_dict[tile] = sum_chng_img

                # Remove the tmp directory to clean up...
                if os.path.exists(base_tmp_dir):
                    shutil.rmtree(base_tmp_dir)

                success = True
            else:
                logger.debug("No change features available as outputs from previous steps...")

        except Exception as e:
            logger.debug("An error occurred during plugin processing. See stacktrace...", stack_info=True)
            logger.exception(e)
            success = False

        return success, out_dict

