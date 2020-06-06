from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib
from osgeo import gdal
from osgeo import ogr
from rios import ratapplier

import logging
import os
import datetime

logger = logging.getLogger(__name__)

gdal.UseExceptions()

def _ratapplier_check_string_col_valid(info, inputs, outputs, otherargs):
    uid_col_vals = getattr(inputs.inrat, 'CLP_UID')
    lat_col_vals = getattr(inputs.inrat, 'lat')
    lon_col_vals = getattr(inputs.inrat, 'lon')
    prop_chng_col_vals = getattr(inputs.inrat, 'prop_chng')
    score_col_vals = getattr(inputs.inrat, 'score')

    firstobs_day_col_vals = getattr(inputs.inrat, 'firstobs_day')
    firstobs_month_col_vals = getattr(inputs.inrat, 'firstobs_month')
    firstobs_year_col_vals = getattr(inputs.inrat, 'firstobs_year')

    lastobs_day_col_vals = getattr(inputs.inrat, 'lastobs_day')
    lastobs_month_col_vals = getattr(inputs.inrat, 'lastobs_month')
    lastobs_year_col_vals = getattr(inputs.inrat, 'lastobs_year')

    scr5obs_day_col_vals = getattr(inputs.inrat, 'scr5obs_day')
    scr5obs_month_col_vals = getattr(inputs.inrat, 'scr5obs_month')
    scr5obs_year_col_vals = getattr(inputs.inrat, 'scr5obs_year')

    for i in range(uid_col_vals.shape[0]):
        if prop_chng_col_vals[i] >= 0.25:
            feat = ogr.Feature(otherargs.lyr_defn)
            feat.SetField("uid", int(uid_col_vals[i]))
            feat.SetField("prop_chng", float(prop_chng_col_vals[i]))
            feat.SetField("score", int(score_col_vals[i]))

            feat.SetField("firstobsday", int(firstobs_day_col_vals[i]))
            feat.SetField("firstobsmonth", int(firstobs_month_col_vals[i]))
            feat.SetField("firstobsyear", int(firstobs_year_col_vals[i]))

            feat.SetField("lastobsday", int(lastobs_day_col_vals[i]))
            feat.SetField("lastobsmonth", int(lastobs_month_col_vals[i]))
            feat.SetField("lastobsyear", int(lastobs_year_col_vals[i]))

            feat.SetField("scr5obsday", int(scr5obs_day_col_vals[i]))
            feat.SetField("scr5obsmonth", int(scr5obs_month_col_vals[i]))
            feat.SetField("scr5obsyear", int(scr5obs_year_col_vals[i]))

            pt = ogr.Geometry(ogr.wkbPoint)
            pt.SetPoint_2D(0, lon_col_vals[i], lat_col_vals[i])

            feat.SetGeometry(pt)

            if otherargs.lyr.CreateFeature(feat) != 0:
                raise Exception("Failed to create the feature within the layer.")

            feat.Destroy()


class CreateSummaryVecFeats(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["outvecdir", "outlutdir"]
        EODataDownUserAnalysis.__init__(self, analysis_name='CreateSummaryVecFeats', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None
        success = True
        out_dict = None
        try:
            scn_ext_info = scn_db_obj.ExtendedInfo
            if (scn_ext_info is not None):
                scn_chng_info = scn_ext_info['GenChngSummaryFeats']

            if isinstance(scn_chng_info, dict):
                scn_unq_name = sen_obj.get_scn_unq_name_record(scn_db_obj)
                out_vec_file = os.path.join(self.params['outvecdir'], "{}_chng_vec.gpkg".format(scn_unq_name))

                if sen_obj.get_sensor_name() == 'LandsatGOOG':
                    scn_obs_date = scn_db_obj.Sensing_Time
                elif sen_obj.get_sensor_name() == 'Sentinel2GOOG':
                    scn_obs_date = scn_db_obj.Sensing_Time
                elif sen_obj.get_sensor_name() == 'Sentinel1ASF':
                    scn_obs_date = scn_db_obj.Acquisition_Date
                else:
                    raise Exception("")

                try:
                    import tqdm
                    progress_bar = rsgislib.TQDMProgressBar()
                except:
                    from rios import cuiprogress
                    progress_bar = cuiprogress.GDALProgressBar()

                drv = gdal.GetDriverByName("GPKG")
                if drv is None:
                    raise Exception("Driver GPKG is not avaiable.")

                ds = drv.Create(out_vec_file, 0, 0, 0, gdal.GDT_Unknown)
                if ds is None:
                    raise Exception("Could not create output file: {}.".format(out_vec_file))

                out_dict = dict()
                for tile in scn_chng_info:
                    logger.debug("Processing tile {}...".format(tile))
                    clumps_img = scn_chng_info[tile]

                    in_rats = ratapplier.RatAssociations()
                    out_rats = ratapplier.RatAssociations()
                    in_rats.inrat = ratapplier.RatHandle(clumps_img)

                    lyr = ds.CreateLayer(tile, None, ogr.wkbPoint)
                    if lyr is None:
                        raise Exception("Could not create output layer: {}.".format(tile))

                    field_uid_defn = ogr.FieldDefn("uid", ogr.OFTInteger)
                    if lyr.CreateField(field_uid_defn) != 0:
                        raise Exception("Could not create field: 'uid'.")

                    field_prop_chng_defn = ogr.FieldDefn("prop_chng", ogr.OFTReal)
                    if lyr.CreateField(field_prop_chng_defn) != 0:
                        raise Exception("Could not create field: 'prop_chng'.")

                    field_score_defn = ogr.FieldDefn("score", ogr.OFTInteger)
                    if lyr.CreateField(field_score_defn) != 0:
                        raise Exception("Could not create field: 'score'.")

                    # First Observation Date
                    field_firstobsday_defn = ogr.FieldDefn("firstobsday", ogr.OFTInteger)
                    if lyr.CreateField(field_firstobsday_defn) != 0:
                        raise Exception("Could not create field: 'firstobsday'.")

                    field_firstobsmonth_defn = ogr.FieldDefn("firstobsmonth", ogr.OFTInteger)
                    if lyr.CreateField(field_firstobsmonth_defn) != 0:
                        raise Exception("Could not create field: 'firstobsmonth'.")

                    field_firstobsyear_defn = ogr.FieldDefn("firstobsyear", ogr.OFTInteger)
                    if lyr.CreateField(field_firstobsyear_defn) != 0:
                        raise Exception("Could not create field: 'firstobsyear'.")

                    # Last Observation Date
                    field_lastobsday_defn = ogr.FieldDefn("lastobsday", ogr.OFTInteger)
                    if lyr.CreateField(field_lastobsday_defn) != 0:
                        raise Exception("Could not create field: 'lastobsday'.")

                    field_lastobsmonth_defn = ogr.FieldDefn("lastobsmonth", ogr.OFTInteger)
                    if lyr.CreateField(field_lastobsmonth_defn) != 0:
                        raise Exception("Could not create field: 'lastobsmonth'.")

                    field_lastobsyear_defn = ogr.FieldDefn("lastobsyear", ogr.OFTInteger)
                    if lyr.CreateField(field_lastobsyear_defn) != 0:
                        raise Exception("Could not create field: 'lastobsyear'.")

                    # Observation Date Where Score Reached 5
                    field_scr5obsday_defn = ogr.FieldDefn("scr5obsday", ogr.OFTInteger)
                    if lyr.CreateField(field_scr5obsday_defn) != 0:
                        raise Exception("Could not create field: 'scr5obsday'.")

                    field_scr5obsmonth_defn = ogr.FieldDefn("scr5obsmonth", ogr.OFTInteger)
                    if lyr.CreateField(field_scr5obsmonth_defn) != 0:
                        raise Exception("Could not create field: 'scr5obsmonth'.")

                    field_scr5obsyear_defn = ogr.FieldDefn("scr5obsyear", ogr.OFTInteger)
                    if lyr.CreateField(field_scr5obsyear_defn) != 0:
                        raise Exception("Could not create field: 'scr5obsyear'.")

                    lyr_defn = lyr.GetLayerDefn()

                    otherargs = ratapplier.OtherArguments()
                    otherargs.lyr = lyr
                    otherargs.lyr_defn = lyr_defn

                    ratcontrols = ratapplier.RatApplierControls()
                    ratcontrols.setProgress(progress_bar)
                    ratapplier.apply(_ratapplier_check_string_col_valid, in_rats, out_rats, otherargs, controls=ratcontrols)

                ds = None

                out_dict[tile] = out_vec_file
                success = True
            else:
                logger.debug("No change features available as outputs from previous steps...")

        except Exception as e:
            logger.debug("An error occurred during plugin processing. See stacktrace...", stack_info=True)
            logger.exception(e)
            success = False

        success = False
        return success, out_dict

