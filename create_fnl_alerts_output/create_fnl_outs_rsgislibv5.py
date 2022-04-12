import rsgislib.tools.utils
import glob
import os
import datetime
import geopandas
import numpy

def create_fnl_alert_lyrs(luts_dir, vec_dir, out_vec_file, update_date):
    """

    :param luts_dir:
    :param vec_dir:
    :param out_vec_file:
    :param update_date: The date at the start of the period of interest. Alerts after this date will be returned
    :return:
    """
    lut_files = glob.glob(os.path.join(luts_dir, "*.json"))

    base_date = datetime.datetime(year=1970, month=1, day=1)
    update_day_since_base = (update_date - base_date).days
    print(update_day_since_base)

    for lut_file in lut_files:
        print(lut_file)
        lut_dict = rsgislib.tools.utils.read_json_to_dict(lut_file)
        first = True
        last_key = ''
        last_key_dt = None
        for lut_key in lut_dict:
            #print("\t{}".format(lut_key))
            lut_key_dt = datetime.datetime.fromisoformat(lut_key)
            if first:
                last_key = lut_key
                last_key_dt = lut_key_dt
                first = False
            else:
                if lut_key_dt > last_key_dt:
                    last_key = lut_key
                    last_key_dt = lut_key_dt

        if not first:
            print("Last Key: {}".format(last_key))
            tile_vec_file = lut_dict[last_key]['file']
            tile_vec_file_name = os.path.basename(tile_vec_file)
            tile_vec_file = os.path.join(vec_dir, tile_vec_file_name)
            if not os.path.exists(tile_vec_file):
                raise Exception("Could not file vector file '{}' at path: {}".format(tile_vec_file_name, tile_vec_file))

            tile_vec_lyr = lut_dict[last_key]['layer']
            print("\t{}".format(tile_vec_file))
            print("\t{}".format(tile_vec_lyr))

            base_gpdf = geopandas.read_file(tile_vec_file, layer=tile_vec_lyr)

            if base_gpdf.shape[0] > 0:
                #print(base_gpdf.shape)
                days_col = numpy.zeros((base_gpdf.shape[0]), dtype=int)
                for i in range(base_gpdf.shape[0]):
                    #print(i)
                    #print(base_gpdf.loc[i])
                    scr5obsday = base_gpdf.loc[i, 'scr5obsday']
                    scr5obsmonth = base_gpdf.loc[i, 'scr5obsmonth']
                    scr5obsyear = base_gpdf.loc[i, 'scr5obsyear']
                    if (scr5obsday == 0) or (scr5obsmonth == 0) or (scr5obsyear == 0):
                        days_col[i] = 0
                    else:
                        c_date = datetime.datetime(year=scr5obsyear, month=scr5obsmonth, day=scr5obsday)
                        days_col[i] = (c_date - base_date).days
                #print(days_col)
                base_gpdf = base_gpdf[days_col > update_day_since_base]

            base_gpdf = base_gpdf[base_gpdf['score'] == 5]

            #print(base_gpdf.shape)
            #print(base_gpdf)
            if base_gpdf.shape[0] > 0:
                base_gpdf.to_file(out_vec_file, layer=tile_vec_lyr, driver='GPKG')

        print("")
        #break



#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_6m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_6m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_6m/gmw_chng_alerts_201901_201906.gpkg",
#                      datetime.datetime(year=2019, month=1, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_12m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_12m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_12m/gmw_chng_alerts_201906_201912.gpkg",
#                      datetime.datetime(year=2019, month=7, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_18m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_18m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_18m/gmw_chng_alerts_202001_202006.gpkg",
#                      datetime.datetime(year=2020, month=1, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_22m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_22m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_22m/gmw_chng_alerts_202007_202009.gpkg",
#                      datetime.datetime(year=2020, month=7, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_23m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_23m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_23m/gmw_chng_alerts_202011.gpkg",
#                      datetime.datetime(year=2020, month=9, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_24m/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_24m/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_24m/gmw_chng_alerts_202012.gpkg",
#                      datetime.datetime(year=2020, month=11, day=1))

#create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202101/gmw_chng_fnl_tile_luts",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202101/gmw_chng_fnl_vecs",
#                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202101/gmw_chng_alerts_202101.gpkg",
#                      datetime.datetime(year=2020, month=12, day=1))


create_fnl_alert_lyrs("/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202106/gmw_chng_fnl_tile_luts",
                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202106/gmw_chng_fnl_vecs",
                      "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202106/gmw_chng_alerts_202106.gpkg",
                      datetime.datetime(year=2021, month=6, day=1))

