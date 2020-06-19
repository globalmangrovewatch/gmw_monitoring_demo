import json
import os
import rsgislib
import rsgislib.vectorutils
import rsgislib.tools
import osgeo.ogr as ogr

def zero_pad_num_str(num_val, str_len=3, round_num=False, round_n_digts=0, integerise=False):
    """
    A function which zero pads a number to make a string

    :param num_val: number value to be processed.
    :param str_len: the number of characters in the output string.
    :param round_num: boolean whether to round the input number value.
    :param round_n_digts: If rounding, the number of digits following decimal points to round to.
    :param integerise: boolean whether to integerise the input number
    :return: string with the padded numeric value.

    """
    if round_num:
        num_val = round(num_val, round_n_digts)
    if integerise:
        num_val = int(num_val)

    num_str = "{}".format(num_val)
    num_str = num_str.zfill(str_len)
    return num_str

input_file = 'img_bboxes.json'

out_vec_file = '/Users/pete/Temp/gmw_monitoring_files/gmw_chng_alert_tile_lut.geojson'
out_vec_lyr = 'gmw_chng_alert_tile_lut'

rsgis_utils = rsgislib.RSGISPyUtils()

out_img_res_x, out_img_res_y = rsgislib.tools.metres_to_degrees(0.0, 20, 20)
print("{} x {}".format(out_img_res_x, out_img_res_y))
out_img_res = out_img_res_x
if out_img_res_y < out_img_res_x:
    out_img_res = out_img_res_y

wkt_str = rsgis_utils.getWKTFromEPSGCode(4326)

with open(input_file) as f:
    bboxes = json.load(f)
    tile_names = list()
    for bbox in bboxes["bboxes"]:
        min_lon = int(round(bbox[0]))
        max_lon = int(round(bbox[1]))
        min_lat = int(round(bbox[2]))
        max_lat = int(round(bbox[3]))

        print("({}, {}) ({}, {})".format(min_lon, max_lon, min_lat, max_lat))

        if (max_lon-min_lon) != 1:
            raise Exception("Longtitude does not have a range of 1...")
        if (max_lat-min_lat) != 1:
            raise Exception("Latitude does not have a range of 1...")

        if min_lon < 0:
            lon_name = "e{}".format(zero_pad_num_str(min_lon*-1, 3))
        else:
            lon_name = "w{}".format(zero_pad_num_str(min_lon, 3))

        if max_lat < 0:
            lat_name = "s{}".format(zero_pad_num_str(max_lat*-1, 2))
        else:
            lat_name = "n{}".format(zero_pad_num_str(max_lat, 2))

        tile_name = "tile_{}{}".format(lon_name, lat_name)
        tile_names.append(tile_name)

atts = dict()
atts['tile'] = tile_names
atts_types = dict()
atts_types['names'] = ['tile']
atts_types['types'] = [ogr.OFTString]

rsgislib.vectorutils.createPolyVecBBOXs(out_vec_file, out_vec_lyr, 'GEOJSON', 4326, bboxes["bboxes"], atts=atts, attTypes=atts_types, overwrite=True)