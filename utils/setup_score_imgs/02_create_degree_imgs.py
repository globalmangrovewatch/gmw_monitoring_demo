import json
import os
import rsgislib
import rsgislib.imageutils
import rsgislib.tools

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

out_scores_dir = '/Users/pete/Temp/gmw_monitoring_files/scores_imgs'
out_uid_dir = '/Users/pete/Temp/gmw_monitoring_files/uid_imgs'

rsgis_utils = rsgislib.RSGISPyUtils()

out_img_res_x, out_img_res_y = rsgislib.tools.metres_to_degrees(0.0, 20, 20)
print("{} x {}".format(out_img_res_x, out_img_res_y))
out_img_res = out_img_res_x
if out_img_res_y < out_img_res_x:
    out_img_res = out_img_res_y

wkt_str = rsgis_utils.getWKTFromEPSGCode(4326)

with open(input_file) as f:
    bboxes = json.load(f)

    for bbox in bboxes["bboxes"]:
        #print("{}".format(bbox))

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

        scr_file_name = "gmw_tile_{}{}_chng_scr.kea".format(lon_name, lat_name)
        uid_file_name = "gmw_tile_{}{}_chng_uid.kea".format(lon_name, lat_name)

        print(scr_file_name)
        print(uid_file_name)

        scr_file_path = os.path.join(out_scores_dir, scr_file_name)
        uid_file_path = os.path.join(out_uid_dir, uid_file_name)

        rsgislib.imageutils.createBlankImgFromBBOX([min_lon, max_lon, min_lat, max_lat], wkt_str, scr_file_path,
                                                   out_img_res, 0, 1, 'KEA', rsgislib.TYPE_8UINT, snap2grid=False)

        rsgislib.imageutils.createBlankImgFromBBOX([min_lon, max_lon, min_lat, max_lat], wkt_str, uid_file_path,
                                                   out_img_res, 0, 3, 'KEA', rsgislib.TYPE_32UINT, snap2grid=False)

        print("")


