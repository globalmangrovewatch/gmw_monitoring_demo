import eodatadown.eodatadownsystemmain
import eodatadown.eodatadownutils
import rsgislib
import rsgislib.imageutils
import rsgislib.vectorutils
import rsgislib
import datetime
import glob
import os


def extract_sen1_img_data(eodd_config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir, stch_stats_file, scn_json_file):
    rsgis_utils = rsgislib.RSGISPyUtils()
    bbox = rsgis_utils.getVecLayerExtent(roi_vec_file, roi_vec_lyr)
    bbox_epsg = rsgis_utils.getProjEPSGFromVec(roi_vec_file, roi_vec_lyr)
    if bbox_epsg != 4326:
        bbox_wgs84 = rsgis_utils.reprojBBOX_epsg(bbox, bbox_epsg, 4326)
    else:
        bbox_wgs84 = bbox
    
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(eodd_config_file)

    sen_obj = sys_main_obj.get_sensor_obj("Sentinel1ASF")    
    scns = sen_obj.query_scn_records_date_bbox(start_date, end_date, bbox_wgs84, start_rec=0, n_recs=0, valid=True, cloud_thres=None)
    print("N scns = {}".format(len(scns)))

    n_img = 1
    scns_dict = dict()
    for scn in scns:
        print(scn.ARDProduct_Path)
        scn_date_str = sen_obj.get_scn_obs_date(scn.PID).isoformat()
        try:
            scn_dB_file = glob.glob(os.path.join(scn.ARDProduct_Path, "*dB_osgb.tif"))
            if len(scn_dB_file) == 1:
                scn_dB_file = scn_dB_file[0]
            else:
                raise Exception("There should only be 1 dB image for the scene.")

            basename = os.path.splitext(os.path.basename(scn_dB_file))[0]
            scn_dir = os.path.join(out_dir, "{}_{}".format(scn.Product_File_ID, scn.PID))
            if not os.path.exists(scn_dir):
                os.mkdir(scn_dir)
                
            scn_tmp_dir = os.path.join(tmp_dir, "{}_{}".format(scn.Product_File_ID, scn.PID))
            if not os.path.exists(scn_tmp_dir):
                os.mkdir(scn_tmp_dir)

            img_epsg = rsgis_utils.getEPSGCode(scn_dB_file)
            bbox_img_proj = rsgis_utils.reprojBBOX_epsg(bbox, bbox_epsg, img_epsg)
            rsgis_dtype = rsgis_utils.getRSGISLibDataTypeFromImg(scn_dB_file)

            sub_img = os.path.join(scn_tmp_dir, "{}_sub.kea".format(basename))
            rsgislib.imageutils.subsetbbox(scn_dB_file, sub_img, 'KEA', rsgis_dtype, bbox_img_proj[0], bbox_img_proj[1], bbox_img_proj[2], bbox_img_proj[3])
            rsgislib.imageutils.popImageStats(sub_img, usenodataval=True, nodataval=32767, calcpyramids=True)

            stch_img = os.path.join(out_dir, "{}_sen1_img.png".format(n_img))
            rsgislib.imageutils.stretchImageWithStats(sub_img, stch_img, stch_stats_file, 'PNG', rsgislib.TYPE_8UINT, rsgislib.imageutils.STRETCH_LINEARSTDDEV, 2)
            scns_dict[scn_date_str] = stch_img
            print("")
        except:
            print("ERROR Caught but ignored")
    rsgis_utils.writeDict2JSON(scns_dict, scn_json_file)
                    
os.environ["RSGISLIB_IMG_CRT_OPTS_GTIFF"] = "TILED=YES:COMPRESS=LZW:BIGTIFF=YES"
config_file = '/scratch/a.pfb/gmw_monitoring/scripts/eodd_config_west_africa/EODataDownConfig_psql.json'
start_date = datetime.datetime.now()
end_date = datetime.datetime(year=2019, month=1, day=1)

roi_vec_file = 'site1.geojson'
roi_vec_lyr = 'site1'
out_dir = '/scratch/a.pfb/gmw_monitoring/video_examples/site1/imgs'
tmp_dir = '/scratch/a.pfb/gmw_monitoring/video_examples/site1/tmp'
stch_stats_file = '/scratch/a.pfb/gmw_monitoring/scripts/visual/sen1_strch_stats.txt'
scn_json_file = '/scratch/a.pfb/gmw_monitoring/video_examples/site1/sen1_imgs.json'
extract_sen1_img_data(config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir, stch_stats_file, scn_json_file)


