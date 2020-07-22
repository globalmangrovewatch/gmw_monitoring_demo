import eodatadown.eodatadownsystemmain
import eodatadown.eodatadownutils
import rsgislib
import rsgislib.imageutils
import rsgislib.vectorutils
import rsgislib
import datetime
import glob
import os

def subset_mask_img(input_img, roi_vec_file, roi_vec_lyr, scn_tmp_dir, out_img):
    basename = os.path.splitext(os.path.basename(input_img))[0]
    rsgis_utils = rsgislib.RSGISPyUtils()
    bbox = rsgis_utils.getVecLayerExtent(roi_vec_file, roi_vec_lyr)
    rsgis_dtype = rsgis_utils.getRSGISLibDataTypeFromImg(input_img)
    sub_img = os.path.join(scn_tmp_dir, "{}_sub.kea".format(basename))
    rsgislib.imageutils.subsetbbox(input_img, sub_img, 'KEA', rsgis_dtype, bbox[0], bbox[1], bbox[2], bbox[3])
    msk_img = os.path.join(scn_tmp_dir, "{}_msk.kea".format(basename))
    rsgislib.vectorutils.rasteriseVecLyr(roi_vec_file, roi_vec_lyr, sub_img, msk_img, gdalformat="KEA")
    rsgislib.imageutils.maskImage(sub_img, msk_img, out_img, "GTIFF", rsgis_dtype, 32767, 0)
    rsgislib.imageutils.popImageStats(out_img, usenodataval=True, nodataval=32767, calcpyramids=True)


def extract_sen1_img_data(eodd_config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir, stch_stats_file):
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
    for scn in scns:
        print(scn.ARDProduct_Path)
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

            print("")
        except:
            print("ERROR Caught but ignored")
                    
os.environ["RSGISLIB_IMG_CRT_OPTS_GTIFF"] = "TILED=YES:COMPRESS=LZW:BIGTIFF=YES"
config_file = '/bigdata/eodd_wales_ard/scripts/eodd/config/EODataDownBaseConfig_psql.json'
start_date = datetime.datetime.now()
end_date = datetime.datetime(year=2019, month=1, day=1)

roi_vec_file = '/data/extract_sen1_data/wye_roi_osgb.geojson'
roi_vec_lyr = 'wye_roi_osgb'
out_dir = '/data/extract_sen1_data/out_data'
tmp_dir = '/data/extract_sen1_data/tmp'
extract_sen1_img_data(config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir)



roi_vec_file = 'River_Shapefiles/Neath/Neath.shp'
roi_vec_lyr = 'Neath'
out_dir = '/data/extract_sen1_data/out_data_neath'
tmp_dir = '/data/extract_sen1_data/tmp_neath'
extract_sen1_img_data(config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir)


roi_vec_file = 'River_Shapefiles/Severn/Severn.shp'
roi_vec_lyr = 'Severn'
out_dir = '/data/extract_sen1_data/out_data_severn'
tmp_dir = '/data/extract_sen1_data/tmp_severn'
extract_sen1_img_data(config_file, roi_vec_file, roi_vec_lyr, start_date, end_date, out_dir, tmp_dir)

