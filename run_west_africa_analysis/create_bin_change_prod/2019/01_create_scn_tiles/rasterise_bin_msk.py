from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.vectorutils
import rsgislib.imageutils.imagelut
import subprocess

logger = logging.getLogger(__name__)

class RasteriseBinChngMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        basename = self.params['scn_unq_name']

        logger.debug("Have change features vector file: {}".format(self.params['chng_vec']))
        chng_feats_vec_lyrs = rsgislib.vectorutils.getVecLyrsLst(self.params['chng_vec'])
        if len(chng_feats_vec_lyrs) != 1:
            raise Exception("The number of layers is not equal to one. Don't know what has happened...")
        chng_feats_vec_lyr = chng_feats_vec_lyrs[0]
        logger.debug("Have change features vector layer: {}".format(chng_feats_vec_lyr))

        n_feats = rsgislib.vectorutils.getVecFeatCount(self.params['chng_vec'], chng_feats_vec_lyr, computeCount=True)

        if n_feats > 0:
            gmw_chng_wgs84_vec = os.path.join(self.params['tmp_dir'], "{}_chng_gmw_vec_wgs84.gpkg".format(basename))
            if os.path.exists(gmw_chng_wgs84_vec):
                rsgislib.vectorutils.delete_vector_file(gmw_chng_wgs84_vec)
            cmd = "ogr2ogr -f GPKG -nln {0} -t_srs EPSG:4326 {1} {2} {0}".format(chng_feats_vec_lyr, gmw_chng_wgs84_vec,
                                                                                 self.params['chng_vec'])
            logger.debug("Going to run command: '{}'".format(cmd))
            try:
                subprocess.check_call(cmd, shell=True)
            except OSError as e:
                raise Exception('Failed to run command: ' + cmd)

            lyr_bbox = rsgis_utils.getVecLayerExtent(gmw_chng_wgs84_vec, chng_feats_vec_lyr)
            print(lyr_bbox)

            scr_tiles = rsgislib.imageutils.imagelut.query_img_lut(lyr_bbox, self.params["img_lut"], self.params["scr_lut_lyr"])
            if len(scr_tiles) > 0:
                for scr_tile in scr_tiles:
                    print(scr_tile)
                    tile_basename = rsgis_utils.get_file_basename(scr_tile).replace("_chng_scr", "").replace("gmw_", "")
                    gmw_tile_chng_img = os.path.join(self.params["out_dir"], "{}_{}_chng_msk.kea".format(basename, tile_basename))
                    rsgislib.vectorutils.rasteriseVecLyr(gmw_chng_wgs84_vec, chng_feats_vec_lyr, scr_tile,
                                                         gmw_tile_chng_img, gdalformat="KEA", burnVal=1)


    def required_fields(self, **kwargs):
        return ["scn_unq_name", "chng_vec", "out_dir", "img_lut", "scr_lut_lyr", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        if os.path.exists(self.params['out_dir']):
            import glob
            imgs = glob.glob(os.path.join(self.params['out_dir'], "*.kea"))
            for img in imgs:
                files_dict[img] = 'gdal_image'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        import shutil
        if os.path.exists(self.params['out_dir']):
            shutil.rmtree(self.params['out_dir'])
            os.mkdir(self.params['out_dir'])

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
            os.mkdir(self.params['tmp_dir'])

if __name__ == "__main__":
    RasteriseBinChngMsk().std_run()


