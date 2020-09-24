from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc

logger = logging.getLogger(__name__)

class CreateBaseChangeMasks(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        basename = rsgis_utils.get_file_basename(self.params['out_img'])

        gmw_tile_qa_edits_img = os.path.join(self.params["tmp_dir"], "{}_chng_edits.kea".format(basename))
        rsgislib.vectorutils.rasteriseVecLyr(self.params["qa_edits_vec"], self.params["qa_edits_lyr"],
                                             self.params["count_tile_img"], gmw_tile_qa_edits_img,
                                             gdalformat="KEA", burnVal=1)


        bandDefns = []
        bandDefns.append(rsgislib.imagecalc.BandDefn('qa', gmw_tile_qa_edits_img, 1))
        bandDefns.append(rsgislib.imagecalc.BandDefn('chng', self.params["count_tile_img"], 1))
        rsgislib.imagecalc.bandMath(self.params["out_img"], '(chng>2)&&(qa==0)?1:0', 'KEA', rsgislib.TYPE_8UINT, bandDefns)

        rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params["out_vec"], 'qa_chng', 'GPKG', self.params["out_img"],
                                                     imgBandNo=1, maskImg=self.params["out_img"], imgMaskBandNo=1,
                                                     replace_file=True, replace_lyr=True, pxl_val_fieldname='PXLVAL')

    def required_fields(self, **kwargs):
        return ["count_tile_img", "out_img", "out_vec", "qa_edits_vec", "qa_edits_lyr", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_img']] = 'gdal_image'
        files_dict[self.params['out_vec']] = 'gdal_vector'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        if os.path.exists(self.params['out_img']):
            os.remove(self.params['out_img'])

        if os.path.exists(self.params['out_vec']):
            os.remove(self.params['out_vec'])

        if os.path.exists(self.params['tmp_dir']):
            import shutil
            shutil.rmtree(self.params['tmp_dir'])
            os.mkdir(self.params['tmp_dir'])

if __name__ == "__main__":
    CreateBaseChangeMasks().std_run()


