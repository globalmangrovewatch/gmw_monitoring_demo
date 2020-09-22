from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import subprocess

logger = logging.getLogger(__name__)

class MergeBinChngMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):
        rsgislib.imagecalc.calcMultiImgBandStats(self.params['merge_imgs'], self.params['out_img'], rsgislib.SUMTYPE_MAX, 'KEA', rsgislib.TYPE_8UINT, 0, False)
        rsgislib.vectorutils.polygoniseRaster2VecLyr(self.params['out_vec_file'], self.params['out_vec_lyr'], 'GPKG', self.params['out_img'], imgBandNo=1, maskImg=self.params['out_img'],
                                                     imgMaskBandNo=1, replace_file=True, replace_lyr=True, pxl_val_fieldname='PXLVAL')


    def required_fields(self, **kwargs):
        return ["merge_imgs", "out_img", "out_vec_file", "out_vec_lyr"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_img']] = 'gdal_image'
        files_dict[self.params['out_vec_file']] = 'gdal_vector'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        if os.path.exists(self.params['out_img']):
            os.remove(self.params['out_img'])
        if os.path.exists(self.params['out_vec_file']):
            os.remove(self.params['out_vec_file'])

if __name__ == "__main__":
    MergeBinChngMsk().std_run()


