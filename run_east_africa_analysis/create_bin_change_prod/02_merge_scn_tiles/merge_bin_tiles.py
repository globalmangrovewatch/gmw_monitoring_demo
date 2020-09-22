from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import subprocess

logger = logging.getLogger(__name__)

def check_input_kea_imgs(input_imgs, n_bands=0, rm_files=False):
    """

    :param input_imgs:
    :param n_bands:
    :param rm_files:
    :return:
    """
    import h5py
    import rsgislib
    rsgis_utils = rsgislib.RSGISPyUtils()
    out_imgs = []
    for img_file in input_imgs:
        if os.path.exists(img_file):
            try:
                test_f = h5py.File(img_file, 'r')
                kea_img_items = list(test_f.keys())
                test_f.close()

                n_img_bands = rsgis_utils.getImageBandCount(img_file)
                if (n_bands == 0) and (n_img_bands > 0):
                    out_imgs.append(img_file)
                elif (n_bands > 0) and (n_img_bands == n_bands):
                    out_imgs.append(img_file)
            except:
                print("ERROR Image: {}".format(img_file))
                if rm_files:
                    os.remove(img_file)
    return out_imgs


class MergeBinChngMsk(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='exe_scn_processing.py', descript=None)

    def do_processing(self, **kwargs):

        merge_imgs = check_input_kea_imgs(self.params['merge_imgs'], n_bands=1, rm_files=False)
        if os.path.exists(self.params['out_img']):
            os.remove(self.params['out_img'])

        if os.path.exists(self.params['out_vec_file']):
            os.remove(self.params['out_vec_file'])

        rsgislib.imagecalc.calcMultiImgBandStats(merge_imgs, self.params['out_img'], rsgislib.SUMTYPE_SUM, 'KEA', rsgislib.TYPE_16UINT, 0, False)
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


