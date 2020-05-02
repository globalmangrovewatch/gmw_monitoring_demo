from pbprocesstools.pbpt_process import PBPTProcessTool
import os
import logging
import subprocess
import rsgislib
import rsgislib.imageutils
import rsgislib.imagecalc

logger = logging.getLogger(__name__)


def unzip_file(zip_file, out_dir):
    zip_file = os.path.abspath(zip_file)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    os.chdir(out_dir)
    cmd = "unzip '{}'".format(zip_file)
    subprocess.call(cmd, shell=True)


class ProcessSRTMTile(PBPTProcessTool):

    def __init__(self):
        super().__init__(cmd_name='process_srtm_tile.py', descript=None)

    def do_processing(self, **kwargs):
        rsgis_utils = rsgislib.RSGISPyUtils()
        srtm_zip_file = self.params['srtm_zip']
        tmp_dir = self.params['tmp_dir']
        basename = self.params['basename']
        out_min1_img = self.params['out_min1_img']
        out_min0_img = self.params['out_min0_img']

        unzip_file(srtm_zip_file, tmp_dir)

        srtm_file = rsgis_utils.findFile(tmp_dir, "*.hgt")

        srtm_kea_file = os.path.join(tmp_dir, "{}.kea".format(basename))
        rsgislib.imageutils.gdal_translate(srtm_file, srtm_kea_file, 'KEA')

        rsgislib.imagecalc.imageMath(srtm_kea_file, out_min0_img, 'b1<0:0:b1', 'KEA', rsgislib.TYPE_32INT, False, False)
        rsgislib.imageutils.popImageStats(out_min0_img, usenodataval=True, nodataval=0, calcpyramids=True)

        rsgislib.imagecalc.imageMath(srtm_kea_file, out_min1_img, 'b1<1:1:b1', 'KEA', rsgislib.TYPE_32INT, False, False)
        rsgislib.imageutils.popImageStats(out_min1_img, usenodataval=True, nodataval=1, calcpyramids=True)

    def required_fields(self, **kwargs):
        return ["srtm_zip",
                "tmp_dir",
                "out_min1_img",
                "out_min0_img",
                "basename"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_min1_img']] = 'gdal_image'
        files_dict[self.params['out_min0_img']] = 'gdal_image'
        return self.check_files(files_dict)


if __name__ == "__main__":
    ProcessSRTMTile().std_run()


