from pbprocesstools.pbpt_process import PBPTGenProcessToolCmds
import glob
import os.path
import logging

logger = logging.getLogger(__name__)


class CreateSRTMTileCmds(PBPTGenProcessToolCmds):

    def gen_command_info(self, **kwargs):
        srtm_files = glob.glob(os.path.join(kwargs['srtm_dir'], "*.zip"))
        for srtm_file in srtm_files:
            print(srtm_file)
            file_name, dir = os.path.split(srtm_file)
            basename = file_name.replace(".zip", ).replace(".", "_").lower()
            c_dict = dict()
            c_dict['srtm_zip'] = srtm_file
            tmp_dir = os.path.join(kwargs['tmp_dir'], basename)
            if not os.path.exists(tmp_dir):
                os.mkdir(tmp_dir)
            c_dict['tmp_dir'] = tmp_dir
            c_dict['out_min1_img'] = os.path.join(kwargs['out_min1_dir'], "{}_min1_img.kea".format(basename))
            c_dict['out_min0_img'] = os.path.join(kwargs['out_min0_dir'], "{}_min0_img.kea".format(basename))
            c_dict['basename'] = basename

            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(srtm_dir='/scratch/a.pfb/SRTM/RAW',
                              tmp_dir='/scratch/a.pfb/SRTM/tmp',
                              out_min1_dir='/scratch/a.pfb/SRTM/min_val_1_tiles',
                              out_min0_dir = '/scratch/a.pfb/SRTM/min_val_0_tiles'
                              )
        self.write_cmd_files()
        self.create_slurm_sub_sh(jobname='srtm_processing',
                                 mem_per_core_mb=8192,
                                 log_dirs='/scratch/a.pfb/SRTM/logs',
                                 prepend='singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif ')

    def run_check_outputs(self):
        process_tools_mod = 'process_srtm_tile'
        process_tools_cls = 'ProcessSRTMTile'
        time_sample_str = self.generate_readable_timestamp_str()
        out_err_file = 'processing_errs_{}.txt'.format(time_sample_str)
        out_non_comp_file = 'non_complete_errs_{}.txt'.format(time_sample_str)
        self.check_job_outputs(process_tools_mod, process_tools_cls, out_err_file, out_non_comp_file)


if __name__ == "__main__":
    py_script = os.path.abspath("process_srtm_tile.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    create_tools = CreateSRTMTileCmds(cmd=script_cmd, cmds_sh_file="./cmds_lst.sh",
                                      out_cmds_base="./ind_cmds/cmd_file")
    create_tools.parse_cmds()
