from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import os.path
import logging
import glob

logger = logging.getLogger(__name__)

class GenCmdsGenBinChngMsks(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        import rsgislib
        rsgis_utils = rsgislib.RSGISPyUtils()

        tiles = glob.glob(os.path.join(kwargs['base_chng_cnt_tiles'], "*.kea"))
        for tile in tiles:
            print(tile)
            tile_basename = rsgis_utils.get_file_basename(tile)
            tile_base = tile_basename.replace("_chng_bin_msk", "")
            out_base_name = 'gmw_{}_hist_chng'.format(tile_base)
            out_img = os.path.join(kwargs['out_chng_msk_img_dir'], "{}.kea".format(out_base_name))
            out_vec = os.path.join(kwargs['out_chng_msk_vec_dir'], "{}.gpkg".format(out_base_name))
            print("\t{}".format(out_img))
            print("\t{}".format(out_vec))
            if not (os.path.exists(out_img) and os.path.exists(out_vec)):
                c_dict = dict()
                c_dict['count_tile_img'] = tile
                c_dict['out_img'] = out_img
                c_dict['out_vec'] = out_vec
                c_dict['qa_edits_vec'] = kwargs['qa_edits_vec']
                c_dict['qa_edits_lyr'] = kwargs['qa_edits_lyr']
                c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], out_base_name)
                if not os.path.exists(c_dict['tmp_dir']):
                    os.mkdir(c_dict['tmp_dir'])
                self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(base_chng_cnt_tiles='/scratch/a.pfb/gmw_monitoring/data_out/gmw_bin_chng_wgs84_tiles',
                              qa_edits_vec='/scratch/a.pfb/gmw_monitoring/scripts/run_east_africa_analysis/create_bin_change_prod/change_edits/identified_change_regions.gpkg',
                              qa_edits_lyr='identified_change_regions',
                              out_chng_msk_img_dir='/scratch/a.pfb/gmw_monitoring/monitoring/hist_chng_imgs_19_20_qad',
                              out_chng_msk_vec_dir='/scratch/a.pfb/gmw_monitoring/monitoring/hist_chng_imgs_19_20_qad_vec',
                              tmp_dir='/scratch/a.pfb/gmw_monitoring/tmp_dir')
        self.pop_params_db()
        self.create_slurm_sub_sh("gen_base_chng_msk", 18504, '/scratch/a.pfb/gmw_monitoring/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10, n_xtr_cmds=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("gen_base_chng_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'gen_base_chng_msk'
    process_tools_cls = 'CreateBaseChangeMasks'

    create_tools = GenCmdsGenBinChngMsks(cmd=script_cmd, db_conn_file="/home/a.pfb/eodd_gmw_info/pbpt_db_conn_east.txt",
                                        lock_file_path="./gmw_monitor_lock_file.txt",
                                        process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()

