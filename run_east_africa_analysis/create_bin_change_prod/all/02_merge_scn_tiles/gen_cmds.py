from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import os.path
import logging
import glob
import tqdm

logger = logging.getLogger(__name__)

class GenCmdsGenBinChngMsks(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        import rsgislib
        rsgis_utils = rsgislib.RSGISPyUtils()
        tiles = glob.glob(os.path.join(kwargs['ref_tiles_path'], "*.kea"))
        for tile in tqdm.tqdm(tiles):
            tile_basename = rsgis_utils.get_file_basename(tile).replace("_chng_scr", "").replace("gmw_", "")
            tiles_to_merge = glob.glob(os.path.join(kwargs['scn_wgs84_img_path'], "*", "*{}*.kea".format(tile_basename)))
            if len(tiles_to_merge) > 0:
                c_dict = dict()
                c_dict["merge_imgs"] = tiles_to_merge
                c_dict["out_img"] = os.path.join(kwargs['out_tile_path'], "{}_chng_bin_msk.kea".format(tile_basename))
                c_dict["out_vec_file"] = os.path.join(kwargs['out_tile_path'], "{}_chng_bin_msk.gpkg".format(tile_basename))
                c_dict["out_vec_lyr"] = tile_basename
                print(c_dict["out_img"])
                print(c_dict["out_vec_file"])
                if not (os.path.exists(c_dict["out_vec_file"]) and os.path.exists(c_dict["out_vec_file"])):
                    self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(scn_wgs84_img_path='/scratch/a.pfb/gmw_monitoring/data_out/gmw_scn_bin_chng_wgs84_tiles',
                              ref_tiles_path='/scratch/a.pfb/gmw_monitoring/monitoring/scores_imgs',
                              out_tile_path='/scratch/a.pfb/gmw_monitoring/data_out/gmw_bin_chng_wgs84_tiles',
                              out_vec_tile_path='/scratch/a.pfb/gmw_monitoring/data_out/gmw_bin_chng_wgs84_tiles_vec')
        self.pop_params_db()
        self.create_slurm_sub_sh("merge_tile_chng_msks", 18504, '/scratch/a.pfb/gmw_monitoring/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10, n_xtr_cmds=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("merge_bin_tiles.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'merge_bin_tiles'
    process_tools_cls = 'MergeBinChngMsk'

    create_tools = GenCmdsGenBinChngMsks(cmd=script_cmd, db_conn_file="/home/a.pfb/eodd_gmw_info/pbpt_db_conn_east.txt",
                                        lock_file_path="./gmw_monitor_lock_file.txt",
                                        process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()

