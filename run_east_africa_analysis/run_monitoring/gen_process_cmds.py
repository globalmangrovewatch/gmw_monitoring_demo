from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import os.path
import logging
import datetime

logger = logging.getLogger(__name__)

class CreateEODataDownCmds(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        import eodatadown.eodatadownrun
        eodatadown.eodatadownrun.find_new_downloads(kwargs['config_file'], kwargs['sensors'])

        start_date = datetime.datetime(year=2021, month=2, day=1)
        end_date = datetime.datetime(year=2021, month=2, day=28)

        scns = eodatadown.eodatadownrun.get_scenes_need_processing_date_order(kwargs['config_file'], kwargs['sensors'],
                                                                              start_datetime=start_date,
                                                                              end_datetime=end_date)
        for scn in scns:
            c_dict = dict()
            c_dict['scn_info'] = scn
            self.params.append(c_dict)

    def run_gen_commands(self):
        self.gen_command_info(config_file='/scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json',
                              sensors=['LandsatGOOG', 'Sentinel2GOOG'])
        self.pop_params_db()
        self.create_slurm_sub_sh("eodd_ea_monitor", 18504, '/scratch/a.pfb/gmw_monitoring/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("exe_scn_processing.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'exe_scn_processing'
    process_tools_cls = 'ProcessEODDScn'

    create_tools = CreateEODataDownCmds(cmd=script_cmd, db_conn_file="/home/a.pfb/eodd_gmw_info/pbpt_db_conn_east.txt",
                                        lock_file_path="./gmw_monitor_lock_file.txt",
                                        process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()

