from pbprocesstools.pbpt_q_process import PBPTGenQProcessToolCmds
import os.path
import logging
import datetime

logger = logging.getLogger(__name__)

class GenCmdsGenBinChngMsks(PBPTGenQProcessToolCmds):

    def gen_command_info(self, **kwargs):
        import eodatadown.eodatadownsystemmain

        sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
        sys_main_obj.parse_config(kwargs['config_file'])

        for sensor in kwargs['sensors']:
            logger.debug("Try to get sensor object for: '{}'.".format(sensor))
            sensor_obj = sys_main_obj.get_sensor_obj(sensor)
            logger.debug("Got sensor object for: '{}'.".format(sensor))

            end_date = datetime.datetime(year=2018, month=1, day=1)
            start_date = datetime.datetime.now()

            scns = sensor_obj.query_scn_records_date(start_date, end_date, start_rec=0, n_recs=0, valid=True, cloud_thres=None)

            scn_chng_vecs_path = kwargs['scn_ls_chng_vecs_path']
            if sensor == "Sentinel2GOOG":
                scn_chng_vecs_path = kwargs['scn_s2_chng_vecs_path']

            for scn in scns:
                scn_unq_name = sensor_obj.get_scn_unq_name_record(scn)
                scn_chng_path = os.path.join(scn_chng_vecs_path, scn_unq_name)
                if os.path.exists(scn_chng_path):
                    scn_chng_vec = self.find_file(scn_chng_path, '*.gpkg')
                    if scn_chng_vec is not None:
                        c_dict = dict()
                        c_dict['scn_unq_name'] = scn_unq_name
                        c_dict['chng_vec'] = scn_chng_vec
                        c_dict['out_dir'] = os.path.join(kwargs['scn_out_wgs84_img_path'], scn_unq_name)
                        if not os.path.exists(c_dict['out_dir']):
                            os.mkdir(c_dict['out_dir'])
                        c_dict['img_lut'] = kwargs['img_lut']
                        c_dict['scr_lut_lyr'] = kwargs['scr_lut_lyr']
                        c_dict['tmp_dir'] = os.path.join(kwargs['tmp_dir'], scn_unq_name)
                        if not os.path.exists(c_dict['tmp_dir']):
                            os.mkdir(c_dict['tmp_dir'])
                        self.params.append(c_dict)


    def run_gen_commands(self):
        self.gen_command_info(config_file='/scratch/a.pfb/gmw_monitoring/scripts/eodd_config_east_africa/EODataDownConfig_psql.json',
                              sensors=['LandsatGOOG', 'Sentinel2GOOG'],
                              scn_ls_chng_vecs_path='/scratch/a.pfb/gmw_monitoring/data/east_africa/landsat/scn_chng_vecs',
                              scn_s2_chng_vecs_path='/scratch/a.pfb/gmw_monitoring/data/east_africa/sen2/scn_chng_vecs',
                              img_lut='/scratch/a.pfb/gmw_monitoring/monitoring/gmw_monitor_lut.gpkg',
                              scr_lut_lyr='score_imgs',
                              scn_out_wgs84_img_path='/scratch/a.pfb/gmw_monitoring/data_out/gmw_scn_bin_chng_wgs84_tiles',
                              tmp_dir='/scratch/a.pfb/gmw_monitoring/tmp_dir')
        self.pop_params_db()
        self.create_slurm_sub_sh("gen_bin_msk_ea", 18504, '/scratch/a.pfb/gmw_monitoring/logs',
                                 run_script='run_exe_analysis.sh', job_dir="job_scripts",
                                 db_info_file=None, account_name='scw1376', n_cores_per_job=10, n_jobs=10, n_xtr_cmds=10,
                                 job_time_limit='2-23:59',
                                 module_load='module load parallel singularity\n\nexport http_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\nexport https_proxy="http://a.pfb:proxy101019@10.212.63.246:3128"\n')

if __name__ == "__main__":
    py_script = os.path.abspath("rasterise_bin_msk.py")
    script_cmd = "singularity exec --bind /scratch/a.pfb:/scratch/a.pfb --bind /home/a.pfb:/home/a.pfb /scratch/a.pfb/sw_imgs/au-eoed-dev.sif python {}".format(py_script)

    process_tools_mod = 'rasterise_bin_msk'
    process_tools_cls = 'RasteriseBinChngMsk'

    create_tools = GenCmdsGenBinChngMsks(cmd=script_cmd, db_conn_file="/home/a.pfb/eodd_gmw_info/pbpt_db_conn_east.txt",
                                        lock_file_path="./gmw_monitor_lock_file.txt",
                                        process_tools_mod=process_tools_mod, process_tools_cls=process_tools_cls)
    create_tools.parse_cmds()

