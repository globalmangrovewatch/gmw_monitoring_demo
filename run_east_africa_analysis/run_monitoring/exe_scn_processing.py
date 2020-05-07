from pbprocesstools.pbpt_process import PBPTProcessTool
import logging

logger = logging.getLogger(__name__)

class ProcessEODDScn(PBPTProcessTool):

    def __init__(self):
        super().__init__(cmd_name='gen_process_cmds.py', descript=None)

    def do_processing(self, **kwargs):
        import eodatadown.eodatadownrun
        logging.debug("Running analysis for scene {} from sensor {}.".format(self.params['scn_info'][2],
                                                                             self.params['scn_info'][1]))
        eodatadown.eodatadownrun.run_scn_analysis(self.params['scn_info'])
        logging.debug("Completed analysis for scene {} from sensor {}.".format(self.params['scn_info'][2],
                                                                               self.params['scn_info'][1]))

    def required_fields(self, **kwargs):
        return ["scn_info"]

    def outputs_present(self, **kwargs):
        import eodatadown.eodatadownrun
        logging.debug("Testing if analysis for scene {} from sensor {} is complete.".format(self.params['scn_info'][2],
                                                                                            self.params['scn_info'][1]))
        processing_required = eodatadown.eodatadownrun.does_scn_need_processing(self.params['scn_info'][0],
                                                                                self.params['scn_info'][1],
                                                                                self.params['scn_info'][2])
        return not processing_required

if __name__ == "__main__":
    ProcessEODDScn().std_run()


