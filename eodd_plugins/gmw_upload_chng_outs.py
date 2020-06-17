from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib

import logging
import os
import datetime

def upload_to_google_bucket(file_to_upload, goog_cred, bucket_name, bucket_dir):
    """
    A function to upload a file to a google cloud bucket.

    :param file_to_upload: The local file to be uploaded.
    :param goog_cred: The JSON credentials file from Google.
    :param bucket_name: The google bucket name.
    :param bucket_dir: The directory path within the bucket for the file to be saved.

    """
    from google.cloud import storage
    print("Upload: {}".format(file_to_upload))
    # Explicitly use service account credentials by specifying the private key  file.
    storage_client = storage.Client.from_service_account_json(goog_cred)

    lcl_path, file_name = os.path.split(file_to_upload)
    bucket_file_path = os.path.join(bucket_dir, file_name)
    bucket_obj = storage_client.get_bucket(bucket_name)
    blob_obj = bucket_obj.blob(bucket_file_path)
    blob_obj.upload_from_filename(file_to_upload)

logger = logging.getLogger(__name__)

class UploadGMWChange(EODataDownUserAnalysis):

    def __init__(self):
        usr_req_keys = ["goog_cred", "bucket_name", "bucket_vec_dir", "bucket_lut_dir", "lcl_lut_dir"]
        EODataDownUserAnalysis.__init__(self, analysis_name='UploadToGoog', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj, plgin_objs):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None, False

        success = True
        out_dict = None

        if sen_obj.get_sensor_name() == 'LandsatGOOG':
            scn_obs_date = scn_db_obj.Sensing_Time
        elif sen_obj.get_sensor_name() == 'Sentinel2GOOG':
            scn_obs_date = scn_db_obj.Sensing_Time
        elif sen_obj.get_sensor_name() == 'Sentinel1ASF':
            scn_obs_date = scn_db_obj.Acquisition_Date
        else:
            raise Exception("Did not recognise the sensor name...")

        base_date = datetime.datetime(year=2020, month=1, day=1)
        base_date_delta = scn_obs_date - base_date
        zero_date_delta = datetime.timedelta()
        if base_date_delta > zero_date_delta:
            if 'CreateSummaryVecFeats' in plgin_objs:
                if plgin_objs['CreateSummaryVecFeats'].Completed and plgin_objs['CreateSummaryVecFeats'].Outputs and plgin_objs['CreateSummaryVecFeats'].Success:
                    scn_chng_info = plgin_objs['CreateSummaryVecFeats'].ExtendedInfo

                    for tile in scn_chng_info:
                        upload_to_google_bucket(scn_chng_info[tile]['file'], self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_vec_dir"])
                        lut_file_name = "gmw_{}_lut.json".format(tile)
                        lut_file_path = os.path.join(self.params["lcl_lut_dir"], lut_file_name)
                        upload_to_google_bucket(lut_file_path, self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_lut_dir"])
            else:
                logger.debug("No change features available as outputs from previous steps...")

        return success, out_dict, False

