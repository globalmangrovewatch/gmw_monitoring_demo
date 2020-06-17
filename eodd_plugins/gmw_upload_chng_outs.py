from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib

import logging
import os
import datetime
import pprint

logger = logging.getLogger(__name__)

def blob_exists(goog_cred, bucket_name, filename):
    from google.cloud import storage
    client = storage.Client.from_service_account_json(goog_cred)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    print(blob.exists())
    return blob.exists()


def list_files(goog_cred, bucket_name, bucket_folder):
    """List all files in GCP bucket."""
    from google.cloud import storage
    client = storage.Client.from_service_account_json(goog_cred)
    bucket = client.get_bucket(bucket_name)
    files = bucket.list_blobs(prefix=bucket_folder)
    fileList = [file.name for file in files if '.' in file.name]
    return fileList


def upload_to_google_bucket(file_to_upload, goog_cred, bucket_name, bucket_dir):
    """
    A function to upload a file to a google cloud bucket.

    :param file_to_upload: The local file to be uploaded.
    :param goog_cred: The JSON credentials file from Google.
    :param bucket_name: The google bucket name.
    :param bucket_dir: The directory path within the bucket for the file to be saved.

    """
    lcl_path, file_name = os.path.split(file_to_upload)
    bucket_file_path = os.path.join(bucket_dir, file_name)
    print(list_files(goog_cred, bucket_name, bucket_dir))
    if not blob_exists(goog_cred, bucket_name, bucket_file_path):
        from google.cloud import storage
        logger.debug("Uploading: {}".format(file_to_upload))
        logger.debug("Using Google Credentials file: {}".format(goog_cred))
    
        # Explicitly use service account credentials by specifying the private key  file.
        storage_client = storage.Client.from_service_account_json(goog_cred)

        bucket_obj = storage_client.get_bucket(bucket_name)
        blob_obj = bucket_obj.blob(bucket_file_path)
        blob_obj.upload_from_filename(file_to_upload)
        if not blob_exists(goog_cred, bucket_name, bucket_file_path):
            raise Exception("File was not successfully uploaded to Google Cloud: '{}'".format(file_to_upload))


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
        logger.debug("Scene has a date of '{}'".format(scn_obs_date.isoformat()))
        try:
            base_date = datetime.datetime(year=2020, month=1, day=1)
            base_date_delta = scn_obs_date - base_date
            zero_date_delta = datetime.timedelta()
            if base_date_delta > zero_date_delta:
                logger.debug("Scene is after 1/1/2020 and will therefore be uploaded")
                if 'CreateSummaryVecFeats' in plgin_objs:
                    logger.debug("Scene has had CreateSummaryVecFeats plugin executed.")
                    if plgin_objs['CreateSummaryVecFeats'].Completed and plgin_objs['CreateSummaryVecFeats'].Outputs and plgin_objs['CreateSummaryVecFeats'].Success:
                        logger.debug("CreateSummaryVecFeats plugin has been successfully executed.")
                        scn_chng_info = plgin_objs['CreateSummaryVecFeats'].ExtendedInfo
                        pprint.pprint(scn_chng_info)
                        for tile in scn_chng_info:
                            logger.debug("Processing Tile: {}.".format(tile))
                            upload_to_google_bucket(scn_chng_info[tile], self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_vec_dir"])
                            logger.debug("Uploaded: {}.".format(scn_chng_info[tile]))
                            lut_file_name = "gmw_{}_lut.json".format(tile)
                            lut_file_path = os.path.join(self.params["lcl_lut_dir"], lut_file_name)
                            logger.debug("LUT file to upload: {}".format(lut_file_path))
                            upload_to_google_bucket(lut_file_path, self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_lut_dir"])
                            logger.debug("Uploaded: {}.".format(lut_file_path))
                    else:
                        logger.debug("CreateSummaryVecFeats plugin has NOT been successfully executed")
                else:
                    logger.debug("No change features available as outputs from previous steps...")
            else:
                logger("Scene is from before 1/1/2020 and will therefore not be uploaded.")
        except Exception as e:
            print(e)
            import traceback
            print(traceback.format_exc())
            logger.exception(e)
            raise e

        return success, out_dict, False

