from eodatadown.eodatadownuseranalysis import EODataDownUserAnalysis
from eodatadown.eodatadownutils import EODataDownUtils

import rsgislib

import logging
import os

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
        usr_req_keys = ["goog_cred", "bucket_name", "bucket_vec_dir", "bucket_lut_dir"]
        EODataDownUserAnalysis.__init__(self, analysis_name='UploadToGoog', req_keys=usr_req_keys)

    def perform_analysis(self, scn_db_obj, sen_obj, plgin_objs):
        logger.info("Processing Scene: {}".format(scn_db_obj.PID))
        if scn_db_obj.Invalid:
            return False, None, False

        success = True
        out_dict = None

        if 'CreateSummaryVecFeats' in plgin_objs:
            if plgin_objs['CreateSummaryVecFeats'].Completed and plgin_objs['CreateSummaryVecFeats'].Output and plgin_objs['CreateSummaryVecFeats'].Success:
                scn_chng_info = plgin_objs['CreateSummaryVecFeats'].ExtendedInfo

                import pprint
                pprint.pprint(scn_chng_info)
                vec_files = scn_chng_info["out_chng_vec"]
                lut_files = scn_chng_info["out_vec_luts"]

                for vec_file in vec_files:
                    upload_to_google_bucket(vec_file, self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_vec_dir"])

                for lut_file in lut_files:
                    upload_to_google_bucket(lut_file, self.params["goog_cred"], self.params["bucket_name"], self.params["bucket_lut_dir"])

        else:
            logger.debug("No change features available as outputs from previous steps...")

        return success, out_dict, False

