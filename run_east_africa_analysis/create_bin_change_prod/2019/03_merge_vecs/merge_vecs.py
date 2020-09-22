import rsgislib.vectorutils
import glob

input_vecs = glob.glob("/scratch/a.pfb/gmw_monitoring/data_out/gmw_bin_chng_wgs84_tiles_vec_2019/*.gpkg")

rsgislib.vectorutils.merge_vector_files(input_vecs, '/scratch/a.pfb/gmw_monitoring/data_out/gmw_opt_chng_feats_msk_2019.gpkg', output_lyr='chng_feats', out_format='GPKG', out_epsg=None)