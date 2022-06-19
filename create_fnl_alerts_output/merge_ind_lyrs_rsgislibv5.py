import rsgislib.vectorutils

vec_file = "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202201/gmw_chng_alerts_202201.gpkg"
out_vec_file = "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202112/gmw_chng_alerts_202201_merged.gpkg"

rsgislib.vectorutils.merge_vector_lyrs_to_gpkg(vec_file, out_vec_file, "gmw_chng_alerts")
