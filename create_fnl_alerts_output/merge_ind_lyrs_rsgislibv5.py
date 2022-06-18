import rsgislib.vectorutils

vec_file = "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202112/gmw_chng_alerts_202112.gpkg"
out_vec_file = "/Users/pete/Temp/gmw_monitoring_bin_base/chng_alts_202112/gmw_chng_alerts_202112_merged.gpkg"

rsgislib.vectorutils.merge_vector_lyrs_to_gpkg(vec_file, out_vec_file, "gmw_chng_alerts")
