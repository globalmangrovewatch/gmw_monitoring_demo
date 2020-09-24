import rsgislib.vectorutils
import glob

input_vecs = glob.glob("/scratch/a.pfb/gmw_monitoring/monitoring/hist_chng_imgs_19_20_qad_vec/*.gpkg")

rsgislib.vectorutils.mergeVectors2GPKG(input_vecs, '/scratch/a.pfb/gmw_monitoring/data_out/gmw_qad_chng_feats_msk.gpkg', 'chng_feats', False)

