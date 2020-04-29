import json
import glob
import rsgislib

rsgis_utils = rsgislib.RSGISPyUtils()

imgs = glob.glob("/mangroves_server/mangroves/global_mangrove_watch_original/FinalProducts/RasterTiles/GMW_2010_Tiles/*.kea")

bboxes = list()
for img in imgs:
    print(img)
    img_bbox = rsgis_utils.getImageBBOX(img)
    bboxes.append(img_bbox)

with open('img_bboxes.json', 'w') as fp:
    json.dump({"bboxes":bboxes}, fp, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)


