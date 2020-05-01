eoddsenroi.py -s Landsat -f ./refs/east_africa_roi.geojson -l east_africa_roi -o ./east_africa_roi_landsat.json
eoddsenroi.py -s Sentinel2 -f ./refs/east_africa_roi.geojson -l east_africa_roi -o ./east_africa_roi_sen2.json
eoddsenroi.py -s OtherBBOX -f ./refs/east_africa_roi_tiles.geojson -l east_africa_roi_tiles -o ./east_africa_roi_tiles.json

eoddsenroi.py -s Landsat -f ./refs/west_africa_roi.geojson -l west_africa_roi -o ./west_africa_roi_landsat.json
eoddsenroi.py -s Sentinel2 -f ./refs/west_africa_roi.geojson -l west_africa_roi -o ./west_africa_roi_sen2.json
eoddsenroi.py -s OtherBBOX -f ./refs/west_africa_roi_tiles.geojson -l west_africa_roi_tiles -o ./west_africa_roi_tiles.json
