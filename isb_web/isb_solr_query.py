import requests
import geojson

DEFAULT_COLLECTION = "isb_core_records"
BASE_URL = f"http://localhost:8984/solr/{DEFAULT_COLLECTION}"
RPT_FIELD = "producedBy_samplingSite_location_rpt"

# Identify the bounding boxes for solr and leaflet for diagnostic purposes
SOLR_BOUNDS = -1
LEAFLET_BOUNDS = -2

MIN_LAT = "min_lat"
MAX_LAT = "max_lat"
MIN_LON = "min_lon"
MAX_LON = "max_lon"


##
# Create a GeoJSON rendering of the Solr Heatmap response.
# Generates a GeoJSON polygon (rectangle) feature for each Solr heatmap cell
# that has a count value over 0.
# Returns the generated features as a GeoJSON FeatureCollection
#
def solr_grid_heatmap(q, bb, field=RPT_FIELD, grid_level=None, show_bounds=False, show_solr_bounds=False):
    # TODO: dealing with the antimeridian ("dateline") in the Solr request.
    # Should probably do this by computing two requests when the request BB
    # straddles the antimeridian.
    if len(bb) < 2:
        bb = {
            MIN_LAT: -180.0,
            MAX_LAT: 180.0,
            MIN_LON: -90.0,
            MAX_LON: 90.0
        }
    """
    Note, this is the format that the map callback appeared to be generating
    bb = {
        MIN_LAT: bounds[0][1],
        MAX_LAT: bounds[1][1],
        MIN_LON: bounds[0][0],
        MAX_LON: bounds[1][0]
    }
    """
    if bb[MIN_LAT] < -180.0:
        bb[MIN_LAT] = -180.0
    if bb[MAX_LAT] > 180.0:
        bb[MAX_LAT] = 180.0
    # logging.warning(bb)
    headers = {"Accept": "application/json"}
    params = {
        "q": q,
        "rows": 0,
        "wt": "json",
        "facet": "true",
        "facet.heatmap": field,
        "facet.heatmap.distErrPct": 0.2,
        # "facet.heatmap.gridLevel": grid_level,
        "facet.heatmap.geom": f"[{bb[MIN_LAT]} {bb[MIN_LON]} TO {bb[MAX_LAT]} {bb[MAX_LON]}]"
    }
    # if grid level is None, then Solr calculates an "appropriate" grid scale
    # based on the bounding box and distErrPct. Seems a bit off...
    if grid_level is not None:
        params["facet.heatmap.gridLevel"] = grid_level
    # Get the solr heatmap for the provided bounds
    url = f"{BASE_URL}/select"
    response = requests.get(url, headers=headers, params=params)

    # logging.debug("Got: %s", response.url)
    res = response.json()
    hm = res.get("facet_counts", {}).get("facet_heatmaps", {}).get(field, {})
    # print(hm)
    gl = hm.get('gridLevel', -1)
    # logging.warning(hm)
    d_lat = hm['maxY'] - hm['minY']
    dd_lat = d_lat / (hm['rows'])
    d_lon = hm['maxX'] - hm['minX']
    dd_lon = d_lon / (hm['columns'])
    lat_0 = hm['maxY']  # - dd_lat
    lon_0 = hm['minX']  # + dd_lon
    _max_value = 0

    # Container for the generated geojson features
    grid = []
    if show_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon([[
                (bb[MIN_LAT], bb[MIN_LON],),
                (bb[MAX_LAT], bb[MIN_LON],),
                (bb[MAX_LAT], bb[MAX_LON],),
                (bb[MIN_LAT], bb[MAX_LON],),
                (bb[MIN_LAT], bb[MIN_LON],),
            ]]),
            properties={'count': LEAFLET_BOUNDS}
        )
        grid.append(bbox)
    if show_solr_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon([[
                (hm['minX'], hm['minY'],),
                (hm['maxX'], hm['minY'],),
                (hm['maxX'], hm['maxY'],),
                (hm['minX'], hm['maxY'],),
                (hm['minX'], hm['minY'],),
            ]]),
            properties={'count': SOLR_BOUNDS}
        )
        grid.append(bbox)

    # Process the Solr heatmap response. Draw a box for each cell
    # that has a count > 0 and set the "count" property of the
    # feature to that value.
    for i_row in range(0, hm['rows']):
        for i_col in range(0, hm['columns']):
            if hm['counts_ints2D'][i_row] is not None:
                v = hm['counts_ints2D'][i_row][i_col]
                if v > 0:
                    if v > _max_value:
                        _max_value = v
                    p0lat = lat_0 - dd_lat * i_row
                    p0lon = lon_0 + dd_lon * i_col
                    pts = geojson.Polygon([
                        [
                            (p0lon, p0lat,),
                            (p0lon + dd_lon, p0lat,),
                            (p0lon + dd_lon, p0lat - dd_lat,),
                            (p0lon, p0lat - dd_lat,),
                            (p0lon, p0lat,),
                        ]
                    ])
                    feature = geojson.Feature(geometry=pts, properties={'count': v})
                    grid.append(feature)
    # returns GeoJSON, maximum count value, and grid level used by solr
    return geojson.FeatureCollection(grid), _max_value, gl
