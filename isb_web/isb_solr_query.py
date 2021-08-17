import typing
import requests
import geojson
import fastapi

import isb_web.config

BASE_URL = isb_web.config.Settings().solr_url
_RPT_FIELD = "producedBy_samplingSite_location_rpt"

# Identify the bounding boxes for solr and leaflet for diagnostic purposes
SOLR_BOUNDS = -1
LEAFLET_BOUNDS = -2

MIN_LAT = "min_lat"
MAX_LAT = "max_lat"
MIN_LON = "min_lon"
MAX_LON = "max_lon"

# 0.2 seems ok for the grid cells.
_GEOJSON_ERR_PCT = 0.2
# 0.1 for the leaflet heatmap tends to generate more cells for the heatmap “blob” generation
_LEAFLET_ERR_PCT = 0.1


def clip_float(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def _get_heatmap(
    q: typing.AnyStr, bb: typing.Dict, dist_err_pct: float, grid_level=None
) -> typing.Dict:
    # TODO: dealing with the antimeridian ("dateline") in the Solr request.
    # Should probably do this by computing two requests when the request BB
    # straddles the antimeridian.
    if bb is None or len(bb) < 2:
        bb = {MIN_LAT: -90.0, MAX_LAT: 90.0, MIN_LON: -180.0, MAX_LON: 180.0}
    bb[MIN_LAT] = clip_float(bb[MIN_LAT], -90.0, 90.0)
    bb[MAX_LAT] = clip_float(bb[MAX_LAT], -90.0, 90.0)
    bb[MIN_LON] = clip_float(bb[MIN_LON], -180.0, 180.0)
    bb[MAX_LON] = clip_float(bb[MAX_LON], -180.0, 180.0)
    # logging.warning(bb)
    headers = {"Accept": "application/json"}
    params = {
        "q": q,
        "rows": 0,
        "wt": "json",
        "facet": "true",
        "facet.heatmap": _RPT_FIELD,
        "facet.heatmap.distErrPct": dist_err_pct,
        # "facet.heatmap.gridLevel": grid_level,
        "facet.heatmap.geom": f"[{bb[MIN_LON]} {bb[MIN_LAT]} TO {bb[MAX_LON]} {bb[MAX_LAT]}]",
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
    total_matching = res.get("response", {}).get("numFound", 0)
    hm = res.get("facet_counts", {}).get("facet_heatmaps", {}).get(_RPT_FIELD, {})
    hm["numDocs"] = total_matching
    return hm


##
# Create a GeoJSON rendering of the Solr Heatmap response.
# Generates a GeoJSON polygon (rectangle) feature for each Solr heatmap cell
# that has a count value over 0.
# Returns the generated features as a GeoJSON FeatureCollection,
# https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
def solr_geojson_heatmap(
    q, bb, grid_level=None, show_bounds=False, show_solr_bounds=False
):
    hm = _get_heatmap(q, bb, _GEOJSON_ERR_PCT, grid_level)
    # print(hm)
    gl = hm.get("gridLevel", -1)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"]  # - dd_lat
    lon_0 = hm["minX"]  # + dd_lon
    _max_value = 0

    # Container for the generated geojson features
    grid = []
    if show_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                    ]
                ]
            ),
            properties={"count": LEAFLET_BOUNDS},
        )
        grid.append(bbox)
    if show_solr_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                    ]
                ]
            ),
            properties={"count": SOLR_BOUNDS},
        )
        grid.append(bbox)

    # Process the Solr heatmap response. Draw a box for each cell
    # that has a count > 0 and set the "count" property of the
    # feature to that value.
    _total = 0
    _count_matrix = hm.get("counts_ints2D", None)
    if _count_matrix is not None:
        for i_row in range(0, hm["rows"]):
            for i_col in range(0, hm["columns"]):
                if _count_matrix[i_row] is not None:
                    v = _count_matrix[i_row][i_col]
                    if v > 0:
                        _total = _total + v
                        if v > _max_value:
                            _max_value = v
                        p0lat = lat_0 - dd_lat * i_row
                        p0lon = lon_0 + dd_lon * i_col
                        pts = geojson.Polygon(
                            [
                                [
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                ]
                            ]
                        )
                        feature = geojson.Feature(geometry=pts, properties={"count": v})
                        grid.append(feature)
    geodata = geojson.FeatureCollection(grid)
    geodata["max_count"] = _max_value
    geodata["grid_level"] = gl
    geodata["total"] = _total
    geodata["num_docs"] = hm.get("numDocs", 0)
    return geodata


# Generate a list of [latitude, longitude, value] from
# a solr heatmap. Latitude and longitude represent the
# centers of the solr heatmap grid cells. The value is the count
# for the grid cell.
# Suitable for consumption by leaflet: https://leafletjs.com
def solr_leaflet_heatmap(q, bb, grid_level=None):
    hm = _get_heatmap(q, bb, _LEAFLET_ERR_PCT, grid_level)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"] - dd_lat / 2.0
    lon_0 = hm["minX"] + dd_lon / 2.0
    data = []
    max_value = 0
    _total = 0
    for i_row in range(0, hm["rows"]):
        for i_col in range(0, hm["columns"]):
            if hm["counts_ints2D"][i_row] is not None:
                v = hm["counts_ints2D"][i_row][i_col]
                if v > 0:
                    _total = _total + v
                    lt = lat_0 - dd_lat * i_row
                    lg = lon_0 + dd_lon * i_col
                    data.append([lt, lg, v])
                    if v > max_value:
                        max_value = v
    # return list of [lat, lon, count] and maximum count value
    return {
        "data": data,
        "max_value": max_value,
        "total": _total,
        "num_docs": hm.get("numDocs", 0),
    }


def solr_query(params):
    """
    Issue a request against the solr select endpoint.

    Params is a list of [k, v] to support duplicate keys, which solr
    uses a lot of.

    Args:
        params: list of list, see https://solr.apache.org/guide/8_9/common-query-parameters.html

    Returns:
        Iterator for the solr response.
    """
    url = f"{BASE_URL}/select"
    headers = {"Accept": "application/json"}
    wt_map = {
        "csv": "text/plain",
        "xml": "application/xml",
        "geojson": "application/geo+json",
        "smile": "application/x-jackson-smile",
        "json": "application/json",
    }
    for k,v in params:
        if k == "wt":
            content_type = wt_map.get(v.lower(),"json")
    response = requests.get(url, headers=headers, params=params, stream=True)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type=content_type
    )


def solr_luke():
    """
    Information about the solr isb_core_records schema
    See: https://solr.apache.org/guide/8_9/luke-request-handler.html

    Returns:
        JSON document iterator
    """
    url = f"{BASE_URL}/admin/luke"
    params = {"show": "schema", "wt": "json"}
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, params=params, stream=True)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type="application/json"
    )
