from typing import Any

from .antimeridian_splitter import split_polygon
import math
import typing
import geojson
import h3
from isb_web.isb_solr_query import clip_float, solr_records_forh3_counts

# Mainly adapted from https://github.com/datadavev/seeh3/blob/main/app/seeh3.py

BB_REGEX = r"^([+-]?[0-9]+(\.[0-9]+)?,?){4}$"

# These are H3 cells from resolutions 0-16 that overlap the north or south poles
POLES = {
    "8af2939520c7fff",
    "8df2939520864bf",
    "8e03262a696431f",
    "8903262a697ffff",
    "8bf2939520c6fff",
    "80f3fffffffffff",
    "8cf2939520c69ff",
    "8503262bfffffff",
    "8d03262a4490cff",
    "8403263ffffffff",
    "8c03262a4490dff",
    "8803262a69fffff",
    "81f2bffffffffff",
    "81033ffffffffff",
    "8703262a6ffffff",
    "8a03262a6967fff",
    "83f293fffffffff",
    "84f2939ffffffff",
    "8af293952087fff",
    "85f29383fffffff",
    "89f2939520bffff",
    "8b03262a4490fff",
    "8df2939520c687f",
    "8003fffffffffff",
    "8e03262a4490cf7",
    "830326fffffffff",
    "8a03262a4497fff",
    "89f2939520fffff",
    "8bf293952086fff",
    "87f293952ffffff",
    "8b03262a6964fff",
    "88f2939521fffff",
    "8c03262a69643ff",
    "8903262a44bffff",
    "8603262a7ffffff",
    "8ef2939520864f7",
    "8cf2939520865ff",
    "86f293957ffffff",
    "820327fffffffff",
    "8d03262a696433f",
    "82f297fffffffff",
    "8ef2939520c684f",
}


class RecordCount(typing.TypedDict):
    n: int
    rn: float
    ln: float


class H3SolrQueryParams:
    q: str
    resolution: str

    def __init__(self, q: str, resolution: str):
        self.q = q
        self.resolution = resolution


def estimate_resolution(bb,) -> int:  # noqa: C901 -- simple enough in spite of high computational complexity
    if bb is None:
        return 1
    dx = abs(bb[2] - bb[0])
    if dx > 90:
        return 2
    if dx > 45:
        return 3
    if dx > 22:
        return 4
    if dx > 10:
        return 5
    if dx > 5:
        return 6
    if dx > 2:
        return 7
    if dx > 1:
        return 8
    if dx > 0.5:
        return 9
    if dx > 0.1:
        return 10
    return 10


def get_h3_solr_query_from_bb(bb: str, resolution: str, q: str) -> H3SolrQueryParams:
    if bb is not None and len(bb) > 4:
        bbox: list = bb.split(",")
        bbox = [float(v) for v in bbox]
        bbox[0] = clip_float(bbox[0], -180, 180)
        bbox[1] = clip_float(bbox[1], -90, 90)
        bbox[2] = clip_float(bbox[2], -180, 180)
        bbox[3] = clip_float(bbox[3], -90, 90)
        fq = f"producedBy_samplingSite_location_ll:[{bbox[1]},{bbox[0]} TO {bbox[3]},{bbox[2]}]"
        if q is None:
            q = fq
        else:
            q = f"{q} AND {fq}"
        if resolution is None:
            resolution = estimate_resolution(bbox)
    if q is None:
        q = "*:*"
    return H3SolrQueryParams(q, resolution)


def get_record_counts(
    query: str = "*:*", resolution: int = 1, exclude_poles: bool = True
) -> dict[Any, dict[str, Any]]:
    """
    Facet records matching query on resolution, returning dict with keys being h3.
    """
    field_name = f"producedBy_samplingSite_location_h3_{resolution}"
    response = solr_records_forh3_counts(query, field_name)
    counts = {}
    total = 0
    for entry in response.get("result-set", {}).get("docs", []):
        try:
            h = entry[field_name]
            if h not in POLES or not exclude_poles:
                n = entry["count(*)"]
                total += n
                counts[h] = {
                    "n": n,
                    "rn": 0,
                    "ln": 0,
                }
        except KeyError:
            pass
    if total == 0:
        log_total: float = 0
    else:
        log_total = math.log(total)
    for k in counts.keys():
        counts[k]["rn"] = counts[k]["n"] / total
        if counts[k]["n"] > 0:
            counts[k]["ln"] = math.log(counts[k]["n"]) / log_total
        else:
            counts[k]["ln"] = 0
    return counts


def h3_to_features(cell: str, cell_props: dict = {}) -> list[geojson.Feature]:
    """Given a h3 cell, return one or more geojson Features representing the cell.

    More than one feature may be returned if the polygon is split on the
    anti-meridian.

    Features are returned with properties:
      h3 = the cell
      km2 = the area of the cell in km^2
    """
    polygon = geojson.MultiPolygon(
        [
            h3.cell_to_boundary(cell, geo_json=True),
        ]
    )
    split_polygons = split_polygon(
        polygon, output_format="geojsondict"
    )
    res = []
    props = {
        "h3": cell,
        "km2": h3.cell_area(cell, unit="km^2"),
    }
    props.update(cell_props)
    for p in split_polygons:
        res.append(
            geojson.Feature(
                geometry=p,
                properties=props,
            )
        )
    return res


def h3s_to_feature_collection(cells: set[str], cell_props: dict = {}) -> geojson.FeatureCollection:
    """Returns the geojson feature collection representing the provided h3 cells.

    Cell polygons may be split on the anti-meridian.
    """
    features = []
    for cell in cells:
        props = cell_props.get(cell, {})
        features += h3_to_features(cell, cell_props=props)
    feature_collection = geojson.FeatureCollection(features)
    feature_collection['properties'] = {"h3_cells": cells}
    return feature_collection
