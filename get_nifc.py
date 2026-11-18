"""
Download 2020 California wildfire perimeters (second ground truth).

--source calfire (default): CAL FIRE FRAP "California Fire Perimeters (all)", the
    statewide compilation of perimeters from CAL FIRE, USFS, BLM, NPS and others.
    It records each fire's ALARM_DATE and CAUSE (1 = lightning), so broad_eval.py
    can use the recorded cause and start date directly.
--source nifc: NIFC "InterAgencyFirePerimeterHistory All Years View". Name and year
    only. Our first run returned just 12 small 2020 California perimeters (none of
    the siege complexes), so it is kept only for comparison.

Usage:
    python get_nifc.py                  # CAL FIRE
    python get_nifc.py --source nifc
If a service URL has moved: download the layer as GeoJSON by hand, then
    python get_nifc.py --from-file perimeters.geojson
Output: fire_perimeters_ca2020.geojson
        (properties: name, year, agency, gis_acres, cause, alarm_date, source)
"""
import argparse
import json
import time
import urllib.parse
import urllib.request

URLS = {
    "calfire": ("https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/"
                "California_Historic_Fire_Perimeters/FeatureServer/0"),
    "nifc": ("https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
             "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer/0"),
}
CA_BBOX = (-124.5, 32.5, -114.0, 42.0)   # same box as get_glm.py
NAME_FIELDS = ["INCIDENT", "poly_IncidentName", "INCDNT_NM", "FIRE_NAME"]
YEAR_FIELDS = ["FIRE_YEAR_INT", "FIRE_YEAR", "FIRE_YEAR_", "YEAR_"]
AGENCY_FIELDS = ["AGENCY", "attr_POOProtectingAgency", "AGENCY_"]
ACRES_FIELDS = ["GIS_ACRES", "poly_GISAcres", "Shape__Area_acres"]
CAUSE_FIELDS = ["CAUSE", "cause"]
DATE_FIELDS = ["ALARM_DATE", "alarm_date", "attr_FireDiscoveryDateTime"]

p = argparse.ArgumentParser()
p.add_argument("--source", choices=sorted(URLS), default="calfire")
p.add_argument("--url", default=None, help="Override the feature layer URL (ends in /FeatureServer/<n>)")
p.add_argument("--year", type=int, default=2020)
p.add_argument("--from-file", default=None, help="Normalise a GeoJSON you downloaded by hand")
p.add_argument("--out", default="fire_perimeters_ca2020.geojson")
a = p.parse_args()
a.url = a.url or URLS[a.source]


def to_date(v):
    """ArcGIS dates come as epoch milliseconds (or an ISO string in a hand download)."""
    if v in (None, ""):
        return None
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(float(v) / 1000))
    except (TypeError, ValueError):
        return str(v)[:10]


def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def get_json(url, params=None, tries=4):
    full = url + ("?" + urllib.parse.urlencode(params) if params else "")
    for i in range(tries):
        try:
            with urllib.request.urlopen(full, timeout=120) as r:
                return json.load(r)
        except Exception as e:  # network hiccup: retry with back-off
            if i == tries - 1:
                raise
            print(f"  retrying after error: {e}")
            time.sleep(2 ** i)


def pick(props, candidates):
    for c in candidates:
        if c in props and props[c] not in (None, ""):
            return props[c]
    return None


def in_bbox(geom):
    """True if any vertex of the geometry is inside the California box."""
    rings = geom["coordinates"] if geom["type"] == "Polygon" else [r for poly in geom["coordinates"] for r in poly]
    x0, y0, x1, y1 = CA_BBOX
    return any(x0 <= x <= x1 and y0 <= y <= y1 for ring in rings for x, y in ring[:: max(1, len(ring) // 50)])


def normalise(features):
    out = []
    for f in features:
        g, pr = f.get("geometry"), f.get("properties") or {}
        if not g or g["type"] not in ("Polygon", "MultiPolygon") or not in_bbox(g):
            continue
        year = pick(pr, YEAR_FIELDS)
        try:
            year = int(str(year)[:4])
        except (TypeError, ValueError):
            year = None
        if year != a.year:
            continue
        out.append({"type": "Feature", "geometry": g, "properties": {
            "name": str(pick(pr, NAME_FIELDS) or "").strip().upper(),
            "year": year,
            "agency": pick(pr, AGENCY_FIELDS),
            "gis_acres": pick(pr, ACRES_FIELDS),
            "cause": to_int(pick(pr, CAUSE_FIELDS)),
            "alarm_date": to_date(pick(pr, DATE_FIELDS)),
            "source": "file" if a.from_file else a.source,
        }})
    return out


t0 = time.time()
if a.from_file:
    features = normalise(json.load(open(a.from_file))["features"])
else:
    meta = get_json(a.url, {"f": "json"})
    fields = {f["name"]: f["type"] for f in meta.get("fields", [])}
    year_field = next((f for f in YEAR_FIELDS if f in fields), None)
    if year_field is None:
        raise SystemExit(f"No year field found in {a.url}. Fields: {sorted(fields)}")
    is_str = fields[year_field] == "esriFieldTypeString"
    where = f"{year_field} = '{a.year}'" if is_str else f"{year_field} = {a.year}"
    max_n = int(meta.get("maxRecordCount", 1000))
    print(f"Querying {meta.get('name', a.url)}: {where}, California bounding box, {max_n} per page")
    base = {"where": where, "outFields": "*", "f": "geojson", "outSR": 4326,
            "geometry": ",".join(map(str, CA_BBOX)), "geometryType": "esriGeometryEnvelope",
            "inSR": 4326, "spatialRel": "esriSpatialRelIntersects",
            "geometryPrecision": 5, "maxAllowableOffset": 0.0003,  # ~30 m simplification
            "resultRecordCount": max_n}
    raw, offset = [], 0
    while True:
        page = get_json(a.url + "/query", {**base, "resultOffset": offset})
        if "error" in page:
            raise SystemExit(f"Service error: {page['error']}")
        feats = page.get("features", [])
        raw += feats
        print(f"  {len(raw):,} perimeters downloaded")
        if not feats or not (page.get("exceededTransferLimit") or page.get("properties", {}).get("exceededTransferLimit")):
            break
        offset += len(feats)
    features = normalise(raw)

json.dump({"type": "FeatureCollection", "features": features}, open(a.out, "w"))
names = sorted({f["properties"]["name"] for f in features})
print("\n===== PERIMETER SUMMARY =====")
print(f"{a.year} perimeters in California: {len(features):,} ({len(names):,} distinct names)")
causes = [f["properties"]["cause"] for f in features]
dates = sorted(f["properties"]["alarm_date"] for f in features if f["properties"]["alarm_date"])
print(f"  with a cause: {sum(c is not None for c in causes):,} (lightning, cause 1: {causes.count(1):,}); "
      f"with an alarm date: {len(dates):,}" + (f" ({dates[0]} to {dates[-1]})" if dates else ""))
for key in ["AUGUST", "LNU", "SCU", "CZU", "NORTH", "SQF"]:
    hits = [n for n in names if key in n]
    print(f"  names containing {key:<7} {len(hits):>3}  {', '.join(hits[:4])}{' ...' if len(hits) > 4 else ''}")
print(f"Time: {time.time() - t0:.0f} s.  Saved: {a.out}")
