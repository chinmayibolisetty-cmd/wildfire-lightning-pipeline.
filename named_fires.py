"""
Spot-check the largest recorded lightning fires (FPA FOD, Aug 15-22, 2020):
did the pipeline see each one as a new fire, how soon, and was it matched to lightning?

Same matching rule as validate.py: a pipeline fire counts if it is within 5 km
and 2 days of the recorded fire.

Usage:  python3.14 named_fires.py
Needs:  fpa_fod_ca_lightning.csv, new_fires.csv, fire_flash_matches.csv
        (from match_prototype.py --d-km 5 --t-hours 48 --new-km 5)
Output: named_fires.csv
"""
import numpy as np
import pandas as pd

R_KM, DAYS, TOP_N = 5.0, 2, 15
COS0 = np.cos(np.radians(37.0))

fod = pd.read_csv("fpa_fod_ca_lightning.csv")
fod["date"] = pd.to_datetime(fod["FIRE_YEAR"].astype(str)) + pd.to_timedelta(fod["DISCOVERY_DOY"] - 1, unit="D")
fod = fod[fod.date.between("2020-08-15", "2020-08-22")]
top = fod.sort_values("FIRE_SIZE", ascending=False).head(TOP_N)

new = pd.read_csv("new_fires.csv")
new["time"] = pd.to_datetime(new["time"], errors="coerce", format="mixed")
lit = pd.read_csv("fire_flash_matches.csv")
lit["fire_time"] = pd.to_datetime(lit["fire_time"], errors="coerce", format="mixed")
if new["time"].isna().any():
    print(f"Note: {new['time'].isna().sum()} rows in new_fires.csv have no readable time and are skipped.")
new = new.dropna(subset=["time"])


def nearby(df, lat_col, lon_col, time_col, fire):
    km = np.hypot((df[lon_col] - fire.LONGITUDE) * 111.32 * COS0, (df[lat_col] - fire.LATITUDE) * 110.57)
    days = (df[time_col].dt.normalize() - fire.date).dt.days
    mask = (km <= R_KM) & (days.abs() <= DAYS)
    return df[mask].assign(km=km[mask], days=days[mask])  # filter km/days too, so empty stays empty


rows = []
for f in top.itertuples():
    n = nearby(new, "latitude", "longitude", "time", f)
    m = nearby(lit, "fire_lat", "fire_lon", "fire_time", f)
    name = str(f.FIRE_NAME).title() if isinstance(f.FIRE_NAME, str) else "(unnamed)"
    cplx = str(f.COMPLEX_NAME).title() if "COMPLEX_NAME" in top and isinstance(f.COMPLEX_NAME, str) else ""
    rows.append({
        "fire": name,
        "complex": cplx,
        "discovered": f.date.strftime("%b %d"),
        "acres": int(round(f.FIRE_SIZE)),
        "seen_as_new_fire": "yes" if len(n) else "no",
        "first_detection": (n["time"].dropna().min().strftime("%b %d %H:%M UTC")
                            if n["time"].notna().any() else ""),
        "lightning_matched": "yes" if len(m) else "no",
        "nearest_flash_km": round(lit.loc[m.index, "nearest_flash_km"].min(), 1) if len(m) else "",
        "flash_to_detection_h": round(lit.loc[m.index, "lag_hours"].min(), 1) if len(m) else "",
    })

res = pd.DataFrame(rows)
res.to_csv("named_fires.csv", index=False)
pd.set_option("display.width", 200)
print(f"\n===== LARGEST RECORDED LIGHTNING FIRES, AUG 15-22, 2020 (top {TOP_N} by size) =====")
print(res.to_string(index=False))
print(f"\nSeen as a new fire:  {(res.seen_as_new_fire == 'yes').sum()} of {len(res)}")
print(f"Lightning-matched:   {(res.lightning_matched == 'yes').sum()} of {len(res)}")
print(f"Acres covered by lightning-matched fires: "
      f"{res.loc[res.lightning_matched == 'yes', 'acres'].sum():,} of {res.acres.sum():,}")
print("Saved: named_fires.csv")
