import itertools, re, subprocess, sys
import numpy as np
import pandas as pd

# Fairer baseline: keep every flash's TIME, but move the whole lightning field
# 50-100 km in a random direction. Timing stays realistic; only the
# fire-flash location link is broken.
glm = pd.read_csv("glm_aug2020.csv")
rng = np.random.default_rng(7)
SHIFTS = 3
for k in range(SHIFTS):
    ang, dist = rng.uniform(0, 2 * np.pi), rng.uniform(50, 100)
    s = glm.copy()
    s["lat"] = s["lat"] + dist * np.sin(ang) / 110.57
    s["lon"] = s["lon"] + dist * np.cos(ang) / (111.32 * np.cos(np.radians(37)))
    s.to_csv(f"glm_shift{k}.csv", index=False)


def run(glm_file, d, t, nk):
    out = subprocess.run([sys.executable, "match_prototype.py", "--glm", glm_file,
                          "--d-km", str(d), "--t-hours", str(t), "--new-km", str(nk)],
                         capture_output=True, text=True, check=True).stdout
    w = int(re.search(r"window:\s+([\d,]+)", out).group(1).replace(",", ""))
    m = int(re.search(r"candidate flash:\s+([\d,]+)", out).group(1).replace(",", ""))
    return w, m


rows = []
grid = list(itertools.product([5, 10], [48, 96, 168], [2, 5]))
for i, (d, t, nk) in enumerate(grid, 1):
    w, m = run("glm_aug2020.csv", d, t, nk)
    chance = np.mean([run(f"glm_shift{k}.csv", d, t, nk)[1] / max(w, 1) for k in range(SHIFTS)])
    real = m / max(w, 1)
    rows.append({"D_km": d, "T_hours": t, "new_fire_km": nk, "new_fires": w,
                 "real_rate": f"{real:.0%}", "chance_rate": f"{chance:.0%}",
                 "ratio": round(real / chance, 2) if chance else None})
    print(f"[{i}/{len(grid)}] D={d} km, T={t} h, new-fire={nk} km: "
          f"real {real:.0%} vs chance {chance:.0%}")

res = pd.DataFrame(rows).sort_values("ratio", ascending=False)
res.to_csv("tuning_results.csv", index=False)
print("\n===== TUNING SUMMARY =====")
print(res.to_string(index=False))
print("\nratio > 1 means real lightning lines up with fires more than chance does.")
