"""
Precision/recall trade-off against FPA FOD ground truth.

Reruns match_prototype.py + validate.py for several distance (D) and time (T)
settings and collects precision, recall and F1 for the lightning-matched fires.
Afterwards it restores the default run (D = 5 km, T = 48 h, new-fire rule 5 km).

Usage:  python3.14 pr_sweep.py
Output: pr_sweep.csv, pr_tradeoff.png
"""
import itertools
import re
import subprocess
import sys

import pandas as pd

D_VALUES = [2, 5, 10]
T_VALUES = [24, 48, 96, 168]
NEW_KM = 5


def run(*args):
    return subprocess.run([sys.executable, *args], capture_output=True, text=True, check=True).stdout


def last_value(text, label):
    line = next(l for l in text.splitlines() if l.startswith(label))
    v = line.split()[-1]
    return float(v.rstrip("%")) / (100 if v.endswith("%") else 1)


rows = []
grid = list(itertools.product(D_VALUES, T_VALUES))
for i, (d, t) in enumerate(grid, 1):
    run("match_prototype.py", "--d-km", str(d), "--t-hours", str(t), "--new-km", str(NEW_KM))
    try:
        out = run("validate.py")
        row = {"D_km": d, "T_hours": t,
               "fires_flagged": int(last_value(out, "Fires flagged by pipeline")),
               "precision": last_value(out, "Precision"),
               "recall_all": last_value(out, "Recall, all recorded fires"),
               "recall_10plus_acres": last_value(out, "Recall, fires 10+ acres"),
               "f1": last_value(out, "F1 score")}
    except subprocess.CalledProcessError:  # no fire matched any flash at this setting
        row = {"D_km": d, "T_hours": t, "fires_flagged": 0, "precision": 0.0,
               "recall_all": 0.0, "recall_10plus_acres": 0.0, "f1": 0.0}
    rows.append(row)
    print(f"[{i}/{len(grid)}] D={d} km, T={t} h: {row['fires_flagged']} flagged, "
          f"precision {row['precision']:.0%}, recall {row['recall_all']:.0%}, F1 {row['f1']:.2f}")

# Baseline: all new fires, no lightning filter (same for every D/T)
base = run("validate.py")
base_line = next(l for l in base.splitlines() if l.startswith("Precision"))

# Restore the default run so other scripts see the usual files
run("match_prototype.py", "--d-km", "5", "--t-hours", "48", "--new-km", str(NEW_KM))
run("validate.py")

res = pd.DataFrame(rows)
res.to_csv("pr_sweep.csv", index=False)
show = res.copy()
for c in ["precision", "recall_all", "recall_10plus_acres"]:
    show[c] = (show[c] * 100).round().astype(int).astype(str) + "%"
print("\n===== PRECISION / RECALL SWEEP (lightning-matched fires vs FPA FOD) =====")
print(show.sort_values("f1", ascending=False).to_string(index=False))
nums = re.findall(r"(\d+)%", base_line)
print(f"\nWithout the lightning filter (all new fires): precision {nums[0]}%")
print("Saved: pr_sweep.csv, pr_tradeoff.png")

# ---- Chart ----
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    colors = {2: "#9ca3af", 5: "#dc2626", 10: "#2563eb"}
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for d, g in res[res.fires_flagged > 0].groupby("D_km"):
        g = g.sort_values("T_hours")
        ax.plot(g.recall_10plus_acres * 100, g.precision * 100, "-o", color=colors.get(d, "black"),
                label=f"D = {d} km")
        # one label per point; settings that land on the same point share it, e.g. "48/96/168 h"
        for (x, y), pts in g.groupby([g.recall_10plus_acres * 100, g.precision * 100]):
            ax.annotate("/".join(str(int(t)) for t in pts.T_hours) + " h", (x, y),
                        textcoords="offset points", xytext=(5, 4), fontsize=8)
    ax.axhline(float(nums[0]), color="black", ls="--", lw=1, label="No lightning filter")
    ax.set(xlabel="Recall: recorded lightning fires of 10+ acres found (%)",
           ylabel="Precision: flagged fires near a recorded fire (%)",
           title="Precision vs recall against FPA FOD (labels = time window T)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(alpha=0.3)
    ax.legend(frameon=False)
    plt.tight_layout()
    plt.savefig("pr_tradeoff.png", dpi=150)
except ImportError:
    print("(matplotlib not installed, chart skipped)")
