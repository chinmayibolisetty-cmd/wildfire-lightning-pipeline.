"""
Chance baseline: how many fires would "match" lightning by coincidence?
Shuffles the flash times (keeping flash locations), reruns the matching,
and compares the result to the real match rate.

Usage: python3.14 baseline.py [--runs 5]
"""
import argparse
import re
import subprocess
import sys

import numpy as np
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--runs", type=int, default=5)
args, extra = parser.parse_known_args()


def run_match(glm_file):
    out = subprocess.run([sys.executable, "match_prototype.py", "--glm", glm_file, *extra],
                         capture_output=True, text=True, check=True).stdout
    window = int(re.search(r"window:\s+([\d,]+)", out).group(1).replace(",", ""))
    matched = int(re.search(r"candidate flash:\s+([\d,]+)", out).group(1).replace(",", ""))
    return window, matched


print("Running real matching...")
window, real = run_match("glm_aug2020.csv")
real_rate = real / max(window, 1)

glm = pd.read_csv("glm_aug2020.csv")
rng = np.random.default_rng(42)
rates = []
for k in range(args.runs):
    shuffled = glm.copy()
    shuffled["flash_time"] = rng.permutation(shuffled["flash_time"].values)
    shuffled.to_csv("glm_shuffled.csv", index=False)
    w, m = run_match("glm_shuffled.csv")
    rates.append(m / max(w, 1))
    print(f"  shuffle {k + 1}: {m} of {w} matched ({rates[-1]:.0%})")

print("\n===== BASELINE SUMMARY =====")
print(f"Real match rate:        {real} of {window} new fires ({real_rate:.0%})")
print(f"Chance match rate:      {np.mean(rates):.0%} average over {args.runs} shuffles "
      f"(range {min(rates):.0%}-{max(rates):.0%})")
if np.mean(rates) > 0:
    print(f"Real / chance ratio:    {real_rate / np.mean(rates):.1f}x")
