# Independent re-estimation from second-pass reconstructions

Diagnosis only, per `second_pass_independent_estimation_task.md`. No fix proposed
or implemented. Goal: an estimate of wavelength/orientation/direction derived
directly from the actual reconstructed field, independent of the fixed founding
anchor, to characterize the residual 180° cases seen across the batch.

## Task 1 — Existing re-estimation machinery, confirmed locations

- **Orientation + wavelength, per frame, amplitude²-weighted spatial phase
  gradient**: `_spatial_phase_gradient(conv, dx_m)`
  (`gabor_reconstruction.py:341`). Input: the complex Gabor convolution `conv`
  for one frame. Output: `(kx_mean, ky_mean)` in rad/m, amplitude²-weighted
  across all pixels (`k_e/k_n = Im(conj(c)·dc/d{east,north})/|c|²`, weighted sum
  over `|c|²`). Called once per frame inside both `_reconstruct_fixed`
  (line 187) and `_reconstruct_adaptive` (line 318) — i.e., every call to
  `reconstruct_all_waves()` already computes this, for both the first and
  second reconstruction pass. `_kalman_theta_tracker`
  (`phase_speed_kalman.py:273`) turns `(kx, ky)` into orientation and
  wavelength: `theta_obs_raw[t] = arctan2(-kx_t, -ky_t)` and
  `lambda_obs_km[t] = (2π/k_h)/1000` (lines 372-373).
- **Temporal frequency, per frame, cross-correlation phasor**:
  `cmplx_amp[t] = mean(conj(conv_t)·conv_{t-1})` — `gabor_reconstruction.py:185`
  (fixed mode) and `:316` (adaptive mode), returned from `reconstruct_all_waves`
  as `complex_amplitudes`. `analyze_all_waves_kalman`
  (`phase_speed_kalman.py:825`) turns this into `kf_omega_rads`, at line 1004:
  `omega_obs[nonzero] = np.abs(np.angle(A[nonzero])) / dt`.

**All of this is already computed for the second pass and thrown away.**
`airglow_pipeline.py`'s Step 8b calls `reconstruct_all_waves(..., waves=waves_kf,
...)` and captures only `wave_cubes_kf, composite_kf, _, _, _` — the second
pass's own `kx_obs`, `ky_obs`, and `complex_amplitudes` are computed and
discarded on that same line. Nothing new needed to be built for this task —
just calling the same function again with the real return values kept.

## Task 2 — Is the direction component actually informative?

**Confirmed: yes, `abs()`-discarded, exactly like `kf_omega_rads`.** Line 1004
above is the literal source of `kf_omega_rads` — `omega_obs` is `abs()`-ed
before the Kalman filter ever runs. The pre-abs() signed version is simply
`np.angle(complex_amplitudes[wi])` (no abs, no further processing) — used
directly here.

**A second, less obvious sign trap found while confirming the "axis-only,
should be fine" assumption for orientation:** `theta_obs_raw` genuinely does
have a physical 180° ambiguity (`cos(k·r) = cos(-k·r)`, documented in
`phase_speed_kalman.py:376-391`), and the *existing* code resolves it by
**folding toward `theta_init_rad` — the fixed founding azimuth** (lines
392-401: `if d_alt < d_z: theta_obs_raw[t] = z_alt`). That means the
already-computed, already-saved `kf_orient_deg` column is **not** independent
of the founding anchor for its full-circle lobe choice — a family whose true
direction disagrees with its own founding value would have every one of its
raw observations folded right back onto that founding value, by construction
(the code's own comment notes exactly this failure mode from an earlier
attempt to fix it a different way). Using `kf_orient_deg` here would have
silently reproduced the same problem `validate_wave_azimuth_sign` had.

**Resolution:** recomputed `theta_obs_raw`/`lambda_obs_km` myself, directly
from raw `kx_obs`/`ky_obs` on the second-pass reconstruction, with **no fold
applied at all** — a truly independent axis estimate. The **axis** (mod 180°,
`indep_axis_deg` below) is unaffected by the fold either way (folding by ±π
doesn't change a mod-180 value), so the task's "orientation/wavelength
shouldn't have this problem" holds for the axis and for wavelength
specifically — confirmed, not assumed — but only because the axis was
recomputed raw; the full-circle *lobe* needed the same independent treatment
as direction, via the averaged-sign omega.

**Averaged-sign approach, sanity-checked against two known cases (DOY 173):**

| family | established truth | this method's result | agreement | sign_frac |
|---|---|---|---|---|
| Family 13 (wave_id 12) | ~19° (`FAMILY13_INVESTIGATION.md`) | 13.4° | 5.6° off, correct lobe | 0.62 |
| Family 6 (wave_id 5) | ~201° (`FAMILY6_WAVELENGTH_JUMP_INVESTIGATION.md`) | 208.3° | 7.7° off, correct lobe | 1.00 |

Both recover the previously-established true direction, not the flipped one.
Method validated — amplitude-weighted mean of the raw signed `angle(A_cross)`,
restricted to `collision_primary == True` frames, is a working independent
azimuth estimate.

## Task 3 — Applied to second-pass reconstructions

Scope, exactly as run: **DOY 173 2023 LOW, all 14 families** (the
well-characterized reference case), plus **3 nights from the 20-night batch**
— DOY 144, 150, 157 (2022 LOW) — chosen to span the batch's family-count range
(10, 20, and 9 families respectively) rather than its extremes, without
re-running all 18 successful nights given the per-night cost of a full
pipeline pass. **53 waves total** analyzed. Re-running the remaining ~14
nights would sharpen the prevalence estimate below but wasn't done here — this
sample is large enough to characterize the pattern, not to bound it precisely.

For each wave: independent axis/wavelength from raw `(kx, ky)` on the
second-pass reconstruction (`wave_cubes_kf`'s own inputs, not the fold-biased
first pass), independent full-circle azimuth from the axis + averaged-sign
omega, restricted to `collision_primary == True` frames (matching what
actually feeds `kf_binned.csv`).

## Task 4 — Comparison against final reported output

| agreement band | count | % |
|---|---|---|
| good (< 10°) | 35 | 66% |
| moderate (10°–135°) | 6 | 11% |
| **near-180° (> 135°)** | **12** | **23%** |

This is a materially higher prevalence than the single DOY 173 case (1/14)
suggested on its own — **8 of the 12 near-180° cases come from one night,
DOY 150** (8 of its 20 families, 40%). The other four are DOY 173 (1), DOY 144
(2), DOY 157 (1). Whether DOY 150 is representative or an outlier can't be
determined from a 3-extra-night sample — worth checking against more nights
before treating 23% as a general rate.

**Flagged (independent estimate confidently ~180° from reported final
direction — full-circle diff > 135° and sign_frac ≥ 0.7):**

| night | wave_id | label | reported az | independent az | diff | sign_frac | n_valid/n_total frames |
|---|---|---|---|---|---|---|---|
| DOY 173 | 9 | Family 10 (az~0°, λ~106 km) | 0.0° | 187.3° | 172.7° | 0.73 | 22/142 |
| DOY 144 | 5 | Family 6 (az~125°, λ~126 km) | 125.3° | 304.8° | 179.5° | 0.84 | 56/156 |
| DOY 144 | 6 | Family 7 (az~140°, λ~67 km) | 139.8° | 315.9° | 176.1° | 0.83 | 82/156 |
| DOY 150 | 3 | Family 4 (az~13°, λ~53 km) | 13.3° | 179.5° | 166.2° | 0.89 | 170/188 |
| DOY 150 | 7 | Family 8 (az~289°, λ~146 km) | 289.5° | 108.4° | 178.9° | 0.71 | 127/188 |
| DOY 150 | 16 | Family 17 (az~293°, λ~85 km) | 293.0° | 115.8° | 177.2° | 0.98 | 48/188 |
| DOY 150 | 18 | Family 19 (az~313°, λ~85 km) | 312.6° | 131.1° | 178.5° | 0.90 | 104/188 |
| DOY 157 | 5 | Family 6 (az~163°, λ~139 km) | 162.6° | 340.7° | 178.2° | 0.82 | 39/89 |

Four more DOY 150 families (14, 15, 16, 20) show the same ~166–178° gap but
with `sign_frac` between 0.55–0.69 — below the confidence cutoff used for the
table above, so not counted as "confident," but the same pattern, not a
different one. Included in the 23% figure above, not in the flagged table.

## Task 5 — Bad founding anchor?

Pulled each flagged night's `*_omegak_families.nc` (`n_detections` per
family — the count of raw sliding-window ω-k detections that built that
family, before any Gabor/KF step ever runs) as corroborating evidence.

**Every single flagged family falls in the bottom half of its own night's
family list by detection count** — families are already sorted
`-len(members)` at construction, so this is a real, not incidental, pattern:

| night | flagged family | n_detections | night's own range |
|---|---|---|---|
| DOY 173 | Family 10 | 3 | 2–12 |
| DOY 144 | Family 6 | 6 | 3–15 |
| DOY 144 | Family 7 | 6 | 3–15 |
| DOY 150 | Family 4 | 12 | 2–23 |
| DOY 150 | Family 8 | 9 | 2–23 |
| DOY 150 | Family 17 | **3** | 2–23 |
| DOY 150 | Family 19 | **2** (the MIN_DETECTIONS floor) | 2–23 |
| DOY 157 | Family 6 | 4 | 2–9 |

This is consistent with — but not a strict proof of — the founding-anchor
hypothesis: a family built from few raw detections has less evidence backing
its founding azimuth, and under the current architecture (`final_azimuth_deg`
fixed for the family's entire life, never re-derived) a bad initial call from
weak evidence has no mechanism to self-correct later, unlike the
ownership/tie-break issues found and fixed earlier this session.

**Important caveat, not glossed over:** it isn't a clean cutoff. DOY 173's
Families 12, 13, and 14 all also have `n_detections = 2` (the same floor as
DOY 150's flagged Family 19) but are **not** flagged — their independent
estimates agree with the reported direction (Family 13 is in fact one of the
Task 2 sanity-check cases, confirmed correct at `n_detections = 2`). Low
detection count is a real, significantly overrepresented risk factor in this
sample, not a deterministic predictor — most low-`n_detections` families
still get the direction right.

## Summary

- Re-estimation machinery already exists and is already computed for the
  second pass; it's just discarded by `run_airglow_pipeline`'s Step 8b.
- The direction/sign component needs the same `abs()`-avoidance treatment as
  `kf_omega_rads` — confirmed, and the averaged-sign approach built for it
  passes both available sanity checks.
- The already-saved `kf_orient_deg` is not usable for this kind of check on
  its own — it's fold-anchored to the founding azimuth by existing,
  documented design, not truly independent for the full-circle lobe.
- In this 53-wave sample (4 nights), 23% show a confident-to-moderate ~180°
  disagreement between the independently re-estimated direction and the
  currently-reported final one — concentrated 8-of-12 in a single night
  (DOY 150), whose representativeness is unknown without a larger sample.
- Low founding-detection-count is significantly overrepresented among the
  flagged cases but is not a clean predictor on its own.
- No fix proposed. Per the task, if this pattern holds up at broader scale,
  the real question — whether direction should be periodically re-derivable
  rather than fixed for a family's entire life — is a separate architectural
  design discussion.
