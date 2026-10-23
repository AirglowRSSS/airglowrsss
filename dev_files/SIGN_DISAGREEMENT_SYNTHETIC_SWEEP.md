# Task 1b: Synthetic ground-truth sweep — diagnosis

Diagnosis only, per scope. No fix written. This resolves the open question left by
`SIGN_DISAGREEMENT_INVESTIGATION.md` with an exact, code-level answer — and **corrects**
that report's leading hypothesis, which pointed at Path A (the original ω-k detection).
Path A is fine. The bug is in Path B.

## Method

Built a real `xr.Dataset` (dims `time, north, east`, matching `OmegaKFilterXarray`'s
expected input) containing a single synthetic plane wave, `cos(Kx·x + Ky·y − Ωt)`, for a
range of known ground-truth azimuths `az_true` (Convention B, 0°=N/90°=E/clockwise).
λ=40 km, period=20 min, dx=1.33 km, 150×150 px, 46 frames at 2-min cadence — all inside
the pipeline's normal operating range, well clear of any passband edge.

For each `az_true`, ran the **real, deployed** functions for both paths:

- **Path A:** `OmegaKFilterXarray` → `create_filter(apply_dispersion=False)` →
  `analyze_and_plot_waves(...)` → `identify_physical_waves(...)` →
  `physical_waves[0]['azimuth']`. This is the actual call chain
  `wave_validation.py`'s `sliding_window_analysis` uses.
- **Path B:** `reconstruct_all_waves(waves=[{orientation_deg, wavelength_km, ...}], ...,
  rms_calibrate=False)` (fixed-kernel mode, real `create_gabor_kernel`) →
  `analyze_all_waves_kalman(...)` → majority sign of `kf_omega_rads` over valid frames →
  recovered azimuth = `orientation_deg` if sign is positive else `orientation_deg + 180`.
  This is the same logic `validate_wave_azimuth_sign` uses to compute `expected_sign`/
  `measured_sign`.

Azimuth coverage: every 45° around the full circle, plus fine spacing (5° steps) near
0°, 90°, and 180° per the task's instructions, given 90° has already produced one bug
this session.

## Results

| az_true | Path A | A err | Path B | B: frac ω>0 | B err | A vs B agree |
|---|---|---|---|---|---|---|
| 0 | 0.00° | 0.00° | 0.0° | 1.00 | 0.00° | ✅ |
| 5 | 5.71° | 0.71° | 5.0° | 1.00 | 0.00° | ✅ |
| 10 | 11.31° | 1.31° | 10.0° | 1.00 | 0.00° | ✅ |
| 15 | 16.70° | 1.70° | 15.0° | 1.00 | 0.00° | ✅ |
| 20 | 18.43° | 1.57° | 20.0° | 1.00 | 0.00° | ✅ |
| 25 | 23.96° | 1.04° | 25.0° | 1.00 | 0.00° | ✅ |
| 45 | 45.00° | 0.00° | 45.0° | 1.00 | 0.00° | ✅ |
| 85 | 84.29° | 0.71° | 85.0° | 1.00 | 0.00° | ✅ |
| 90 | 90.00° | 0.00° | 90.0° | 1.00 | 0.00° | ✅ |
| 95 | 95.71° | 0.71° | 95.0° | 1.00 | 0.00° | ✅ |
| 135 | 135.00° | 0.00° | 135.0° | 1.00 | 0.00° | ✅ |
| 165 | 163.30° | 1.70° | 165.0° | 1.00 | 0.00° | ✅ |
| 170 | 168.69° | 1.31° | 170.0° | 1.00 | 0.00° | ✅ |
| 175 | 174.29° | 0.71° | 175.0° | 1.00 | 0.00° | ✅ |
| **180** | 180.00° | 0.00° | **0.0°** | 1.00 | **180.00°** | ❌ |
| **185** | 185.71° | 0.71° | **5.0°** | 1.00 | **179.29°** | ❌ |
| **225** | 225.00° | 0.00° | **45.0°** | 1.00 | **180.00°** | ❌ |
| **270** | 270.00° | 0.00° | **90.0°** | 1.00 | **180.00°** | ❌ |
| **315** | 315.00° | 0.00° | **135.0°** | 1.00 | **180.00°** | ❌ |
| **355** | 354.29° | 0.71° | **175.0°** | 1.00 | **179.29°** | ❌ |

Full CSV: `sign_disagreement_sweep_results.csv` (attached data, reproduced in the table
above).

**Pattern:** Path A tracks ground truth across the *entire* 360° circle, with only
small (≤1.7°) discretization error from the discrete FFT wavenumber grid — no
degradation anywhere, including at 90° and 180°, both previously-implicated boundaries.
Path B is exact for every `az_true < 180°`, and wrong by *exactly* 180° for every
single `az_true ≥ 180°` tested, with **zero exceptions** across 6 test points spanning
the full second half of the circle. `frac ω>0` (the fraction of valid frames where
`kf_omega_rads > 0`) is **1.00 in every single row**, including the six where the
recovered azimuth is wrong. `kf_omega_rads` is never negative, for any tested azimuth,
including a wave manifestly moving in the `orientation_deg + 180°` direction where a
negative sign is exactly what a correct measurement should produce.

## Root cause

`phase_speed_kalman.py`, `analyze_all_waves_kalman`, lines 1001–1004:

```python
# ω_obs(t) = |angle(A_cross(t))| / dt  — always non-negative.
omega_obs = np.full(n_time, np.nan)
nonzero   = amp_obs > 0.0
omega_obs[nonzero] = np.abs(np.angle(A[nonzero])) / dt   # ≥ 0 always
```

and line 1009:

```python
omega_init = abs(2.0 * np.pi * ps / lam_m) if lam_m > 0 else 0.0
```

Both the per-frame *observation* fed to `_kalman_omega_tracker` and the filter's
*initial state* are explicitly `abs()`-ed before the Kalman filter ever runs. The
filter (`_kalman_omega_tracker` itself, `phase_speed_kalman.py:83-266`) is a plain
linear KF with no sign constraint of its own — it would happily track a negative
state if given a negative observation stream. It never gets one. `kf_omega_rads`
(`kf_all[wi]['omega']`, exported as the `kf_omega_rads` column) is therefore
**architecturally incapable of being negative**, by explicit, commented design ("≥ 0
always") — not a bug that sometimes produces the wrong sign, a quantity that was never
designed to carry sign information at all.

This is not a newly-introduced bug. `_kalman_omega_tracker`'s own docstring, and the
`analyze_all_waves_kalman` row-building code's comment (already present before this
week's sessions) both say this explicitly: *"Direction is fixed from ω-k to avoid
sign-flip artefacts... Tracking |ω| eliminates all such artefacts."* I had read that
note as a general caution about noise susceptibility. It is not — it's a literal,
precise description of lines 1004/1009. `kf_prop_azimuth_deg` has always been hard-set
to the fixed ω-k azimuth (never derived from `kf_omega_rads`'s sign) for exactly this
reason, by design, predating any change made this session.

## Correction to `SIGN_DISAGREEMENT_INVESTIGATION.md`

That report's leading hypothesis pointed at Path A (`identify_physical_waves`/
`arctan2(-kx,-ky)`) as the more likely source of the disagreement, based on it being a
"different code path" from the newly-fixed kernel/KF side. This sweep shows the
opposite: **Path A is correct across the full circle; Path B's `kf_omega_rads` cannot
represent the information `validate_wave_azimuth_sign` was built to extract from it.**
The five real-data checks in that report (period, wavelength, SNR, quadrant,
time-of-night) all correctly found no clean discriminator — because there isn't one to
find. Given `kf_omega_rads` is always ≥ 0, `validate_wave_azimuth_sign`'s
`measured_sign` is *always* `+1`, so `frac_matching` reduces entirely to whether a
family's own `azimuth` happens to equal `orientation_deg` (100% "match") or
`orientation_deg + 180` (0% "match") — a question already fully answered by Path A
at detection time, before the diagnostic ever runs. The bimodal 0%/100% split reported
in the previous investigation is not evidence of two different real-data populations;
it is the direct, deterministic consequence of comparing a fixed value against a
quantity that cannot disagree with it in the informative direction.

This also means: **the "9 of 14" / "9 of 10" flagged-family counts in both real
datasets carry no information about whether those families' azimuths are actually
wrong.** Family 1 (DOY 173, 345°) was flagged and — per direct visual confirmation
against real imagery in the prior session — was correct. Given the mechanism just
found, that is now the expected outcome, not a surprising one: roughly half of
detected waves will have `azimuth = orientation_deg + 180` purely as a matter of which
physical direction they happen to travel, and *all* of those will be flagged by
`validate_wave_azimuth_sign` regardless of whether Path A got them right.

## What this means for the diagnostic tool itself

`validate_wave_azimuth_sign` (`phase_speed_kalman.py`) and its wiring in
`airglow_pipeline.py`/the notebook are currently emitting a flag
(`kf_azimuth_validation.csv`, the `flagged` column) that does not carry the meaning its
own docstring and the pipeline's console/notebook messaging claim ("cross-check...
against the sign of its own, independently-measured kf_omega_rads"). The check is not
independent — `kf_omega_rads`'s sign is fully determined by `orientation_deg`
vs. `azimuth`'s relationship by construction, not by anything newly measured from the
data. This is a design/correctness issue in the diagnostic itself, not just a matter of
interpretation, and — per this task's scope — I'm reporting it rather than fixing it.
No implementation in this pass.
