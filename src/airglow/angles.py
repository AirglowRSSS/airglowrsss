"""
angles.py
---------
The one shared angular-comparison/circular-statistics utility for the wave
pipeline. Every place two orientations or azimuths get compared or averaged
should import from here rather than reimplementing the arithmetic inline —
this consolidates eight independent implementations found scattered across
`wave_validation.py`, `gabor_reconstruction.py`, `phase_speed_kalman.py`,
`wave_clustering.py`, `cluster_reconstruction.py`, and `OmegaK_enhanced.py`
(see `REFACTOR_DESIGN.md` Task 1's survey), three of which used different
algebraic forms for the same "distance on a mod-180 circle" concept.

Two angular conventions are in play throughout this codebase, and every
function below is explicit about which one it operates on:

- **axis** (orientation): [0, 180°). 0° and 180° are the same physical axis
  (a wave's spatial orientation, direction-agnostic).
- **full-circle** (azimuth / propagation direction): [0, 360°). 0° and 180°
  are opposite, physically distinct directions.

Mixing these up — comparing a full-circle quantity with an axis-only
tolerance, or vice versa — is the single most common bug pattern this
session found (`build_wave_families`'s and `build_waves_for_gabor`'s
Voronoi grouping both originally checked axis only, silently merging
counter-propagating waves that shared an axis). Callers are responsible for
knowing which kind of quantity they have; this module does not guess.
"""

import numpy as np


def axis_diff_deg(a, b):
    """
    Unsigned distance between two axis-only (mod-180°) quantities, in [0, 90].

    0° and 180° are treated as identical (same physical axis). Vectorized —
    `a`/`b` may be scalars or numpy arrays (broadcast together as usual).

    Replaces: `wave_validation.py`'s `_angular_diff`, `gabor_reconstruction.py`'s
    `_adiff`, `phase_speed_kalman.py`'s `_circ_diff_180`, and the
    `(x + 90) % 180 - 90` form used inline in `_kalman_theta_tracker`'s
    kernel-mismatch check and in that file's own regression test
    (`_half_diff`) — all four are mathematically identical to this function;
    see this module's `__main__` regression block for the equivalence check.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    d = np.abs(a - b) % 180.0
    return np.minimum(d, 180.0 - d)


def full_circle_diff_deg(a, b):
    """
    Unsigned distance between two full-circle (mod-360°) quantities, in [0, 180].

    0° and 180° are physically opposite here — this is the check that was
    missing from `build_wave_families` and `build_waves_for_gabor`'s Voronoi
    grouping before this session's fixes let two counter-propagating,
    same-axis waves merge. Vectorized, same broadcasting as `axis_diff_deg`.

    Replaces: `wave_validation.py`'s `_full_circle_diff`,
    `gabor_reconstruction.py`'s `_full_circle_diff` (a byte-for-byte copy of
    the former), and `abs(circular_diff_deg(...))` at every call site that
    used it that way.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    d = np.abs(a - b) % 360.0
    return np.minimum(d, 360.0 - d)


def signed_circular_diff_deg(a, b):
    """
    Signed full-circle difference `a - b`, wrapped to [-180, 180).

    Positive means `a` is ahead of `b` going clockwise (this codebase's
    azimuth convention: 0=North, 90=East). `abs(signed_circular_diff_deg(a, b))
    == full_circle_diff_deg(a, b)` always — this is the more fundamental
    primitive; `full_circle_diff_deg` is the unsigned distance derived from it.

    Replaces: `phase_speed_kalman.py`'s module-level `circular_diff_deg`
    (identical formula, renamed for consistency with this module's naming).
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    return (a - b + 180.0) % 360.0 - 180.0


def circular_mean_axis_deg(angles_deg, weights=None):
    """
    Weighted circular mean of axis-only (mod-180°) quantities, in [0, 180).

    Uses the doubled-angle trick (2θ maps the 180°-periodic axis onto a
    360°-periodic circle where a plain circular mean is well-defined, then
    halves the result back). `weights=None` gives an unweighted mean.
    NaN entries in `angles_deg` are dropped (and their weights with them)
    before averaging; returns NaN if nothing is left.

    Replaces: `cluster_reconstruction.py`'s `circular_mean_0_180` (identical
    formula — that function already existed, correctly named and already
    importable, but `wave_clustering.py:249` re-derived the same math inline
    instead of calling it; this is the canonical version going forward) and
    the doubled-angle block inside `build_waves_for_gabor`'s median-orientation
    computation.
    """
    angles_deg = np.asarray(angles_deg, dtype=float)
    if weights is None:
        weights = np.ones_like(angles_deg)
    else:
        weights = np.asarray(weights, dtype=float)

    ok = np.isfinite(angles_deg) & np.isfinite(weights)
    if not np.any(ok):
        return np.nan

    angles_rad = np.deg2rad(angles_deg[ok])
    w = weights[ok]
    doubled = 2.0 * angles_rad
    sin_mean = np.average(np.sin(doubled), weights=w)
    cos_mean = np.average(np.cos(doubled), weights=w)

    mean_deg = np.rad2deg(np.arctan2(sin_mean, cos_mean) / 2.0) % 180.0
    return float(mean_deg)


def circular_mean_full_deg(angles_deg, weights=None):
    """
    Weighted circular mean of full-circle (mod-360°) quantities, in [0, 360).

    Plain (undoubled) circular mean via unit vectors — appropriate for
    azimuth/propagation-direction quantities, where 0° and 180° are distinct.
    `weights=None` gives an unweighted mean. NaN entries are dropped (with
    their weights) before averaging; returns NaN if nothing is left.

    Replaces: the unnamed weighted circular-mean helper in
    `OmegaK_enhanced.py` (~line 1000, identical formula).
    """
    angles_deg = np.asarray(angles_deg, dtype=float)
    if weights is None:
        weights = np.ones_like(angles_deg)
    else:
        weights = np.asarray(weights, dtype=float)

    ok = np.isfinite(angles_deg) & np.isfinite(weights)
    if not np.any(ok):
        return np.nan

    angles_rad = np.deg2rad(angles_deg[ok])
    w = weights[ok]
    sin_mean = np.average(np.sin(angles_rad), weights=w)
    cos_mean = np.average(np.cos(angles_rad), weights=w)

    mean_deg = np.rad2deg(np.arctan2(sin_mean, cos_mean)) % 360.0
    return float(mean_deg)


# ---------------------------------------------------------------------------
# Regression tests — run with:  python angles.py
#
# Covers: every named-replacement claim made in the docstrings above (the
# four "different forms, same answer" implementations of axis_diff_deg, the
# already-known 210°/30° opposite-lobe case from phase_speed_kalman.py's own
# regression test, and the circular-mean functions against known means).
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    rng = np.random.default_rng(0)

    print("=" * 60)
    print("angles.py regression tests")
    print("=" * 60)

    # ── axis_diff_deg vs. the three other existing forms, on random pairs ──
    a = rng.uniform(-720, 720, size=2000)
    b = rng.uniform(-720, 720, size=2000)

    # Form 1: wave_validation.py's _angular_diff / gabor_reconstruction.py's
    # _adiff / phase_speed_kalman.py's _circ_diff_180 (all textually
    # identical to each other already).
    d1 = np.abs(a - b) % 180.0
    form1 = np.minimum(d1, 180.0 - d1)

    # Form 2: the (x + 90) % 180 - 90 form used inline in
    # _kalman_theta_tracker's kernel-mismatch check and its own regression
    # test's _half_diff (theta pre-folded to [0,180) first, as those call
    # sites do).
    a_half = a % 180.0
    form2 = np.abs((a_half - b + 90.0) % 180.0 - 90.0)

    mine = axis_diff_deg(a, b)
    assert np.allclose(mine, form1, atol=1e-9), "FAIL: axis_diff_deg vs form1 (_adiff/_angular_diff/_circ_diff_180)"
    assert np.allclose(mine, form2, atol=1e-9), "FAIL: axis_diff_deg vs form2 ((x+90)%180-90)"
    print("  PASS: axis_diff_deg matches all 3 existing inline forms on 2000 random pairs.")

    # ── full_circle_diff_deg vs. abs(circular_diff_deg(...)) (the fourth form) ──
    signed = (a - b + 180.0) % 360.0 - 180.0
    form3 = np.abs(signed)
    mine_full = full_circle_diff_deg(a, b)
    assert np.allclose(mine_full, form3, atol=1e-9), "FAIL: full_circle_diff_deg vs abs(circular_diff_deg)"
    assert np.allclose(np.abs(signed_circular_diff_deg(a, b)), mine_full, atol=1e-9), \
        "FAIL: abs(signed_circular_diff_deg) != full_circle_diff_deg"
    print("  PASS: full_circle_diff_deg matches abs(circular_diff_deg) form on 2000 random pairs.")

    # ── The known opposite-lobe case from phase_speed_kalman.py's own test:
    #    210 deg and 30 deg are the SAME physical axis (210 % 180 = 30);
    #    axis_diff_deg should read ~0, full_circle_diff_deg should read 180. ──
    az_opposite, kc_half = 210.0, 30.0
    ad = axis_diff_deg(az_opposite, kc_half)
    fd = full_circle_diff_deg(az_opposite, kc_half)
    assert ad < 1e-9, f"FAIL: axis_diff_deg(210, 30) = {ad}, expected ~0"
    assert abs(fd - 180.0) < 1e-9, f"FAIL: full_circle_diff_deg(210, 30) = {fd}, expected 180"
    print(f"  PASS: opposite-lobe case (210 deg vs 30 deg) -> axis_diff={ad:.4f} (expect ~0), "
          f"full_circle_diff={fd:.1f} (expect 180).")

    # ── Family 13/Family 6-style case: two waves ~180 deg apart in azimuth
    #    but sharing an axis (the exact silent-merge bug pattern this
    #    session found) must NOT look close under full_circle_diff_deg even
    #    though axis_diff_deg reads them as the same axis. ──
    az1, az2 = 19.06, 200.66   # DOY 173 Family 13 vs Family 6, established values
    assert axis_diff_deg(az1, az2) < 5.0, "FAIL: Family 13/6 should share an axis"
    assert full_circle_diff_deg(az1, az2) > 170.0, "FAIL: Family 13/6 should be ~opposite in full circle"
    print(f"  PASS: Family 13 (az={az1}) vs Family 6 (az={az2}) -> "
          f"axis_diff={axis_diff_deg(az1, az2):.1f} (small), "
          f"full_circle_diff={full_circle_diff_deg(az1, az2):.1f} (~180) -- "
          f"the case that must never look like a match on full-circle grounds.")

    # ── circular_mean_axis_deg: known case, cluster_reconstruction.py's
    #    circular_mean_0_180 formula, cross-checked directly ──
    angs = np.array([10.0, 15.0, 20.0, 170.0, 175.0])  # straddles the 0/180 wrap
    w = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
    # Reference: cluster_reconstruction.py's own implementation, inlined here
    ang_rad = np.radians(angs); doubled = 2 * ang_rad
    ref = np.degrees(np.arctan2(np.average(np.sin(doubled), weights=w),
                                 np.average(np.cos(doubled), weights=w)) / 2.0)
    ref = ref + 180.0 if ref < 0 else ref
    ref = ref - 180.0 if ref >= 180.0 else ref
    mine_mean = circular_mean_axis_deg(angs, w)
    assert abs(mine_mean - ref) < 1e-9, f"FAIL: circular_mean_axis_deg={mine_mean}, ref={ref}"
    print(f"  PASS: circular_mean_axis_deg matches cluster_reconstruction.py's "
          f"circular_mean_0_180 formula exactly ({mine_mean:.2f} deg, wrap-straddling input).")

    # NaN handling
    angs_nan = np.array([10.0, np.nan, 20.0])
    assert np.isfinite(circular_mean_axis_deg(angs_nan)), "FAIL: should ignore NaN, not propagate it"
    assert np.isnan(circular_mean_axis_deg(np.array([np.nan, np.nan]))), "FAIL: all-NaN should give NaN"
    print("  PASS: circular_mean_axis_deg drops NaN entries; all-NaN input gives NaN.")

    # ── circular_mean_full_deg: simple known case (mean of 0 and 90 is 45) ──
    m = circular_mean_full_deg(np.array([0.0, 90.0]))
    assert abs(m - 45.0) < 1e-9, f"FAIL: circular_mean_full_deg([0,90]) = {m}, expected 45"
    print(f"  PASS: circular_mean_full_deg([0, 90]) = {m:.1f} deg (expected 45).")

    # Wrap-around case: mean of 350 and 10 should be 0 (or 360), not 180
    m2 = circular_mean_full_deg(np.array([350.0, 10.0]))
    assert min(abs(m2 - 0.0), abs(m2 - 360.0)) < 1e-9, f"FAIL: circular_mean_full_deg([350,10]) = {m2}, expected ~0/360"
    print(f"  PASS: circular_mean_full_deg([350, 10]) = {m2:.1f} deg (expected ~0, not 180 -- "
          f"a plain arithmetic mean would get this wrong).")

    print("\nAll angles.py regression tests passed.")
