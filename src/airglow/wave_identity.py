"""
wave_identity.py
-----------------
The wave-identity / frame-ownership module: answers "is this the same
physical wave" and "who owns this frame" at three successive points in the
pipeline, using three different kinds of evidence:

  Stage A — cluster_detections_into_families()
      Groups sparse per-window omega-k detections into discrete wave
      families, before any Gabor reconstruction exists.

  Stage B — resolve_frame_ownership()
      Expands each family's discrete detections into dense per-frame
      ownership on the full night's time grid, resolving any frame where
      more than one family is simultaneously active.

  Stage C — resolve_collisions()
      Given actual reconstructed fields, flags episodes where two or more
      waves' reconstructions are essentially the same physical structure
      (via direct field cross-correlation) and picks one primary per
      episode. Designed to be pass-agnostic (usable on either the first or
      second Gabor pass), but `run_airglow_pipeline` currently only calls it
      for the second pass, with `apply_duplicate_gate=False`; the first pass
      still uses `gabor_reconstruction.check_duplicate_waves`, a separate,
      coarser (whole-night-averaged) check.

  unified_validity_mask()
      Combines Stage B's ownership with Stage C's collision outcome and a
      fresh signal-presence check into one authoritative per-frame validity
      array — the single answer `kernel_tuning.py` and `final_estimation.py`
      both consume.

These three stages are kept separate rather than collapsed into one
function because they operate on three different kinds of evidence (sparse
discrete detections, dense per-frame kinematic continuity, dense per-frame
field similarity) available at three different points in the pipeline, in
an unavoidable sequential dependency.

Angular comparisons throughout this module use `angles.py`'s
`axis_diff_deg`/`full_circle_diff_deg`/`circular_mean_axis_deg`/
`circular_mean_full_deg` rather than reimplementing the arithmetic inline.
"""

import numpy as np
import pandas as pd
from scipy.stats import pearsonr

from airglow.angles import (
    axis_diff_deg, full_circle_diff_deg, circular_mean_axis_deg, circular_mean_full_deg,
)


# ---------------------------------------------------------------------------
# Stage A -- cluster raw per-window detections into families
# ---------------------------------------------------------------------------

def cluster_detections_into_families(
    window_results: list,
    dir_tol_deg: float = 15.0,
    lambda_tol_km: float = 20.0,
    min_detections: int = 1,
    verbose: bool = True,
) -> list:
    """
    Cluster per-window detections into temporally coherent wave families.

    Grouping is based only on direction and wavelength (not speed), so a
    family can track a wave whose speed evolves over time. Merging a
    detection into an existing group requires BOTH axis (mod-180°) AND
    full-circle azimuth proximity — the full-circle check is what stops two
    counter-propagating detections that happen to share an axis from being
    merged into one family.

    Grouping is greedy and order-dependent: each family's centroid
    (`direction_center`/`azimuth_full_center`/`wavelength_center`) is fixed
    at its founding (first-seen) detection, not updated to a running mean,
    so a family's total member spread stays bounded by `dir_tol_deg`/
    `lambda_tol_km` regardless of how many detections it accumulates.

    Parameters
    ----------
    window_results  : list of per-window detection dicts (each with a
        `"physical_waves"` list and a `"center_time"`), one per sliding
        analysis window.
    dir_tol_deg     : max azimuth difference (degrees) to merge into same family
    lambda_tol_km   : max wavelength difference (km) to merge into same family
    min_detections  : minimum number of windows a family must appear in to be kept
    verbose : bool
        Print a per-family summary.

    Returns
    -------
    families : list of dicts, each containing:
        family_id         : int
        direction_center  : mean axis (degrees, [0,180)) of the FOUNDING detection
        azimuth_full_center : mean azimuth (degrees, [0,360)) of the FOUNDING detection
        wavelength_center : wavelength (km) of the FOUNDING detection
        members           : list of per-detection dicts with keys:
                            time, direction, azimuth_full, wavelength, speed,
                            period, power, confidence
    """
    all_waves = []
    for r in window_results:
        for w in r["physical_waves"]:
            full_az = float(w["azimuth"])
            all_waves.append({
                "time":         r["center_time"],
                "direction":    full_az % 180,          # [0, 180) for Gabor orientation
                "azimuth_full": full_az,                # [0, 360) for full-circle propagation
                "wavelength":   float(w["wavelength"]),
                "speed":        float(w["phase_speed"]),
                "period":       float(w["period"]),
                "power":        float(w.get("power",      np.nan)),
                "confidence":   float(w.get("confidence", np.nan)),
            })

    if verbose:
        print(f"Total detections across all windows: {len(all_waves)}")

    # Fixed-centroid greedy grouping: comparing every new detection against
    # each group's founding member (not a running mean) prevents slow drift
    # from chaining detections arbitrarily far from where a family started,
    # one small step at a time.
    groups = []
    for w in all_waves:
        matched = False
        for g in groups:
            if (
                axis_diff_deg(w["direction"], g["direction_center"]) < dir_tol_deg
                and full_circle_diff_deg(w["azimuth_full"], g["azimuth_full_center"]) < dir_tol_deg
                and abs(w["wavelength"] - g["wavelength_center"]) < lambda_tol_km
            ):
                g["members"].append(w)
                matched = True
                break
        if not matched:
            groups.append({
                "direction_center":    w["direction"],
                "azimuth_full_center": w["azimuth_full"],
                "wavelength_center":   w["wavelength"],
                "members":             [w],
            })

    # Per-window deduplication: keep only the strongest-power detection per
    # (family, window-time) pair, in case the peak-finder split one wave's
    # power across two nearby k-bins.
    for g in groups:
        seen_times = {}
        for m in g["members"]:
            t = m["time"]
            if t not in seen_times or m["power"] > seen_times[t]["power"]:
                seen_times[t] = m
        n_before = len(g["members"])
        g["members"] = list(seen_times.values())
        n_after = len(g["members"])
        if verbose and n_after < n_before:
            print(f"  Dedup: removed {n_before - n_after} weaker co-temporal "
                  f"peak(s) in family "
                  f"(dir~{g['direction_center']:.0f} deg, "
                  f"lam~{g['wavelength_center']:.0f} km)")

    families = [g for g in groups if len(g["members"]) >= min_detections]
    families = sorted(families, key=lambda g: -len(g["members"]))
    for i, f in enumerate(families):
        f["family_id"] = i + 1

    if verbose:
        print(f"Wave families found (>={min_detections} detections): {len(families)}")
        for f in families:
            speeds = [m["speed"] for m in f["members"]]
            print(f"  Family {f['family_id']}: dir={f['direction_center']:.1f} deg  "
                  f"lam={f['wavelength_center']:.1f} km  "
                  f"n={len(f['members'])}  "
                  f"speed {min(speeds):.0f}-{max(speeds):.0f} m/s")

    return families


# ---------------------------------------------------------------------------
# Stage B -- resolve dense per-frame ownership from discrete families
# ---------------------------------------------------------------------------

def interpolate_family_to_time_grid(
    family,
    time_grid,
    parameter_keys=("direction", "wavelength", "speed"),
    win_half_ns=0.0,
):
    """
    Interpolate one family's discrete member detections onto the full
    dataset time grid, and mark which grid frames fall within reach of an
    actual detection.

    A grid frame counts as "covered" (`_valid_mask`) only if its nearest
    member detection is within `win_half_ns` — this is the half-window
    extension: each detection is trusted to represent conditions for half
    a window's duration on either side of it, not the whole night. Kept as
    a standalone, per-family function (rather than inlined into
    `resolve_frame_ownership`) because it has no cross-family knowledge —
    the Voronoi tie-break that needs all families' results at once happens
    one level up, after every family has been interpolated independently.

    Parameters
    ----------
    family : dict
        One family from `cluster_detections_into_families`.
    time_grid : np.ndarray of datetime64
        Full night time grid to interpolate onto.
    parameter_keys : tuple of str
        Which member fields to interpolate; `"direction"` gets circular
        interpolation (see below), everything else gets linear `np.interp`.
    win_half_ns : float
        Coverage half-window, in nanoseconds.

    Returns
    -------
    dict with `_valid_mask` (bool, per grid frame), `_nearest_dist`
    (nanoseconds to the nearest member detection, per grid frame), and one
    interpolated array per requested parameter (`direction` is renamed to
    `orientation_deg` in the output).
    """
    members = sorted(family["members"], key=lambda m: m["time"])
    t_mem = np.array(
        [np.datetime64(m["time"], "ns") for m in members],
        dtype="datetime64[ns]",
    ).astype(np.float64)
    t_grid = time_grid.astype(np.float64)

    dist_matrix = np.abs(t_grid[:, None] - t_mem[None, :])
    nearest_dist = dist_matrix.min(axis=1)
    valid_mask = nearest_dist <= win_half_ns

    n_valid = int(valid_mask.sum())
    n_total = len(t_grid)
    out = {"_valid_mask": valid_mask, "_nearest_dist": nearest_dist}

    for key in parameter_keys:
        vals = np.array([m[key] for m in members], dtype=np.float64)
        out_key = "orientation_deg" if key == "direction" else key

        if key == "direction":
            # Circular interpolation for orientation [0,180): double the
            # angle to map onto the full circle, interpolate the unit-vector
            # components (always the short arc), halve back.
            angle2_rad = np.deg2rad(vals * 2.0)
            cos_vals = np.cos(angle2_rad)
            sin_vals = np.sin(angle2_rad)
            cos_interp = np.interp(t_grid, t_mem, cos_vals, left=cos_vals[0], right=cos_vals[-1])
            sin_interp = np.interp(t_grid, t_mem, sin_vals, left=sin_vals[0], right=sin_vals[-1])
            angle2_interp = np.arctan2(sin_interp, cos_interp)
            orient_interp = np.rad2deg(angle2_interp) / 2.0 % 180.0
            result = np.where(valid_mask, orient_interp, np.nan)
        else:
            interp_full = np.interp(t_grid, t_mem, vals, left=vals[0], right=vals[-1])
            result = np.where(valid_mask, interp_full, np.nan)

        out[out_key] = result

    pct = 100.0 * n_valid / n_total if n_total > 0 else 0.0
    print(f"    [family {family.get('family_id', '?')}] "
          f"{n_valid}/{n_total} frames covered ({pct:.0f}%)")
    return out


def resolve_frame_ownership(
    families, time_grid, n_stds=3.0,
    win_half_min=45.0,
    dir_tol_deg=15.0, lambda_tol_km=20.0,
    voronoi_dir_tol=None, voronoi_lam_tol=None,
):
    """
    Build the per-frame-owned wave list from discrete families: interpolate
    each family onto the full time grid (`interpolate_family_to_time_grid`),
    then resolve any frame where more than one family from the same
    proximity group is simultaneously covered, by giving it to whichever
    family's nearest detection is closest in time (a Voronoi-style
    nearest-neighbor tie-break).

    Two families are considered part of the same proximity group — and
    therefore compete for ownership when both cover a frame — if their
    founding centroids are within `voronoi_dir_tol` (axis AND full-circle
    azimuth) and `voronoi_lam_tol` of each other; group membership is
    transitive (union-find), so these default to the same tolerances used
    to build the families in the first place, rather than a looser value
    that could chain together families that were never meant to compete.

    Each wave's scalar seed values (`orientation_deg`, `azimuth`) are the
    circular mean (`angles.circular_mean_axis_deg`/`circular_mean_full_deg`)
    over the family's member detections, not a plain median — a median can
    give a wrong answer for a family whose members straddle the 0/180° or
    0/360° wrap boundary (e.g. `median([350, 10]) == 180`, not ~0°).
    Wavelength and speed are not circular quantities and use a plain
    median. These seed values feed `resolve_collisions`'s own tie-break
    distance metric downstream.

    Returns
    -------
    waves : list of dict, one per family, each with '_owned_mask' (bool,
        shape (n_time,)) as its authoritative per-frame ownership answer.
    """
    win_half_ns = win_half_min * 60.0 * 1e9
    n_time = len(time_grid)
    n_fam = len(families)

    all_interp = []
    valid_stack = np.zeros((n_fam, n_time), dtype=bool)
    dist_stack = np.full((n_fam, n_time), np.inf)

    for fi, fam in enumerate(families):
        interp = interpolate_family_to_time_grid(
            fam, time_grid,
            parameter_keys=("direction", "wavelength", "speed"),
            win_half_ns=win_half_ns,
        )
        all_interp.append(interp)
        valid_stack[fi] = interp["_valid_mask"]
        dist_stack[fi] = interp["_nearest_dist"]

    if voronoi_dir_tol is None:
        voronoi_dir_tol = dir_tol_deg
    if voronoi_lam_tol is None:
        voronoi_lam_tol = lambda_tol_km

    group_id = list(range(n_fam))
    for i in range(n_fam):
        for j in range(i + 1, n_fam):
            if (axis_diff_deg(families[i]["direction_center"],
                               families[j]["direction_center"]) < voronoi_dir_tol
                    and full_circle_diff_deg(families[i]["azimuth_full_center"],
                                              families[j]["azimuth_full_center"]) < voronoi_dir_tol
                    and abs(families[i]["wavelength_center"]
                            - families[j]["wavelength_center"]) < voronoi_lam_tol):
                old_g = group_id[j]; new_g = group_id[i]
                group_id = [new_g if g == old_g else g for g in group_id]

    unique_groups = sorted(set(group_id))
    print(f"\nWave group assignment "
          f"(voronoi_dir_tol={voronoi_dir_tol:.0f} deg, "
          f"voronoi_lam_tol={voronoi_lam_tol:.0f} km):")
    for g in unique_groups:
        mig = [fi for fi, gid in enumerate(group_id) if gid == g]
        labels = ", ".join(
            f"Family {families[fi]['family_id']} "
            f"(dir={families[fi]['direction_center']:.0f} deg, "
            f"lam={families[fi]['wavelength_center']:.0f} km)"
            for fi in mig
        )
        print(f"  Wave group {g}: {labels}")

    owned_stack = valid_stack.copy()
    for g in unique_groups:
        mig = [fi for fi, gid in enumerate(group_id) if gid == g]
        if len(mig) < 2:
            continue
        for t in range(n_time):
            competing = [fi for fi in mig if valid_stack[fi, t]]
            if len(competing) < 2:
                continue
            best = competing[int(np.argmin([dist_stack[fi, t] for fi in competing]))]
            for fi in competing:
                if fi != best:
                    owned_stack[fi, t] = False

    print(f"\nOwnership summary ({n_time} frames total):")
    any_unowned = np.all(~owned_stack, axis=0)
    n_unowned = int(any_unowned.sum())
    for fi, fam in enumerate(families):
        n_owned = int(owned_stack[fi].sum())
        print(f"  Family {fam['family_id']}: {n_owned} frames ({100. * n_owned / n_time:.0f}%)")
    if n_unowned > 0:
        print(f"  Frames with no family active: {n_unowned} ({100. * n_unowned / n_time:.0f}%)")
    else:
        print("  All frames covered by at least one family.")

    waves = []
    for fi, (fam, interp) in enumerate(zip(families, all_interp)):
        members = fam["members"]
        owned_mask = owned_stack[fi]
        orient_series = np.where(owned_mask, interp["orientation_deg"], np.nan)
        wl_series = np.where(owned_mask, interp["wavelength"], np.nan)

        median_orient = circular_mean_axis_deg([m["direction"] for m in members])
        median_wl = float(np.median([m["wavelength"] for m in members]))
        median_speed = float(np.median([m["speed"] for m in members]))
        full_azimuths = [m.get("azimuth_full", m["direction"]) for m in members]
        median_azimuth = circular_mean_full_deg(full_azimuths)

        waves.append({
            "label": (f"Family {fam['family_id']}  "
                      f"(az~{median_azimuth:.0f} deg, "
                      f"lam~{median_wl:.0f} km)"),
            "orientation_deg": median_orient,
            "wavelength_km": median_wl,
            "azimuth": median_azimuth,
            "phase_speed": median_speed,
            "n_stds": n_stds,
            "orientation_deg_series": orient_series,
            "wavelength_km_series": wl_series,
            "_owned_mask": owned_mask,
            "_win_half_min": win_half_min,
            "_group_id": group_id[fi],
        })

    return waves


# ---------------------------------------------------------------------------
# Stage C -- resolve field-correlation collisions, pass-agnostic
# ---------------------------------------------------------------------------

def resolve_collisions(
    waves: list,
    wave_cubes: list,
    complex_amplitudes: list,
    time_vals: np.ndarray,
    pass_label: str,
    corr_threshold: float = 0.7,
    smoothing_window: int = 5,
    min_episode_frames: int = 3,
    dir_tol_deg: float = 22.5,
    lambda_tol_km: float = 20.0,
    apply_duplicate_gate: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Flag episodes where two or more waves' reconstructions are essentially
    the same physical structure, via direct field cross-correlation, and
    resolve one primary per episode.

    Algorithm: per-pair correlation (smoothed with a `smoothing_window`
    rolling mean) → per-frame connected components (any pair whose smoothed
    correlation clears `corr_threshold` is linked) → contiguous-frame
    episode detection per component → an eligibility-gated,
    founding-parameter-distance tie-break picks one primary member per
    episode. `pass_label` is a logging-only string ("first-pass" /
    "second-pass" or similar); this function does not branch on it.

    Designed to be pass-agnostic — usable on the first-pass or second-pass
    reconstruction — but **callers must decide `apply_duplicate_gate`
    correctly for the pass they're calling on**: the second-pass call in
    `run_airglow_pipeline` passes `apply_duplicate_gate=False`. Passing
    `True` on a call this function wasn't validated for is not caught
    automatically.

    Eligibility gate: a wave with amplitude effectively zero for an
    episode's entire span is excluded from the episode's comparison group
    when `apply_duplicate_gate=True` (it can't meaningfully "lose" a
    collision to a duplicate it never had real signal to duplicate), and
    left with `collision_primary=True` untouched. If every member of an
    episode is signal-free, falls back to a distance-only choice over the
    full group. With `apply_duplicate_gate=False`, zero-amplitude waves stay
    in the full comparison group; eligibility only preferences which member
    is picked primary, never excludes group membership.

    Duplicate-detection gate, `apply_duplicate_gate`: when True (the
    default), correlation alone is not sufficient for two waves to be
    treated as colliding at a frame — an edge also requires
    `axis_diff_deg(orientation) < dir_tol_deg` AND
    `abs(wavelength diff) < lambda_tol_km`, both evaluated on the same
    smoothed rolling window as correlation. This exists because correlation
    alone can cross `corr_threshold` for two waves with visibly different
    orientation or wavelength.

    Known limitation: `lambda_tol_km`/`dir_tol_deg` here reuse the same
    tolerances Stage A/B use for a different question (whether one wave's
    own tracked parameters have drifted too far from where it started, a
    temporal-continuity check) — they haven't been separately validated for
    "are these two simultaneously-reconstructed waves the same physical
    wave." A fixed absolute-km wavelength tolerance in particular can admit
    a real, non-noisy relative wavelength gap between two visibly different
    fields. Accepted for now because this gate's output feeds an
    intermediate kernel-tuning signal, not final output, so an occasional
    false-negative here is lower-cost than the same failure would be in
    final parameter estimation.

    Known limitation: `smoothing_window=5` has no documented physical
    rationale (unlike `corr_threshold`, which was set just above the
    observed p99 of pairwise correlation on real data) — inherited as-is,
    not re-derived here.

    `complex_amplitudes` should be the SAME pass's own amplitude series as
    `wave_cubes` (first-pass amplitude for a first-pass call, second-pass
    for a second-pass call) — this is what the eligibility gate and
    tie-break both key off of.

    Parameters
    ----------
    waves : list of dict
        Must have 'label', 'orientation_deg', 'wavelength_km' (founding
        scalar values) and 'orientation_deg_series'/'wavelength_km_series'
        ((n_time,) arrays for this pass -- for the first pass this is
        `waves` straight from resolve_frame_ownership(); for the second
        pass it's the KF-refined per-frame series built by kernel_tuning.py).
    wave_cubes : list of np.ndarray, each (n_time, ny, nx)
        This pass's reconstructed fields, one per wave, same order as `waves`.
    complex_amplitudes : list of np.ndarray, each (n_time,) complex
        This pass's cross-correlation phasor per wave (from
        `reconstruct_all_waves`' own return value) -- used for the
        eligibility gate via |A|.
    time_vals : np.ndarray of datetime64
        Full night time grid, same length/order as wave_cubes[i].
    pass_label : str
        Logging label only ("first-pass" / "second-pass" or similar).

    Returns
    -------
    pd.DataFrame, one row per (wave_id, frame), columns:
        wave_id, time, frame_idx, pass, collision_id, collision_group_wave_ids,
        collision_primary, collision_primary_wave_id
    """
    n = len(waves)
    n_time = len(time_vals)

    base_rows = pd.DataFrame({
        "wave_id": np.repeat(np.arange(n), n_time),
        "time": np.tile(time_vals, n),
        "frame_idx": np.tile(np.arange(n_time), n),
    })
    base_rows["collision_id"] = -1
    base_rows["collision_group_wave_ids"] = ""
    base_rows["collision_primary_wave_id"] = -1
    base_rows["collision_primary"] = True

    if n < 2 or len(wave_cubes) < 2:
        if verbose:
            print(f"No collision check ({pass_label}): fewer than 2 waves.")
        return base_rows

    smoothed = {}
    axis_diff_by_pair = {}
    wl_diff_by_pair = {}
    for i in range(n):
        for j in range(i + 1, n):
            cube_i, cube_j = wave_cubes[i], wave_cubes[j]
            raw = np.full(n_time, np.nan)
            for t in range(n_time):
                a, b = cube_i[t].ravel(), cube_j[t].ravel()
                if not (np.any(a) and np.any(b)):
                    continue
                m = np.isfinite(a) & np.isfinite(b)
                if m.sum() > 1:
                    r, _ = pearsonr(a[m], b[m])
                    if np.isfinite(r):
                        raw[t] = r
            smoothed[(i, j)] = (
                pd.Series(raw).rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
            )
            # Per-frame axis/wavelength proximity for the duplicate-
            # detection gate (see docstring). Smoothed with the SAME
            # rolling window as correlation, not compared instantaneous --
            # comparing a smoothed correlation series against instantaneous
            # axis/wavelength values lets genuinely-different pairs slip
            # through on frames where correlation dipped momentarily but
            # axis/wavelength still happened to read close.
            if apply_duplicate_gate:
                oi = np.asarray(waves[i]["orientation_deg_series"], dtype=float) % 180.0
                oj = np.asarray(waves[j]["orientation_deg_series"], dtype=float) % 180.0
                li = np.asarray(waves[i]["wavelength_km_series"], dtype=float)
                lj = np.asarray(waves[j]["wavelength_km_series"], dtype=float)
                axis_diff_by_pair[(i, j)] = (
                    pd.Series(axis_diff_deg(oi, oj))
                    .rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
                )
                wl_diff_by_pair[(i, j)] = (
                    pd.Series(np.abs(li - lj))
                    .rolling(window=smoothing_window, min_periods=1).mean().to_numpy()
                )

    all_ids = list(range(n))

    def _components(edges):
        parent = {wid: wid for wid in all_ids}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for a, b in edges:
            union(a, b)
        groups = {}
        for wid in all_ids:
            groups.setdefault(find(wid), set()).add(wid)
        return [frozenset(g) for g in groups.values() if len(g) >= 2]

    frame_groups = []
    for t in range(n_time):
        edges = []
        for pair, s in smoothed.items():
            if not (np.isfinite(s[t]) and s[t] >= corr_threshold):
                continue
            if apply_duplicate_gate:
                ad = axis_diff_by_pair[pair][t]
                wld = wl_diff_by_pair[pair][t]
                if not (np.isfinite(ad) and ad < dir_tol_deg):
                    continue
                if not (np.isfinite(wld) and wld < lambda_tol_km):
                    continue
            edges.append(pair)
        frame_groups.append(set(_components(edges)))

    episodes = []
    open_eps = {}
    for t in range(n_time + 1):
        active = frame_groups[t] if t < n_time else set()
        for g in list(open_eps.keys()):
            if g not in active:
                start = open_eps.pop(g)
                if t - start >= min_episode_frames:
                    episodes.append((g, start, t - 1))
        for g in active:
            if g not in open_eps:
                open_eps[g] = t

    if not episodes:
        if verbose:
            print(f"No correlation-based collisions detected ({pass_label}, "
                  f"threshold={corr_threshold:.2f}).")
        return base_rows

    label_map = {i: w["label"] for i, w in enumerate(waves)}
    amp_by_wave = {wid: np.abs(complex_amplitudes[wid]) for wid in range(n)}

    rows_by_key = {}  # (wave_id, frame_idx) -> row dict, for O(1) write-back
    for wid in range(n):
        for t in range(n_time):
            rows_by_key[(wid, t)] = dict(
                wave_id=wid, time=time_vals[t], frame_idx=t,
                collision_id=-1, collision_group_wave_ids="",
                collision_primary_wave_id=-1, collision_primary=True,
            )

    next_cid = 0
    printed = []
    for group, t_start, t_end in episodes:
        seg = slice(t_start, t_end + 1)
        eligible = set()
        for wid in group:
            amp_i = np.nan_to_num(amp_by_wave[wid][seg], nan=0.0)
            if not np.all(amp_i < 1e-9):
                eligible.add(wid)

        # Zero-amplitude ineligibility: under the duplicate gate, this
        # excludes signal-free members from group membership entirely, not
        # just primary-preference -- a signal-free member can't be marked
        # collision_primary=False for "losing" to a duplicate it had no
        # real presence to lose to. With the gate off, `eligible` only
        # preferences which member is picked primary; every member of
        # `group` still gets a row.
        if apply_duplicate_gate:
            comparison_group = eligible if eligible else set(group)
        else:
            comparison_group = set(group)

        dists = {}
        for wid in comparison_group:
            gi = float(waves[wid]["orientation_deg"]) % 180.0
            gli = float(waves[wid]["wavelength_km"])
            oi = waves[wid]["orientation_deg_series"][seg]
            li = waves[wid]["wavelength_km_series"][seg]
            d = np.nanmean(axis_diff_deg(oi % 180.0, gi) / dir_tol_deg
                            + np.abs(li - gli) / lambda_tol_km)
            dists[wid] = d if np.isfinite(d) else np.inf

        if apply_duplicate_gate:
            primary = min(comparison_group, key=dists.get)
        else:
            candidates = eligible if eligible else set(dists)
            primary = min(candidates, key=dists.get)

        cid = next_cid
        next_cid += 1
        group_str = ", ".join(label_map[w] for w in sorted(comparison_group))
        printed.append((comparison_group, t_start, t_end, primary))

        for wid in comparison_group:
            for t in range(t_start, t_end + 1):
                row = rows_by_key[(wid, t)]
                row["collision_id"] = cid
                row["collision_group_wave_ids"] = group_str
                row["collision_primary_wave_id"] = primary
                row["collision_primary"] = (wid == primary)

    if verbose:
        print(f"{len(episodes)} correlation-based collision episode(s) "
              f"detected ({pass_label}, threshold={corr_threshold:.2f}):")
        for group, t_start, t_end, primary in printed:
            members = ", ".join(label_map[w] for w in sorted(group))
            print(f"  {{{members}}}  frames {t_start}-{t_end} ({t_end - t_start + 1} frames) "
                  f"-- kept {label_map[primary]}")

    out = pd.DataFrame(rows_by_key.values())
    out["pass"] = pass_label
    return out.sort_values(["wave_id", "frame_idx"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Unified per-frame validity mask
# ---------------------------------------------------------------------------

def unified_validity_mask(owned_mask, collision_primary, amplitude, snr_min_amplitude=0.05):
    """
    The single authoritative per-frame validity answer, combining Stage B's
    ownership with Stage C's collision outcome and a fresh signal-presence
    check -- computed once here so `kernel_tuning.py` and
    `final_estimation.py` both consume the same definition.

    Closes a real gap `collision_primary` has on its own: it defaults True
    for any frame that never triggered the correlation check at all --
    including a frame with no real signal that also doesn't resemble any
    other wave. `amplitude > 0` and the fresh SNR check catch that case
    directly, in whichever pass's own amplitude series is passed in
    (should be `wave_cubes`/`complex_amplitudes`-derived for the same pass
    `collision_primary` was resolved against).

    Parameters
    ----------
    owned_mask : np.ndarray, bool, shape (n_time,)
        Stage B's per-frame ownership answer for this wave.
    collision_primary : np.ndarray, bool, shape (n_time,)
        Stage C's per-frame outcome for this wave, this pass.
    amplitude : np.ndarray, float, shape (n_time,)
        |complex_amplitudes| for this wave, this pass.
    snr_min_amplitude : float
        Same threshold already tuned/used elsewhere in the pipeline for
        this exact purpose (kf_snr_min) -- reused, not reinvented.

    Returns
    -------
    np.ndarray, bool, shape (n_time,)
    """
    owned_mask = np.asarray(owned_mask, dtype=bool)
    collision_primary = np.asarray(collision_primary, dtype=bool)
    amp = np.nan_to_num(np.asarray(amplitude, dtype=float), nan=0.0)
    amp_ref = amp.max() if amp.max() > 0 else 1.0
    return owned_mask & collision_primary & (amp > 1e-9) & ((amp / amp_ref) >= snr_min_amplitude)
