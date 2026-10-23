"""
legacy_prerefactor_pipeline.py
-------------------------------
Archived pre-refactor Stage A/B/C wave-identity chain, retired by this
session's five-stage rewrite. Moved here (not deleted) per
PIPELINE_CLEANUP.md Step 3, following PIPELINE_AUDIT_RESULTS.md's finding
that these functions had no live callers left inside run_airglow_pipeline
itself but were still exercised by other, currently-usable notebooks
(CaseStudy_RealData_OneNight.ipynb and its snapshots, and the archived
mc_pipeline.py -- see this repo's sibling notebook-directory archive/).

What this replaced
-------------------
    build_wave_families                         -> wave_identity.cluster_detections_into_families
    build_waves_for_gabor                       -> wave_identity.resolve_frame_ownership
    interpolate_family_to_time_grid (private helper of the above)
                                                 -> wave_identity.interpolate_family_to_time_grid
    flag_kf_wave_collisions_by_correlation      -> wave_identity.resolve_collisions

Removing these four names from their original files (wave_validation.py,
gabor_reconstruction.py, phase_speed_kalman.py) also resolves the
interpolate_family_to_time_grid naming collision PIPELINE_AUDIT_RESULTS.md
flagged -- there is now only one definition of that name in the live
package, in wave_identity.py.

The one substantive edit made while moving flag_kf_wave_collisions_by_correlation:
removed a genuinely unreachable duplicate `return df` at the end of the
function (dead code after the real return, confirmed via control-flow
before removal) -- no behavior change.

Why this is archived, not deleted: real historical value -- this is the
mechanism that produced every pre-refactor pipeline run's climatology
results, and CaseStudy_RealData_OneNight.ipynb + mc_pipeline.py (both also
archived) still call it directly. See PIPELINE_AUDIT_RESULTS.md for the
full call-graph evidence and PIPELINE_CLEANUP.md for the cleanup decision
record.
"""

import numpy as np
import pandas as pd
from scipy.stats import pearsonr


def build_wave_families(
    window_results: list,
    dir_tol_deg: float = 15.0,
    lambda_tol_km: float = 20.0,
    min_detections: int = 1,
) -> list:
    """
    Cluster per-window detections into temporally coherent wave families.

    Grouping is based only on direction and wavelength (not speed), so a
    family can track a wave whose speed evolves over time.

    Parameters
    ----------
    window_results  : output of sliding_window_analysis()
    dir_tol_deg     : max azimuth difference (degrees) to merge into same family
    lambda_tol_km   : max wavelength difference (km) to merge into same family
    min_detections  : minimum number of windows a family must appear in to be kept

    Returns
    -------
    families : list of dicts, each containing:
        family_id         : int
        direction_center  : mean azimuth (degrees, 0=N, 90=E, cw)
        wavelength_center : mean wavelength (km)
        members           : list of per-detection dicts with keys:
                            time, direction, wavelength, speed, period,
                            power, confidence
    """

    def _angular_diff(a, b):
        d = abs(a - b) % 180
        return min(d, 180 - d)

    def _full_circle_diff(a, b):
        d = abs(a - b) % 360
        return min(d, 360 - d)

    # Flatten all detections
    all_waves = []
    for r in window_results:
        for w in r["physical_waves"]:
            full_az = float(w["azimuth"])
            all_waves.append({
                "time":         r["center_time"],
                "direction":    full_az % 180,          # [0, 180) for Gabor orientation
                "azimuth_full": full_az,                # [0, 360) for KF sign convention
                "wavelength":   float(w["wavelength"]),
                "speed":        float(w["phase_speed"]),
                "period":       float(w["period"]),
                "power":        float(w.get("power",      np.nan)),
                "confidence":   float(w.get("confidence", np.nan)),
            })

    print(f"Total detections across all windows: {len(all_waves)}")

    # Greedy grouping — centroid is FIXED at the founding detection.
    #
    # Critically, we do NOT update direction_center / wavelength_center
    # after a group is created.  Comparing against a running mean allows
    # slow parameter drift to chain detections arbitrarily far from the
    # founding values (e.g. a wave rotating 60° can be absorbed into one
    # family via 24 incremental 2.5° steps).  Using a fixed founding
    # centroid bounds the total parameter spread to ≤ dir_tol_deg and
    # ≤ lambda_tol_km, which is the intended behaviour.
    #
    # Consequence: a wave whose azimuth rotates by 60° over 6 hours will
    # be split into ~(60/dir_tol_deg) families, each covering one segment
    # of the rotation.  The Voronoi step in build_waves_for_gabor then
    # stitches their reconstructions into a seamless time series.
    # _angular_diff alone is not enough: it wraps every 180 deg, so an axis
    # of 14 deg and one of 175 deg read as only 19 deg apart (both sit near
    # the 0/180 boundary from opposite sides) even though the two waves
    # propagate in nearly opposite true directions (azimuth_full 194 deg vs
    # 355 deg, 161 deg apart) -- silently merging genuinely different waves
    # into one "family" and violating the fixed-founding-centroid invariant
    # this loop is supposed to guarantee. Require azimuth_full proximity
    # too (a real full-circle distance, not axis-only) so only detections
    # that agree on both the spatial axis AND the propagation direction can
    # join the same group.
    groups = []
    for w in all_waves:
        matched = False
        for g in groups:
            if (
                _angular_diff(w["direction"], g["direction_center"]) < dir_tol_deg
                and _full_circle_diff(w["azimuth_full"], g["azimuth_full_center"]) < dir_tol_deg
                and abs(w["wavelength"] - g["wavelength_center"]) < lambda_tol_km
            ):
                g["members"].append(w)
                # Do NOT update direction_center or wavelength_center here.
                # The founding centroid is the reference for all future matches.
                matched = True
                break
        if not matched:
            groups.append({
                "direction_center":    w["direction"],
                "azimuth_full_center": w["azimuth_full"],
                "wavelength_center":   w["wavelength"],
                "members":             [w],
            })

    # Per-window deduplication: when the peak-finder splits one wave's power
    # across two nearby k-bins (common during parameter ramps), both detections
    # land in the same family at the same timestamp.  Keep only the
    # strongest-power detection per (family, window-time) pair so the time
    # series has at most one point per window.
    for g in groups:
        seen_times = {}
        for m in g["members"]:
            t = m["time"]
            if t not in seen_times or m["power"] > seen_times[t]["power"]:
                seen_times[t] = m
        n_before = len(g["members"])
        g["members"] = list(seen_times.values())
        n_after = len(g["members"])
        if n_after < n_before:
            print(f"  Dedup: removed {n_before - n_after} weaker co-temporal "
                  f"peak(s) in family "
                  f"(dir~{g['direction_center']:.0f} deg, "
                  f"lam~{g['wavelength_center']:.0f} km)")

    # Filter and sort
    families = [g for g in groups if len(g["members"]) >= min_detections]
    families = sorted(families, key=lambda g: -len(g["members"]))

    for i, f in enumerate(families):
        f["family_id"] = i + 1

    print(f"Wave families found (≥{min_detections} detections): {len(families)}")
    for f in families:
        times  = [m["time"]  for m in f["members"]]
        speeds = [m["speed"] for m in f["members"]]
        print(f"  Family {f['family_id']}: dir={f['direction_center']:.1f}°  "
              f"λ={f['wavelength_center']:.1f} km  "
              f"n={len(f['members'])}  "
              f"speed {min(speeds):.0f}–{max(speeds):.0f} m/s")

    return families


def interpolate_family_to_time_grid(
    family,
    time_grid,
    parameter_keys=("direction", "wavelength", "speed"),
    win_half_ns=0.0,
):
    """
    Map family member parameters onto the full dataset time grid using
    Option 1 (half-window extension) coverage semantics.

    A detection at centre time t_c represents the interval
    [t_c - W/2, t_c + W/2].  A grid frame is "covered" by this family
    if it falls within W/2 of **any** detection centre.  Because adjacent
    window centres are typically separated by one hop (~10 min) and
    W/2 is ~45 min, any within-family gap smaller than W is automatically
    bridged by the overlapping half-window balls.  Gaps larger than W
    produce NaN naturally.

    No extrapolation beyond first/last detection centre ± W/2.

    This function is called per-family.  The **Voronoi (Option 2)**
    tie-breaking between families is handled by ``build_waves_for_gabor``,
    which calls this function for each family and then assigns ownership
    of contested frames to the family whose nearest detection centre is
    closest in time.

    Parameters
    ----------
    family : dict
        One element from ``build_wave_families()``.
    time_grid : np.ndarray of np.datetime64
        Full-dataset time axis.
    parameter_keys : tuple of str
        Member dict keys to interpolate.  ``"direction"`` is remapped to
        ``"orientation_deg"`` in the output.
    win_half_ns : float
        Half the analysis window duration in **nanoseconds**.
        Pass ``WIN_LEN_MIN / 2 * 60 * 1e9``.

    Returns
    -------
    out : dict
        ``"orientation_deg"``, ``"wavelength"``, ``"speed"`` — each
        ``np.ndarray (n_time,)`` with NaN where not covered.
        ``"_valid_mask"``   — bool array, True where covered.
        ``"_nearest_dist"`` — float array, ns distance to nearest
                              detection centre (used for Voronoi).
    """
    members = sorted(family["members"], key=lambda m: m["time"])
    t_mem = np.array(
        [np.datetime64(m["time"], "ns") for m in members],
        dtype="datetime64[ns]",
    ).astype(np.float64)
    t_grid = time_grid.astype(np.float64)

    # Distance from each grid frame to its nearest detection centre (ns)
    # shape: (n_time,)
    dist_matrix  = np.abs(t_grid[:, None] - t_mem[None, :])   # (n_time, n_det)
    nearest_dist = dist_matrix.min(axis=1)                     # (n_time,)

    # Option 1: valid if within W/2 of any detection centre
    valid_mask = nearest_dist <= win_half_ns

    n_valid = int(valid_mask.sum())
    n_total = len(t_grid)

    out = {"_valid_mask": valid_mask, "_nearest_dist": nearest_dist}

    for key in parameter_keys:
        vals = np.array([m[key] for m in members], dtype=np.float64)
        out_key = "orientation_deg" if key == "direction" else key

        if key == "direction":
            # ── Circular interpolation for orientation [0°, 180°) ────────
            # Linear np.interp in raw degrees goes the wrong way through the
            # 0°/180° boundary (e.g. 172° → 6° sweeps 166° instead of 14°).
            # Fix: double the angles to map [0°,180°) onto the full circle
            # [0°,360°), interpolate the unit-vector (cos, sin) components
            # separately (always choosing the short arc), then halve back.
            angle2_rad = np.deg2rad(vals * 2.0)
            cos_vals   = np.cos(angle2_rad)
            sin_vals   = np.sin(angle2_rad)

            cos_interp = np.interp(t_grid, t_mem, cos_vals,
                                   left=cos_vals[0], right=cos_vals[-1])
            sin_interp = np.interp(t_grid, t_mem, sin_vals,
                                   left=sin_vals[0], right=sin_vals[-1])

            angle2_interp = np.arctan2(sin_interp, cos_interp)
            orient_interp = np.rad2deg(angle2_interp) / 2.0 % 180.0
            result = np.where(valid_mask, orient_interp, np.nan)
        else:
            # Non-angular parameters: plain linear interpolation
            interp_full = np.interp(t_grid, t_mem, vals,
                                    left=vals[0], right=vals[-1])
            result = np.where(valid_mask, interp_full, np.nan)

        out[out_key] = result

    pct = 100.0 * n_valid / n_total if n_total > 0 else 0.0
    print(f"    [family {family.get('family_id','?')}] "
          f"{n_valid}/{n_total} frames covered ({pct:.0f}%)")
    return out


def build_waves_for_gabor(families, time_grid, n_stds=3.0,
                          win_half_min=45.0,
                          dir_tol_deg=15.0, lambda_tol_km=20.0,
                          voronoi_dir_tol=None, voronoi_lam_tol=None):
    """
    Build the ``waves_detected``-style list with per-frame parameter arrays,
    using Option 1 (half-window extension) + Option 2 (Voronoi) to give
    every frame an unambiguous owner.

    **Option 1 — half-window extension**
        Each detection at centre t_c covers [t_c - W/2, t_c + W/2].
        A grid frame is valid for a family if it falls within W/2 of any
        of that family's detection centres.

    **Option 2 — Voronoi tie-breaking**
        When multiple families are simultaneously valid at a frame
        (possible during parameter transitions), ownership goes to the
        family whose nearest detection centre is closest in time.
        This produces a clean partition with no gaps and no overlap:
        every frame belongs to exactly one family (or to none if no
        family is valid at all).

    The two rules together guarantee:
    - No artificial gap at family transitions (Option 1 extends coverage).
    - No double-reconstruction of any frame (Option 2 partitions overlaps).
    - Genuine absences (wave disappeared for longer than W) remain NaN.

    Parameters
    ----------
    families    : output of ``build_wave_families()``
    time_grid   : full-dataset time axis (np.ndarray of np.datetime64)
    n_stds      : Gabor kernel half-width in sigma units
    win_half_min : half the sliding-window duration in **minutes**
                  (pass ``WIN_LEN_MIN / 2``).
    dir_tol_deg  : direction tolerance used when building families (degrees).
                   Controls the default Voronoi grouping threshold.
    lambda_tol_km : wavelength tolerance used when building families (km).
                    Controls the default Voronoi grouping threshold.
    voronoi_dir_tol : max direction difference (degrees) between two family
        founding centroids for them to be treated as the **same physical
        wave** (and therefore subject to Voronoi tie-breaking rather than
        independent ownership).  Defaults to ``dir_tol_deg`` (1×), matching
        the family-building threshold.
        Rule of thumb: set equal to ``DIR_TOL_DEG`` used in
        ``build_wave_families()``.  The old behaviour was ``2*dir_tol_deg``
        which caused transitive over-grouping when three or more families
        lined up incrementally in direction or wavelength space.
    voronoi_lam_tol : same as ``voronoi_dir_tol`` but for wavelength (km).
        Defaults to ``lambda_tol_km`` (1×).

    Returns
    -------
    waves : list of dict, one per family
    """
    win_half_ns = win_half_min * 60.0 * 1e9   # minutes → nanoseconds
    n_time      = len(time_grid)
    n_fam       = len(families)

    # ── Pass 1: per-family interpolation and coverage ─────────────────────
    all_interp     = []
    valid_stack    = np.zeros((n_fam, n_time), dtype=bool)
    dist_stack     = np.full( (n_fam, n_time), np.inf)

    for fi, fam in enumerate(families):
        interp = interpolate_family_to_time_grid(
            fam, time_grid,
            parameter_keys=("direction", "wavelength", "speed"),
            win_half_ns=win_half_ns,
        )
        all_interp.append(interp)
        valid_stack[fi] = interp["_valid_mask"]
        dist_stack[fi]  = interp["_nearest_dist"]
    # ── Pass 2: group families into physical waves, Voronoi within groups ──
    #
    # Two families belong to the same physical wave if their founding
    # centroids are within voronoi_dir_tol and voronoi_lam_tol of each other.
    # Voronoi tie-breaking is applied ONLY within such groups so that truly
    # distinct simultaneous waves each get independent ownership of their
    # covered frames.
    #
    # Default: 1× the family-building tolerances.  The grouping is
    # TRANSITIVE (union-find), so a 2× multiplier can chain families across
    # large parameter gaps (e.g. λ=43, 72, 108 km all in one group via two
    # 36-km hops even though 43↔108 km is clearly two different waves).
    if voronoi_dir_tol is None:
        voronoi_dir_tol = dir_tol_deg
    if voronoi_lam_tol is None:
        voronoi_lam_tol = lambda_tol_km

    def _adiff(a, b):
        d = abs(a - b) % 180
        return min(d, 180 - d)

    # Mirrors build_wave_families's own _full_circle_diff (wave_validation.py)
    # -- that one is a nested function local to build_wave_families and isn't
    # importable without hoisting it out of an already-correct module, so the
    # 2-line pattern is duplicated here rather than the object being reused.
    def _full_circle_diff(a, b):
        d = abs(a - b) % 360
        return min(d, 360 - d)

    # _adiff alone is not enough: it wraps every 180 deg, so two families whose
    # axes coincide but whose true propagation directions are ~180 deg apart
    # (azimuth_full ~19 deg vs ~201 deg) pass the axis check and get pulled
    # into one Voronoi group even though they are two distinct, counter-
    # propagating waves -- the group's Voronoi tie-break then hands one of
    # them's covered frames to the other, and downstream the weaker one's
    # frozen, ownership-blind second-pass reconstruction reconstructs the
    # stronger one's real signal under its own label (see
    # FAMILY13_INVESTIGATION.md). Require azimuth_full proximity too, exactly
    # as build_wave_families already does for its own (upstream, and already
    # correct) grouping step -- see wave_validation.py:338-341.
    group_id = list(range(n_fam))
    for i in range(n_fam):
        for j in range(i + 1, n_fam):
            if (_adiff(families[i]["direction_center"],
                       families[j]["direction_center"]) < voronoi_dir_tol
                    and _full_circle_diff(families[i]["azimuth_full_center"],
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

    # Start from full valid_stack; Voronoi only within same wave group
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
    n_unowned   = int(any_unowned.sum())
    for fi, fam in enumerate(families):
        n_owned = int(owned_stack[fi].sum())
        print(f"  Family {fam['family_id']}: {n_owned} frames "
              f"({100.*n_owned/n_time:.0f}%)")
    if n_unowned > 0:
        print(f"  Frames with no family active: {n_unowned} "
              f"({100.*n_unowned/n_time:.0f}%)")
        print(f"  NOTE: unowned frames receive zero reconstruction.")
        print(f"  Causes: edges of dataset (first/last W/2={win_half_min:.0f} min),")
        print(f"          or inter-family gaps wider than W={2*win_half_min:.0f} min.")
    else:
        print(f"  All frames covered by at least one family.")

    waves = []
    for fi, (fam, interp) in enumerate(zip(families, all_interp)):
        members = fam["members"]

        owned_mask    = owned_stack[fi]
        orient_series = np.where(owned_mask, interp["orientation_deg"], np.nan)
        wl_series     = np.where(owned_mask, interp["wavelength"],      np.nan)

        # Median scalar values for KF initialisation
        median_orient  = float(np.median([m["direction"]  for m in members]))
        median_wl      = float(np.median([m["wavelength"] for m in members]))
        median_speed   = float(np.median([m["speed"]      for m in members]))
        full_azimuths  = [m.get("azimuth_full", m["direction"]) for m in members]
        median_azimuth = float(np.median(full_azimuths))

        waves.append({
            "label":            (f"Family {fam['family_id']}  "
                                 f"(az~{median_azimuth:.0f} deg, "
                                 f"lam~{median_wl:.0f} km)"),
            # Scalar KF seed values
            "orientation_deg":  median_orient,
            "wavelength_km":    median_wl,
            "azimuth":          median_azimuth,
            "phase_speed":      median_speed,
            "n_stds":           n_stds,
            # Per-frame arrays (NaN where family does not own this frame)
            "orientation_deg_series": orient_series,
            "wavelength_km_series":   wl_series,
            # Ownership mask — useful for diagnostics and the KF
            "_owned_mask":            owned_mask,
            "_win_half_min":          win_half_min,
            "_group_id":              group_id[fi],
        })

    return waves


def flag_kf_wave_collisions_by_correlation(
    kf_results: pd.DataFrame,
    waves_detected: list,
    waves_kf: list,
    wave_cubes_kf: list,
    time_vals: np.ndarray,
    corr_threshold: float = 0.7,
    smoothing_window: int = 5,
    min_episode_frames: int = 3,
    dir_tol_deg: float = 22.5,
    lambda_tol_km: float = 20.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Flag periods where two or more waves' KF-refined second-pass Gabor
    reconstructions are essentially reconstructing the same physical
    structure -- the same failure mode flag_kf_wave_collisions (parameter-
    tolerance based) targeted, but detected directly from the reconstructed
    fields instead of from orientation/wavelength distance.

    Comparing kf_orient_deg/kf_lambda_km against fixed tolerances (the
    previous approach) has two blind spots: it isn't scale-aware (a fixed
    lambda_tol_km is a huge relative tolerance for a short wave and a tiny
    one for a long wave), and it ignores phase -- two waves can sit just
    outside the numeric tolerance while their actual reconstructed spatial
    fields are nearly identical, or have similar orientation/wavelength by
    coincidence while being spatially distinct. Cross-correlating
    wave_cubes_kf directly (same per-frame Pearson r used by
    gabor_reconstruction.check_duplicate_waves, here resolved per-frame
    instead of averaged over the whole night) measures the thing that
    actually matters: are two waves painting the same picture.

    Must run AFTER the second-pass reconstruction (wave_cubes_kf has to
    exist), unlike the old parameter-based check which only needed
    kf_results and so could run before it.

    Resolution is done in two stages, same rationale as the previous
    version (avoid frame-by-frame flip-flopping over which wave "wins"):

    1. Each pair's per-frame Pearson r is smoothed with a rolling mean
       before thresholding; smoothed-collision runs shorter than
       min_episode_frames are discarded as noise.
    2. Per frame, waves connected by a smoothed r >= corr_threshold are
       grouped via connected components -- not pairwise-only, so a three
       (or more)-way collision (e.g. four waves all correlating with their
       neighbors around the same k-space region) resolves as one group
       instead of overlapping independent pairs. A run of frames keeps its
       episode identity only while the group's exact membership is stable;
       a member joining or leaving closes the old episode and opens a new
       one. Within each episode, ONE wave is judged primary: whichever
       stayed closest, averaged over the episode, to its own founding
       parameters (waves_detected[i]['orientation_deg']/['wavelength_km']).
       dir_tol_deg/lambda_tol_km are used only to normalize orientation vs.
       wavelength distance onto comparable scales for that tie-break --
       they no longer gate collision detection itself.

    Purely informational: returns a copy of kf_results with new columns;
    the caller (airglow_pipeline.py Step 8b / notebook Section 12) is
    responsible for acting on collision_primary if it wants to suppress the
    losing wave's contribution to composite_kf.

    Parameters
    ----------
    kf_results : pd.DataFrame
        Output of analyze_all_waves_kalman().
    waves_detected : list of dict
        For wave['label'] (printed summary) and founding
        wave['orientation_deg']/['wavelength_km'] (primary tie-break).
    waves_kf : list of dict
        Per-wave dicts with 'orientation_deg_series'/'wavelength_km_series'
        (n_time,) arrays on the full night grid -- the same dicts already
        built for reconstruct_all_waves() in Step 8b/Section 12.
    wave_cubes_kf : list of np.ndarray, each (n_time, ny, nx)
        Output of reconstruct_all_waves() for the second pass.
    time_vals : np.ndarray of datetime64
        Full night time grid, same length/order as wave_cubes_kf[i].
    corr_threshold : float
        Smoothed per-frame Pearson r at or above which two waves are
        considered to be reconstructing the same structure. Default 0.7,
        not 0.5 (gabor_reconstruction.check_duplicate_waves's default,
        which averages r over the whole night rather than gating per
        frame) -- calibrated empirically on real data: at 0.5, transient
        excursions in genuinely-distinct pairs (mean r ~0.3, briefly
        crossing 0.5 for ~10% of the night) still formed spurious
        episodes, and connected-components let a moderately-correlated
        "hub" wave transitively merge otherwise-unrelated waves into
        oversized groups. At 0.7 (just above the observed p99 of ~0.68
        across all pairs x frames), only genuinely persistent duplicates
        cross threshold.
    smoothing_window : int
        Rolling-mean window (frames) applied to each pair's correlation
        time series before thresholding.
    min_episode_frames : int
        Minimum contiguous frames of stable group membership to count as a
        real episode rather than a noise blip.
    dir_tol_deg, lambda_tol_km : float
        Normalization scale for combining orientation and wavelength
        distance in the primary tie-break (not a detection threshold).
    verbose : bool
        Print a summary of detected episodes.

    Returns
    -------
    kf_results : pd.DataFrame
        Copy of the input with four new columns:
            collision_id           -- int, unique per episode; -1 where
                                       not in collision.
            collision_group_wave_ids -- str, comma-separated wave labels in
                                       this episode's group; "" where not
                                       in collision.
            collision_primary      -- bool, True if this wave is judged
                                       the "real" one for its episode
                                       (always True where not in collision).
            collision_primary_wave_id -- int, wave_id of whoever won this
                                       row's episode (same value for every
                                       member, winner or not); -1 where not
                                       in collision. Lets a caller label a
                                       suppressed frame with which family it
                                       was folded into.
    """
    df = kf_results.copy()
    df["collision_id"] = -1
    df["collision_group_wave_ids"] = ""
    df["collision_primary_wave_id"] = -1
    df["collision_primary"] = True

    n = len(waves_detected)
    if n < 2 or len(wave_cubes_kf) < 2:
        if verbose:
            print("No KF collision check: fewer than 2 waves.")
        return df

    n_time = wave_cubes_kf[0].shape[0]

    # ── Per-pair smoothed correlation time series ───────────────────────────
    smoothed = {}
    for i in range(n):
        for j in range(i + 1, n):
            cube_i, cube_j = wave_cubes_kf[i], wave_cubes_kf[j]
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

    # ── Per-frame connected components on the correlation graph ────────────
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
        edges = [pair for pair, s in smoothed.items()
                 if np.isfinite(s[t]) and s[t] >= corr_threshold]
        frame_groups.append(set(_components(edges)))

    # ── Episode detection: a group keeps its identity only while its exact
    #    membership is stable; joining/leaving starts a new episode. ───────
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
            print(f"No correlation-based KF collisions detected (threshold={corr_threshold:.2f}).")
        return df

    # ── Resolve primary per episode, write back into kf_results ────────────
    def _circ_diff_180(a, b):
        d = np.abs(a - b) % 180.0
        return np.minimum(d, 180.0 - d)

    label_map = {i: w["label"] for i, w in enumerate(waves_detected)}
    _t_full_ns = time_vals.astype("datetime64[ns]").astype(np.int64)

    # Per-wave amplitude, aligned to the same 0..n_time-1 frame-index grid
    # waves_kf[wid]'s series / t_start,t_end already use. kf_results'S OWN
    # "frame_idx" column is NOT this grid -- it's relative to the edge-
    # trimmed slice [edge_frames, n_time_full-edge_frames) (see
    # airglow_pipeline.py's edge_frames = ntaps // 2), offset from the full
    # night grid wave_cubes_kf/t_start/t_end use by edge_frames frames.
    # Sorting by frame_idx alone would silently read the wrong segment (~6
    # frames off with the default ntaps=13) -- confirmed by direct
    # instrumentation while implementing this fix, not assumed. Mirror the
    # same timestamp-based alignment the write-back loop below already uses
    # (kf_idx via searchsorted against _t_full_ns) instead.
    amp_by_wave = {}
    for wid in range(n):
        wmask = (df["wave_id"] == wid).to_numpy()
        wtimes_ns = pd.to_datetime(df.loc[wmask, "time"].values).astype(np.int64)
        kf_idx = np.searchsorted(_t_full_ns, wtimes_ns).clip(0, n_time - 1)
        match = np.abs(_t_full_ns[kf_idx] - wtimes_ns) < int(1e9)
        amp_full = np.zeros(n_time, dtype=float)
        amp_vals = df.loc[wmask, "amplitude"].to_numpy()
        amp_full[kf_idx[match]] = amp_vals[match]
        amp_by_wave[wid] = amp_full

    next_cid = 0
    printed = []
    for group, t_start, t_end in episodes:
        seg = slice(t_start, t_end + 1)
        dists = {}
        eligible = set()
        for wid in group:
            # A wave with amplitude == 0 for every frame of this episode has
            # no real signal to have "stayed close to its founding parameters"
            # -- its orientation/wavelength series is frozen at kh_init /
            # theta_init by construction (no observation ever updates it), so
            # it trivially wins the distance comparison below against any
            # genuinely-tracking wave. NaN is treated the same as 0 here: the
            # established no-detection path assigns an exact 0.0+0.0j (not
            # NaN), but a NaN amplitude carries no real signal either way,
            # whatever its origin. See TIEBREAK_FIX_DESIGN.md.
            amp_i = np.nan_to_num(amp_by_wave[wid][seg], nan=0.0)
            if not np.all(amp_i < 1e-9):
                eligible.add(wid)

            gi  = float(waves_detected[wid]["orientation_deg"]) % 180.0
            gli = float(waves_detected[wid]["wavelength_km"])
            oi  = waves_kf[wid]["orientation_deg_series"][seg]
            li  = waves_kf[wid]["wavelength_km_series"][seg]
            d = np.nanmean(_circ_diff_180(oi % 180.0, gi) / dir_tol_deg
                            + np.abs(li - gli) / lambda_tol_km)
            dists[wid] = d if np.isfinite(d) else np.inf

        # If every member is signal-free for this episode (a "both frozen"
        # episode), nothing real is being reconstructed by anyone in it --
        # composite_kf is unaffected by who's labeled primary, so fall back
        # to the original (moot) distance-only choice rather than add
        # special-case behavior to a path with no material effect.
        candidates = eligible if eligible else set(dists)
        primary = min(candidates, key=dists.get)

        cid = next_cid
        next_cid += 1
        group_str = ", ".join(label_map[w] for w in sorted(group))
        printed.append((group, t_start, t_end, primary))

        for wid in group:
            wmask = (df["wave_id"] == wid).to_numpy()
            wtimes_ns = pd.to_datetime(df.loc[wmask, "time"].values).astype(np.int64)
            kf_idx = np.searchsorted(_t_full_ns, wtimes_ns).clip(0, n_time - 1)
            match = np.abs(_t_full_ns[kf_idx] - wtimes_ns) < int(1e9)
            in_episode = match & (kf_idx >= t_start) & (kf_idx <= t_end)
            idx = df.index[wmask][in_episode]
            df.loc[idx, "collision_id"] = cid
            df.loc[idx, "collision_group_wave_ids"] = group_str
            df.loc[idx, "collision_primary"] = (wid == primary)
            df.loc[idx, "collision_primary_wave_id"] = primary

    if verbose:
        print(f"WARNING: {len(episodes)} correlation-based KF collision episode(s) "
              f"detected (threshold={corr_threshold:.2f}):")
        for group, t_start, t_end, primary in printed:
            members = ", ".join(label_map[w] for w in sorted(group))
            print(f"  {{{members}}}  frames {t_start}-{t_end} ({t_end - t_start + 1} frames) "
                  f"-- kept {label_map[primary]}")

    return df
